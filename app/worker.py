from __future__ import annotations

import json
import os
import socket
import threading
import time

import httpx
import redis
from confluent_kafka import Consumer, Producer

from app.shared import TOPIC_DLQ, TOPIC_MAIN, TOPIC_RETRY, QueryMessage, as_event, cache_key, stable_partition_key

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "geo-workers")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
RESPONDER_URL = os.getenv("RESPONDER_URL", "http://responder:8000")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8000")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_SECONDS = float(os.getenv("RETRY_DELAY_SECONDS", "2"))
DEFAULT_TTL_SECONDS = int(os.getenv("DEFAULT_TTL_SECONDS", "300"))
WORKER_ID = os.getenv("HOSTNAME", socket.gethostname())

redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
producer_lock = threading.Lock()


def make_consumer(name: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP,
            "client.id": f"{WORKER_ID}-{name}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def log(message: str, query: QueryMessage | None = None, topic: str | None = None) -> None:
    parts = [f"[worker={WORKER_ID}]", message]
    if topic:
        parts.append(f"topic={topic}")
    if query:
        parts.append(f"id={query.id}")
        parts.append(f"retry_count={query.retry_count}")
    print(" ".join(parts), flush=True)

# Agrega el ID del worker al payload de la métrica y envía la métrica al servicio de métricas
# manejando cualquier excepción que pueda ocurrir durante el envío.
def send_metric(payload: dict) -> None:
    payload["worker_id"] = WORKER_ID
    try:
        httpx.post(f"{METRICS_URL}/event", json=payload, timeout=2.0)
    except Exception:
        pass

# Publica un mensaje en el topic especificado, utilizando una clave de partición estable basada en el contenido de la consulta
# para asegurar que las consultas similares se procesen en el mismo worker, y luego fuerza el envío de los mensajes al broker.
def publish(topic: str, query: QueryMessage) -> None:
    with producer_lock:
        producer.produce(topic, key=stable_partition_key(query), value=query.model_dump_json())
        producer.flush()

# Procesa una consulta: primero intenta recuperar la respuesta de Redis, y si no está disponible, hace una solicitud HTTP al servicio responder para obtener la respuesta. 
# Luego envia metricas sobre el procesamiento.
def process(query: QueryMessage) -> None:
    started = time.perf_counter()
    key = cache_key(query)
    cached = redis_client.get(key)
    if cached is not None:
        latency_ms = (time.perf_counter() - started) * 1000
        event_name = "recovered" if query.retry_count > 0 else "processed"
        send_metric(as_event(event_name, query, cache_status="hit", latency_ms=round(latency_ms, 3)))
        if event_name == "recovered": # Si la consulta fue recuperada de la caché después de uno o más reintentos, también envía una métrica adicional para indicar que finalmente se procesó con éxito.
            send_metric(as_event("processed", query, cache_status="hit", latency_ms=round(latency_ms, 3)))
        return

    response = httpx.post(f"{RESPONDER_URL}/query", json=query.model_dump(), timeout=5.0)
    response.raise_for_status()
    redis_client.setex(key, query.ttl_seconds or DEFAULT_TTL_SECONDS, json.dumps(response.json()))
    latency_ms = (time.perf_counter() - started) * 1000
    event_name = "recovered" if query.retry_count > 0 else "processed"
    send_metric(as_event(event_name, query, cache_status="miss", latency_ms=round(latency_ms, 3)))
    if event_name == "recovered":
        send_metric(as_event("processed", query, cache_status="miss", latency_ms=round(latency_ms, 3)))

# Maneja los errores que ocurren durante el procesamiento de una consulta. Si el número de reintentos ha alcanzado el máximo permitido, publica la consulta en el topic de DLQ y envía una métrica de error.
def handle_failure(query: QueryMessage, error: Exception, source_topic: str) -> None:
    log(f"processing failed error={error}", query=query, topic=source_topic)
    if query.retry_count >= MAX_RETRIES:
        log("publishing to DLQ", query=query, topic=TOPIC_DLQ)
        publish(TOPIC_DLQ, query)
        send_metric(as_event("dlq", query, error=str(error), topic=TOPIC_DLQ))
        return

    retry = query.model_copy(update={"retry_count": query.retry_count + 1})
    log("scheduling retry publish", query=retry, topic=TOPIC_RETRY)
    time.sleep(RETRY_DELAY_SECONDS)
    publish(TOPIC_RETRY, retry)
    log("published retry", query=retry, topic=TOPIC_RETRY)
    send_metric(as_event("retry", retry, error=str(error), topic=TOPIC_RETRY))

# Función principal del worker: se suscribe a los topics principales y de reintentos, y luego entra en un bucle infinito donde consume mensajes, procesa las consultas, maneja errores y envía métricas según corresponda.
def consume_topic(topic: str, name: str) -> None:
    consumer = make_consumer(name)
    consumer.subscribe([topic])
    log("consumer started", topic=topic)
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Kafka error en {topic}: {msg.error()}", flush=True)
            continue

        query: QueryMessage | None = None
        try:
            query = QueryMessage.model_validate_json(msg.value().decode("utf-8"))
            log("consumed message", query=query, topic=topic)
            process(query)
            consumer.commit(msg)
            log("committed successful message", query=query, topic=topic)
        except Exception as exc:
            if query is None:
                send_metric({"event": "error", "timestamp": time.time(), "error": str(exc), "topic": topic})
                consumer.commit(msg)
                continue
            try:
                handle_failure(query, exc, topic)
                consumer.commit(msg)
                log("committed failed message after routing", query=query, topic=topic)
            except Exception as failure_exc:
                send_metric({"event": "error", "timestamp": time.time(), "error": str(failure_exc)})


def main() -> None:
    threads = [
        threading.Thread(target=consume_topic, args=(TOPIC_MAIN, "main-consumer"), daemon=True),
        threading.Thread(target=consume_topic, args=(TOPIC_RETRY, "retry-consumer"), daemon=True),
    ]
    for thread in threads:
        thread.start()
    print(f"Worker {WORKER_ID} escuchando {TOPIC_MAIN} y {TOPIC_RETRY}", flush=True)
    for thread in threads:
        thread.join()


if __name__ == "__main__":
    main()
