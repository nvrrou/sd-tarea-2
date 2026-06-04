# Link video
https://www.youtube.com/watch?v=LEcszCdBLrQ

# Tarea 2 - Procesamiento y Fallback con Apache Kafka

Implementacion autocontenida de la segunda entrega. La carpeta no depende de los servicios de la Tarea 1, pero reutiliza el dominio de consultas Q1-Q5, distribuciones uniforme/Zipf, Redis como cache y un generador de respuestas.

El sistema permite comparar directamente una arquitectura síncrona tradicional con una arquitectura orientada a eventos basada en Kafka, utilizando métricas de rendimiento y resiliencia para evaluar ambos enfoques.

## ¿Por qué Apache Kafka?

Uno de los principales desafíos de los sistemas distribuidos es evitar que una falla temporal provoque la pérdida de información o afecte el funcionamiento completo de la aplicación. Para abordar este problema, en esta tarea se incorporó Apache Kafka como mecanismo de comunicación entre los distintos componentes del sistema. Apache Kafka es una plataforma distribuida de transmisión de eventos (event streaming) diseñada para almacenar, procesar y distribuir grandes volúmenes de información de forma confiable y escalable. Su principal ventaja es que permite desacoplar productores y consumidores, evitando que ambos dependan directamente entre sí y facilitando la recuperación ante fallos, el procesamiento paralelo y el manejo de altas cargas de trabajo.

En este proyecto Kafka se utiliza como intermediario entre el generador de consultas y los consumidores, permitiendo implementar reintentos automáticos, colas de recuperación (retry topics) y una Dead Letter Queue (DLQ) para consultas que no logran procesarse correctamente.

Fuente: Apache Software Foundation, Apache Kafka Documentation.
https://kafka.apache.org/

## Arquitectura

- `producer`: genera consultas y las publica en `queries.main`.
- `worker`: consumidores Kafka del mismo grupo. Revisan Redis, llaman al responder en cache miss y publican a `queries.retry` o `queries.dlq` si corresponde.
- `responder`: genera respuestas Q1-Q5 desde `967_buildings.csv` y permite simular fallas.
- `sync_api`: baseline sin Kafka para comparar contra la arquitectura original sin cola.
- `metrics`: registra eventos, calcula throughput, p50/p95, retries, recovery, DLQ y backlog aproximado.
- `kafka`: broker KRaft con 6 particiones para permitir consumidores paralelos.
- `redis`: cache con 200 MB, TTL por defecto 300 s y politica `allkeys-lru`.

## Ejecutar

### Guia de Puertos

- `9092`: Kafka, broker para los topics `queries.main`, `queries.retry` y `queries.dlq`.
- `6379`: Redis, cache compartida entre `worker` y `sync_api`.
- `8100`: Metrics, servicio para registrar eventos y consultar `/summary`.
- `8101`: Responder, servicio que resuelve consultas Q1-Q5 y permite simular fallas con `/failure`.
- `8102`: Producer, servicio para generar carga Kafka desde `/run`.
- `8103`: Sync API, baseline sincronico sin Kafka desde `/run`.

La imagen espera el dataset en la raiz de esta carpeta:

```bash
ls 967_buildings.csv
```

Por defecto el responder carga 50.000 filas para que los experimentos partan rapido. Puedes cambiarlo con `BUILDINGS_CSV_MAX_ROWS` en `docker-compose.yml`; usa `0` para cargar todo el CSV.

```bash
docker compose up --build
```

Escalar consumidores:

```bash
docker compose up --build --scale worker=3
```

Generar carga Kafka:

```bash
curl "http://localhost:8102/run?requests_count=500&distribution=zipf&rate_per_second=50"
```

Generar carga sin Kafka:

```bash
curl "http://localhost:8103/run?requests_count=200&distribution=uniform"
```

Ver resumen de metricas:

```bash
curl "http://localhost:8100/summary"
```

El campo `backlog.total` del resumen estima mensajes pendientes en `queries.main` y `queries.retry`.

Simular falla temporal del generador:

```bash
curl -X POST "http://localhost:8101/failure?outage=true"
Start-Sleep -Seconds 20
curl -X POST "http://localhost:8101/failure?outage=false"
```

## Topicos Kafka

- `queries.main`: entrada normal.
- `queries.retry`: consultas fallidas temporalmente, con contador de reintentos.
- `queries.dlq`: consultas que superaron `MAX_RETRIES`.

Cada mensaje contiene `id`, `query_type`, parametros, `created_at`, `retry_count`, `distribution`, `ttl_seconds` y `scenario`.
