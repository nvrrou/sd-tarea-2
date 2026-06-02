from __future__ import annotations

import hashlib
import random
import time
import uuid
from typing import Any

import numpy as np
from pydantic import BaseModel, Field

ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"] # Definir las zonas geográficas para clasificar los edificios, lo que permite distribuir el dataset en buckets y realizar consultas por zona
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"] # Modelo de datos para las consultas, con campos para el tipo de consulta,
CONFIDENCE_VALUES = [0.0, 0.5, 0.7, 0.9] # umbral de confianza, y otros parámetros necesarios para procesar las consultas y generar las respuestas adecuadas
BINS_VALUES = [5, 10] # Número de bins para la consulta Q5, que solicita un histograma de la distribución de confianza en la zona, lo que permite analizar la calidad de los datos

TOPIC_MAIN = "queries.main" # Definir los nombres de los topics de Kafka para la comunicación entre servicios, incluyendo el topic principal para las consultas, un topic de retry para reintentos, y un topic de DLQ para mensajes que no se pudieron procesar después de varios intentos
TOPIC_RETRY = "queries.retry" # Topic para reintentos de consultas que fallaron, permitiendo implementar una lógica de reintentos con backoff y evitar perder consultas importantes debido a fallas temporales
TOPIC_DLQ = "queries.dlq" # Topic de Dead Letter Queue para consultas que no se pudieron procesar después de varios intentos, lo que permite almacenar estos casos para análisis posterior y evitar perder información sobre consultas problemáticas

# Modelo de datos para las consultas, con campos para el tipo de consulta, 
# zona, umbral de confianza, y otros parámetros necesarios para procesar las consultas y generar las respuestas adecuadas
class QueryMessage(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    query_type: str
    zone_id: str
    confidence_min: float = 0.0
    zone_id_b: str | None = None
    bins: int = 5
    retry_count: int = 0
    created_at: float = Field(default_factory=time.time)
    distribution: str = "uniform"
    ttl_seconds: int = 300
    scenario: str = "manual"

# Función para calcular el área aproximada de una zona en kilómetros cuadrados, basada en las coordenadas de los edificios en esa zona
def choose_weighted(values: list[str], distribution: str, alpha: float = 1.2) -> str:
    if distribution == "zipf":
        ranks = np.arange(1, len(values) + 1)
        weights = 1 / np.power(ranks, alpha)
        probabilities = weights / weights.sum()
        return str(np.random.choice(values, p=probabilities))
    return random.choice(values)

# Función para construir una consulta de manera aleatoria, con opciones para diferentes distribuciones de selección y parámetros ajustables
# Generacion aleatoria de consultas, en resumen.
def build_query(distribution: str = "uniform", alpha: float = 1.2, ttl_seconds: int = 300, scenario: str = "manual") -> QueryMessage:
    query_type = choose_weighted(QUERIES, distribution, alpha)
    zone_id = choose_weighted(ZONES, distribution, alpha)
    zone_id_b = None
    if query_type == "Q4":
        zone_id_b = choose_weighted(ZONES, distribution, alpha)
        while zone_id_b == zone_id:
            zone_id_b = choose_weighted(ZONES, distribution, alpha)
    return QueryMessage(
        query_type=query_type,
        zone_id=zone_id,
        zone_id_b=zone_id_b,
        confidence_min=random.choice(CONFIDENCE_VALUES),
        bins=random.choice(BINS_VALUES),
        distribution=distribution,
        ttl_seconds=ttl_seconds,
        scenario=scenario,
    )

# Construccion de la caché key
def cache_key(query: QueryMessage) -> str:
    return f"{query.query_type}:{query.zone_id}:{query.zone_id_b}:{query.confidence_min}:{query.bins}"

# Generar clave de particion para kafka
def stable_partition_key(query: QueryMessage) -> str:
    return hashlib.sha1(cache_key(query).encode("utf-8")).hexdigest()

# Calcular percentiles en una lista
def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(values, p))

# Construir un evento para métricas, con información relevante sobre la consulta y el contexto.
def as_event(event: str, query: QueryMessage | None = None, **extra: Any) -> dict[str, Any]:
    payload = {"timestamp": time.time(), "event": event}
    if query:
        payload.update(
            {
                "query_id": query.id,
                "query_type": query.query_type,
                "zone_id": query.zone_id,
                "distribution": query.distribution,
                "retry_count": query.retry_count,
                "scenario": query.scenario,
                "age_ms": round((time.time() - query.created_at) * 1000, 3),
            }
        )
    payload.update(extra)
    return payload
 