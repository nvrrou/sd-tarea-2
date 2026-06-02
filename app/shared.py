from __future__ import annotations

import hashlib
import math
import random
import time
import uuid
from dataclasses import dataclass
from typing import Any

import numpy as np
from pydantic import BaseModel, Field

# Zonas y parametros usados para generar consultas aleatorias.
ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERIES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
CONFIDENCE_VALUES = [0.0, 0.5, 0.7, 0.9]
BINS_VALUES = [5, 10]

# Topics de Kafka usados para consultas, reintentos y mensajes descartados.
TOPIC_MAIN = "queries.main"
TOPIC_RETRY = "queries.retry"
TOPIC_DLQ = "queries.dlq"


# Modelo comun para transportar una consulta entre servicios.
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


@dataclass(frozen=True)
# Modelo de datos para un edificio, con latitud, longitud, area en metros cuadrados y un valor de confianza.
class ZoneBox:
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float


# Cajas geograficas aproximadas usadas para calcular areas por zona.
ZONAS: dict[str, ZoneBox] = {
    "Z1": ZoneBox(-33.445, -33.420, -70.640, -70.600),
    "Z2": ZoneBox(-33.420, -33.390, -70.600, -70.550),
    "Z3": ZoneBox(-33.530, -33.490, -70.790, -70.740),
    "Z4": ZoneBox(-33.460, -33.430, -70.670, -70.630),
    "Z5": ZoneBox(-33.470, -33.430, -70.810, -70.760),
}


# Calcula el area aproximada de una caja geografica en kilometros cuadrados.
def area_km2(bbox: ZoneBox) -> float:
    dlat = bbox.lat_max - bbox.lat_min
    dlon = bbox.lon_max - bbox.lon_min
    lat_media = math.radians((bbox.lat_min + bbox.lat_max) / 2)
    km_por_grado_lat = 111.32
    km_por_grado_lon = 111.32 * math.cos(lat_media)
    return abs(dlat * km_por_grado_lat * dlon * km_por_grado_lon)

# Precalcula el area de cada zona para usarlo en consultas de densidad, evitando calcularlo cada vez.
ZONE_AREA_KM2 = {zone: area_km2(box) for zone, box in ZONAS.items()}


# Genera consultas con distribucion uniforme o sesgada por Zipf.
def choose_weighted(values: list[str], distribution: str, alpha: float = 1.2) -> str:
    if distribution == "zipf":
        ranks = np.arange(1, len(values) + 1)
        weights = 1 / np.power(ranks, alpha)
        probabilities = weights / weights.sum()
        return str(np.random.choice(values, p=probabilities))
    return random.choice(values)


# Construye una consulta aleatoria con parametros validos para el tipo elegido.
def build_query(distribution: str = "uniform", alpha: float = 1.2, ttl_seconds: int = 300, scenario: str = "manual") -> QueryMessage:
    query_type = choose_weighted(QUERIES, distribution, alpha)
    zone_id = choose_weighted(ZONES, distribution, alpha)
    confidence_min = random.choice(CONFIDENCE_VALUES)
    bins = random.choice(BINS_VALUES)
    zone_id_b = None

    if query_type == "Q4":
        zone_id_b = choose_weighted(ZONES, distribution, alpha)
        while zone_id_b == zone_id:
            zone_id_b = choose_weighted(ZONES, distribution, alpha)

    return QueryMessage(
        query_type=query_type,
        zone_id=zone_id,
        zone_id_b=zone_id_b,
        confidence_min=confidence_min,
        bins=bins,
        distribution=distribution,
        ttl_seconds=ttl_seconds,
        scenario=scenario,
    )


# Construye una clave estable para cachear respuestas por tipo y parametros.
def cache_key(query: QueryMessage) -> str:
    confidence = f"{query.confidence_min:.2f}"
    if query.query_type == "Q1":
        return f"count:{query.zone_id}:conf={confidence}"
    if query.query_type == "Q2":
        return f"area:{query.zone_id}:conf={confidence}"
    if query.query_type == "Q3":
        return f"density:{query.zone_id}:conf={confidence}"
    if query.query_type == "Q4":
        return f"compare:density:{query.zone_id}:{query.zone_id_b}:conf={confidence}"
    if query.query_type == "Q5":
        return f"confidence_dist:{query.zone_id}:bins={query.bins}"
    raise ValueError(f"query_type invalido: {query.query_type}")


# Genera una clave de particion estable para Kafka.
def stable_partition_key(query: QueryMessage) -> str:
    raw = cache_key(query).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


# Calcula un percentil, retornando 0 si no hay datos.
def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    return float(np.percentile(values, p))


# Construye eventos de metricas con contexto de la consulta.
def as_event(event: str, query: QueryMessage | None = None, **extra: Any) -> dict[str, Any]:
    payload = {
        "timestamp": time.time(),
        "event": event,
    }
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
