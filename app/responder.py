from __future__ import annotations

import csv
import math
import os
import random
import statistics
import time
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query

from app.shared import QueryMessage, ZONES

app = FastAPI(title="Generador de Respuestas T2")
# Configuraciones de simulación
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))
ARTIFICIAL_LATENCY_MS = int(os.getenv("ARTIFICIAL_LATENCY_MS", "120"))
OUTAGE = os.getenv("OUTAGE", "false").lower() == "true"
DATASET_PATH = os.getenv("BUILDINGS_CSV_PATH", "/app/967_buildings.csv")
DATASET_MAX_ROWS = int(os.getenv("BUILDINGS_CSV_MAX_ROWS", "50000"))

# Semilla fija para reproducibilidad
random.seed(2026)

# Modelo de datos para un edificio
@dataclass(frozen=True)
class Building:
    latitude: float
    longitude: float
    area_in_meters: float
    confidence: float

# Funciones de carga y procesamiento del dataset
def dataset_path() -> Path:
    path = Path(DATASET_PATH)
    if path.exists():
        return path
    local_path = Path(__file__).resolve().parents[1] / "967_buildings.csv"
    if local_path.exists():
        return local_path
    raise FileNotFoundError("No se encontro el dataset CSV")

# Función para parsear una fila del CSV en un objeto Building, con manejo de errores
def parse_building(row: dict[str, str]) -> Building | None:
    try:
        return Building(
            latitude=float(row["latitude"]),
            longitude=float(row["longitude"]),
            area_in_meters=float(row["area_in_meters"]),
            confidence=float(row["confidence"]),
        )
    except (KeyError, TypeError, ValueError):
        return None

# Asignar una zona a un edificio basado en su latitud y longitud, para distribuirlo en los buckets
def assign_zone(building: Building) -> str:
    lat_bucket = int(abs(building.latitude) * 1000)
    lon_bucket = int(abs(building.longitude) * 1000)
    return ZONES[(lat_bucket + lon_bucket) % len(ZONES)]

# Cargar el dataset completo en memoria, organizando los edificios por zona para consultas rápidas
def load_dataset() -> dict[str, list[Building]]:
    buildings_by_zone: dict[str, list[Building]] = {zone: [] for zone in ZONES}
    loaded = 0
    with dataset_path().open(newline="", encoding="utf-8") as csvfile:
        for row in csv.DictReader(csvfile):
            building = parse_building(row)
            if building is None:
                continue
            buildings_by_zone[assign_zone(building)].append(building)
            loaded += 1
            if DATASET_MAX_ROWS > 0 and loaded >= DATASET_MAX_ROWS:
                break
    if loaded == 0:
        raise ValueError("El dataset no contiene filas utilizables")
    return buildings_by_zone

# Cargar el dataset al iniciar la aplicación, para tenerlo listo para las consultas
DATASET = load_dataset()

# Calcular el área aproximada en km² de una zona basada en las coordenadas de los edificios, para consultas de densidad
def area_km2(records: list[Building]) -> float:
    if len(records) < 2:
        return 1.0
    lat_min = min(row.latitude for row in records)
    lat_max = max(row.latitude for row in records)
    lon_min = min(row.longitude for row in records)
    lon_max = max(row.longitude for row in records)
    mean_lat_rad = math.radians(statistics.mean(row.latitude for row in records))
    area = abs((lat_max - lat_min) * 111.32 * (lon_max - lon_min) * 111.32 * abs(math.cos(mean_lat_rad)))
    return max(area, 1.0)

# Función para simular fallas y latencia en las respuestas, basada en las configuraciones globales
def maybe_fail() -> None:
    time.sleep(ARTIFICIAL_LATENCY_MS / 1000)
    if OUTAGE or random.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="Falla temporal simulada en responder")

# Endpoint para configurar dinámicamente las condiciones de falla y latencia, permitiendo ajustar la simulación sin reiniciar la aplicación
@app.post("/failure")
def configure_failure(
    outage: bool | None = Query(None),
    failure_rate: float | None = Query(None, ge=0.0, le=1.0),
    latency_ms: int | None = Query(None, ge=0),
):
    global OUTAGE, FAILURE_RATE, ARTIFICIAL_LATENCY_MS
    if outage is not None:
        OUTAGE = outage
    if failure_rate is not None:
        FAILURE_RATE = failure_rate
    if latency_ms is not None:
        ARTIFICIAL_LATENCY_MS = latency_ms
    return {"outage": OUTAGE, "failure_rate": FAILURE_RATE, "latency_ms": ARTIFICIAL_LATENCY_MS}

# Endpoint de salud para verificar que la aplicación está funcionando y ver el número de registros por zona, útil para monitoreo y debugging
@app.get("/health")
def health():
    return {"status": "ok", "zones": {zone: len(rows) for zone, rows in DATASET.items()}}

# Endpoint principal para manejar las consultas, procesando el tipo de consulta y los parámetros para filtrar los edificios por zona y confianza, y calcular las métricas solicitadas
@app.post("/query")
def query_handler(query: QueryMessage):
    maybe_fail()
    records = DATASET[query.zone_id]
    selected = [row for row in records if row.confidence >= query.confidence_min]
    if query.query_type == "Q1": # Solo contar el número de edificios que cumplen con el umbral de confianza en la zona especificada
        return {"count": len(selected)}
    if query.query_type == "Q2": # Calcular el área promedio y total de los edificios que cumplen con el umbral de confianza en la zona especificada, para obtener métricas de tamaño
        areas = [row.area_in_meters for row in selected]
        return {"avg_area": statistics.mean(areas) if areas else 0.0, "total_area": sum(areas), "n": len(areas)}
    if query.query_type == "Q3": # Calcular la densidad de edificios que cumplen con el umbral de confianza en la zona especificada, dividiendo el número de edificios seleccionados por el área aproximada de la zona, para obtener una métrica de concentración
        return {"density": len(selected) / area_km2(records)}
    if query.query_type == "Q4": # Comparar la densidad de edificios que cumplen con el umbral de confianza entre la zona especificada y otra zona dada por zone_id_b, para determinar cuál tiene mayor concentración de edificios confiables, y retornar las densidades y el ganador
        if not query.zone_id_b:
            raise HTTPException(status_code=400, detail="Q4 requiere zone_id_b")
        other = [row for row in DATASET[query.zone_id_b] if row.confidence >= query.confidence_min]
        density_a = len(selected) / area_km2(records)
        density_b = len(other) / area_km2(DATASET[query.zone_id_b]) # Calcular la densidad para la zona B usando el mismo método que para la zona A, para una comparación justa
        return {"zone_a": density_a, "zone_b": density_b, "winner": query.zone_id if density_a > density_b else query.zone_id_b}
    if query.query_type == "Q5":
        bins = max(1, query.bins)
        counts = [0] * bins
        for row in records:
            idx = min(int(row.confidence * bins), bins - 1) # Asignar cada edificio a un bin basado en su confianza, para crear un histograma de la distribución de confianza en la zona
            counts[idx] += 1
        return counts
    raise HTTPException(status_code=400, detail="Tipo de consulta desconocido") # Manejar el caso de un tipo de consulta no reconocido, retornando un error 400 con un mensaje claro para el cliente
