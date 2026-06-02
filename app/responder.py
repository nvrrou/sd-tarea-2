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

# Configuracion de simulacion y ubicacion del dataset.
FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))
ARTIFICIAL_LATENCY_MS = int(os.getenv("ARTIFICIAL_LATENCY_MS", "120"))
OUTAGE = os.getenv("OUTAGE", "false").lower() == "true"
DATASET_PATH = os.getenv("BUILDINGS_CSV_PATH", "/app/967_buildings.csv")
DATASET_MAX_ROWS = int(os.getenv("BUILDINGS_CSV_MAX_ROWS", "50000"))

# Semilla fija para reproducibilidad.
random.seed(2026)


# Modelo de datos para un edificio del CSV.
@dataclass(frozen=True)
class Building:
    latitude: float
    longitude: float
    area_in_meters: float
    confidence: float


# Dataset cargado en memoria junto con metadata util para health y densidad.
@dataclass(frozen=True)
class LoadedDataset:
    buildings_by_zone: dict[str, list[Building]]
    area_km2_by_zone: dict[str, float]
    rows_loaded: int
    source_path: str


# Busca el CSV montado en Docker o el archivo local de la version.
def dataset_path() -> Path:
    path = Path(DATASET_PATH)
    if path.exists():
        return path

    local_path = Path(__file__).resolve().parents[1] / "967_buildings.csv"
    if local_path.exists():
        return local_path

    raise FileNotFoundError(
        f"No se encontro el dataset CSV. Monta 967_buildings.csv en {DATASET_PATH} "
        "o configura BUILDINGS_CSV_PATH."
    )


# Convierte una fila del CSV en un Building, ignorando filas invalidas.
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


# Asigna una zona a cada edificio para distribuir el dataset en buckets.
def assign_zone(building: Building) -> str:
    lat_bucket = int(abs(building.latitude) * 1000)
    lon_bucket = int(abs(building.longitude) * 1000)
    return ZONES[(lat_bucket + lon_bucket) % len(ZONES)]


# Calcula el area aproximada cubierta por los edificios de una zona.
def bounding_area_km2(buildings: list[Building]) -> float:
    if len(buildings) < 2:
        return 1.0

    lat_min = min(row.latitude for row in buildings)
    lat_max = max(row.latitude for row in buildings)
    lon_min = min(row.longitude for row in buildings)
    lon_max = max(row.longitude for row in buildings)
    mean_lat_rad = math.radians(statistics.mean(row.latitude for row in buildings))
    km_per_degree_lat = 111.32
    km_per_degree_lon = 111.32 * abs(math.cos(mean_lat_rad))
    area = abs((lat_max - lat_min) * km_per_degree_lat * (lon_max - lon_min) * km_per_degree_lon)
    return max(area, 1.0)


# Carga el dataset en memoria y precalcula el area de cada zona.
def load_dataset() -> LoadedDataset:
    path = dataset_path()
    buildings_by_zone: dict[str, list[Building]] = {zone: [] for zone in ZONES}
    loaded = 0

    with path.open(newline="", encoding="utf-8") as csvfile:
        for row in csv.DictReader(csvfile):
            building = parse_building(row)
            if building is None:
                continue

            buildings_by_zone[assign_zone(building)].append(building)
            loaded += 1
            if DATASET_MAX_ROWS > 0 and loaded >= DATASET_MAX_ROWS:
                break

    if loaded == 0:
        raise ValueError(f"{path} no contiene filas utilizables de edificios")

    area_km2_by_zone = {
        zone: bounding_area_km2(buildings) for zone, buildings in buildings_by_zone.items()
    }
    return LoadedDataset(
        buildings_by_zone=buildings_by_zone,
        area_km2_by_zone=area_km2_by_zone,
        rows_loaded=loaded,
        source_path=str(path),
    )


# Carga el dataset al iniciar la aplicacion para responder consultas rapido.
DATASET = load_dataset()


# Simula latencia y fallas temporales segun la configuracion global.
def maybe_fail() -> None:
    time.sleep(ARTIFICIAL_LATENCY_MS / 1000)
    if OUTAGE or random.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="Falla temporal simulada en responder")


# Permite ajustar dinamicamente falla, tasa de error y latencia.
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


# Reporta el estado del servicio y del dataset cargado.
@app.get("/health")
def health():
    return {
        "status": "ok",
        "outage": OUTAGE,
        "failure_rate": FAILURE_RATE,
        "dataset_path": DATASET.source_path,
        "rows_loaded": DATASET.rows_loaded,
        "zones": {zone: len(rows) for zone, rows in DATASET.buildings_by_zone.items()},
    }


# Procesa consultas por zona, confianza y tipo de metrica solicitada.
@app.post("/query")
def query_handler(query: QueryMessage):
    maybe_fail()
    records = DATASET.buildings_by_zone[query.zone_id]
    selected = [r for r in records if r.confidence >= query.confidence_min]

    if query.query_type == "Q1":
        # Cuenta edificios que cumplen el umbral de confianza.
        return {"count": len(selected)}

    if query.query_type == "Q2":
        # Calcula area promedio y total de los edificios filtrados.
        areas = [r.area_in_meters for r in selected]
        return {
            "avg_area": statistics.mean(areas) if areas else 0.0,
            "total_area": sum(areas),
            "n": len(areas),
        }

    if query.query_type == "Q3":
        # Calcula densidad de edificios filtrados por kilometro cuadrado.
        return {"density": len(selected) / DATASET.area_km2_by_zone[query.zone_id]}

    if query.query_type == "Q4":
        # Compara densidad entre dos zonas y retorna la zona ganadora.
        if not query.zone_id_b:
            raise HTTPException(status_code=400, detail="Q4 requiere zone_id_b")
        other = [r for r in DATASET.buildings_by_zone[query.zone_id_b] if r.confidence >= query.confidence_min]
        density_a = len(selected) / DATASET.area_km2_by_zone[query.zone_id]
        density_b = len(other) / DATASET.area_km2_by_zone[query.zone_id_b]
        return {"zone_a": density_a, "zone_b": density_b, "winner": query.zone_id if density_a > density_b else query.zone_id_b}

    if query.query_type == "Q5":
        # Construye un histograma de confianza para la zona solicitada.
        bins = max(1, query.bins)
        counts = [0] * bins
        for row in records:
            idx = min(int(row.confidence * bins), bins - 1)
            counts[idx] += 1
        width = 1.0 / bins
        return [{"bucket": i, "min": round(i * width, 4), "max": round((i + 1) * width, 4), "count": counts[i]} for i in range(bins)]

    # Maneja tipos de consulta no reconocidos con un error claro.
    raise HTTPException(status_code=400, detail="Tipo de consulta desconocido")
