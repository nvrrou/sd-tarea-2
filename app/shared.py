from __future__ import annotations

import time
import uuid

from pydantic import BaseModel, Field

# Definir las zonas geográficas para clasificar los edificios, lo que permite distribuir el dataset en buckets y realizar consultas por zona
ZONES = ["Z1", "Z2", "Z3", "Z4", "Z5"]

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
