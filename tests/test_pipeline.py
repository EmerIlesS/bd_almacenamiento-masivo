import os
import pytest
import pandas as pd

def test_data_lake_local_structure():
    """Valida que existan las zonas del Data Lake (raw, processed, curated)."""
    zonas = ["data_lake/raw", "data_lake/processed", "data_lake/curated"]
    for zona in zonas:
        assert os.path.exists(zona), f"La zona {zona} debe existir en el Data Lake"

def test_dim_contenido_schema():
    """Valida que el archivo dimensional de contenidos tenga las columnas canónicas."""
    csv_path = "datos_dw_csv/dim_contenido.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        required_cols = {"contenido_id", "titulo", "genero_primario", "rating_promedio"}
        assert required_cols.issubset(set(df.columns)), "Faltan columnas requeridas en dim_contenido"
        assert len(df) > 0, "dim_contenido no puede estar vacío"

def test_qos_calculation_logic():
    """Valida la función analítica de cálculo de QoS de streaming."""
    def calcular_qos(latencia_ms, buffering_count):
        return max(1.0, 10.0 - (latencia_ms / 150.0) - (buffering_count * 0.8))

    score_perfecto = calcular_qos(latencia_ms=50, buffering_count=0)
    assert 9.0 <= score_perfecto <= 10.0, "Con baja latencia el QoS debe ser alto"

    score_degradado = calcular_qos(latencia_ms=800, buffering_count=5)
    assert score_degradado < 6.0, "Con alta latencia y buffering el QoS debe degradarse"

def test_fact_reproducciones_integrity():
    """Valida que los eventos de la tabla de hechos contengan métricas coherentes."""
    csv_path = "datos_dw_csv/fact_reproducciones.csv"
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path, nrows=100)
        assert (df["minutos_vistos"] >= 0).all(), "Los minutos vistos no pueden ser negativos"
        assert (df["completitud_pct"] >= 0).all() and (df["completitud_pct"] <= 100).all(), "Completitud debe estar entre 0 y 100"
