"""
Script de entrada raíz para el Apache Spark Job de HyperFlix (Fase 2)
Permite la ejecución directa:
    python spark_processor.py
o pasando un archivo específico:
    python spark_processor.py data_lake/raw/peliculas.parquet
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scripts.spark_job import ejecutar_spark_job

if __name__ == "__main__":
    archivo = sys.argv[1] if len(sys.argv) > 1 else None
    ejecutar_spark_job(archivo)
