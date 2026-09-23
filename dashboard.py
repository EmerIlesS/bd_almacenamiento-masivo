"""
Punto de entrada raíz para el Dashboard de Monitoreo de HyperFlix
Ejecución:
    python dashboard.py
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scripts.dashboard_monitoreo import generar_dashboard_monitoreo

if __name__ == "__main__":
    generar_dashboard_monitoreo()
