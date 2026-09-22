"""
Script de entrada raíz para la Fase 3: Data Warehouse (Modelo en Estrella)
Ejecución por defecto (HYPERFLIX - Streaming):
    python data_warehouse.py

Ejecución alternativa (Guía de clase - Tienda/Ventas):
    python data_warehouse.py ventas
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    modo = sys.argv[1].lower() if len(sys.argv) > 1 else "hyperflix"
    
    if modo in ["ventas", "tienda", "ecommerce"]:
        from scripts.construir_data_warehouse_estrella import construir_data_warehouse_estrella
        construir_data_warehouse_estrella()
    else:
        from scripts.construir_data_warehouse_hyperflix import construir_data_warehouse_hyperflix
        construir_data_warehouse_hyperflix()
