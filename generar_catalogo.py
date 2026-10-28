"""
Acceso directo raíz para ejecutar el generador de catálogo en Excel.
Ejecuta scripts/generar_catalogo.py directamente.
"""
import os
import runpy

if __name__ == "__main__":
    script_path = os.path.join("scripts", "generar_catalogo.py")
    runpy.run_path(script_path, run_name="__main__")
