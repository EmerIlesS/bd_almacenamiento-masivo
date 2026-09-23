"""
Acceso directo para generar el Catálogo y Diccionario Oficial de Datos.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from generar_diccionario_datos import main

if __name__ == "__main__":
    main()
