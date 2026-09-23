"""
Acceso directo para ejecutar la automatización de Infraestructura como Código (IaC).
"""
import sys
import os

# Asegurar que la carpeta scripts esté en el path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from automatizar_infraestructura import main

if __name__ == "__main__":
    main()
