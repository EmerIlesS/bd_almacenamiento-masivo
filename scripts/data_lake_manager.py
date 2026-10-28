import os
import sys

# Ejecutar el data_lake_manager del directorio raíz
ruta_raiz = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)

import data_lake_manager

if __name__ == "__main__":
    data_lake_manager.ejecutar_pipeline_s3()
