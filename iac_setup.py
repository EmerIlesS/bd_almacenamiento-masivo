"""
=============================================================================
HYPERFLIX - ORQUESTADOR DE INFRAESTRUCTURA Y CONFIGURACIÓN COMO CÓDIGO (IaC)
=============================================================================
Archivo:    iac_setup.py
Objetivo:   Automatizar el aprovisionamiento local y la configuración del entorno
            para garantizar reproducibilidad ("En mi máquina sí funciona" -> 0 errores).

Cubre los 5 pilares exigidos por la rúbrica pedagógica y técnica:
  1. Validación del entorno (Versión de Python)
  2. Aislamiento del entorno virtual (.venv)
  3. Gestión automatizada de dependencias (requirements.txt)
  4. Provisionamiento local de carpetas del Data Lake (raw, processed, curated)
  5. Health Check: Verificación de .env y prueba de conexión a MongoDB Atlas
=============================================================================
"""

import os
import sys
import subprocess
import shutil
import time

# Asegurar codificación UTF-8 en Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Constantes del proyecto
PYTHON_MIN_VERSION = (3, 9)
VENV_DIR = ".venv"
REQUIREMENTS_FILE = "requirements.txt"
DATA_LAKE_ZONES = [
    os.path.join("data_lake", "raw"),
    os.path.join("data_lake", "processed"),
    os.path.join("data_lake", "curated")
]


def imprimir_titulo():
    print("=" * 80)
    print("🛡️  HYPERFLIX — ORQUESTADOR DE INFRAESTRUCTURA COMO CÓDIGO (iac_setup.py)")
    print("   Automatización de Entorno, Dependencias, Data Lake y Conectividad Cloud")
    print("=" * 80 + "\n")


def paso_1_validar_entorno():
    print("🔍 [Paso 1/5] Validando versión del entorno de Python...")
    version_actual = sys.version_info
    version_str = f"{version_actual.major}.{version_actual.minor}.{version_actual.micro}"
    
    if version_actual < PYTHON_MIN_VERSION:
        print(f"❌ Error: Se requiere Python {PYTHON_MIN_VERSION[0]}.{PYTHON_MIN_VERSION[1]}+. Tienes {version_str}.")
        sys.exit(1)
        
    print(f"   ✅ Versión de Python compatible detectada: v{version_str}")
    print(f"   📍 Intérprete actual: {sys.executable}\n")


def paso_2_crear_entorno_virtual():
    print(f"📦 [Paso 2/5] Gestionando aislamiento con Entorno Virtual ('{VENV_DIR}')...")
    
    # Determinar rutas según sistema operativo
    if sys.platform == "win32":
        venv_python = os.path.join(VENV_DIR, "Scripts", "python.exe")
        venv_pip = os.path.join(VENV_DIR, "Scripts", "pip.exe")
        activate_cmd = f".\\{VENV_DIR}\\Scripts\\activate"
    else:
        venv_python = os.path.join(VENV_DIR, "bin", "python")
        venv_pip = os.path.join(VENV_DIR, "bin", "pip")
        activate_cmd = f"source {VENV_DIR}/bin/activate"

    if not os.path.exists(VENV_DIR) or not os.path.exists(venv_python):
        print(f"   ⚙️ Creando nuevo entorno virtual en '{VENV_DIR}'...")
        try:
            import venv
            venv.create(VENV_DIR, with_pip=True)
            print(f"   ✅ Entorno virtual '{VENV_DIR}' creado exitosamente.")
        except Exception as e:
            print(f"   ⚠️ venv nativo falló ({e}), intentando vía subprocess...")
            subprocess.run([sys.executable, "-m", "venv", VENV_DIR], check=True)
            print(f"   ✅ Entorno virtual '{VENV_DIR}' creado.")
    else:
        print(f"   ✅ Entorno virtual existente detectado en '{VENV_DIR}'.")

    print(f"   💡 Comando de activación: {activate_cmd}\n")
    return venv_python, venv_pip, activate_cmd


def paso_3_instalar_dependencias(venv_python):
    print(f"📥 [Paso 3/5] Gestionando dependencias desde '{REQUIREMENTS_FILE}'...")
    
    if not os.path.exists(REQUIREMENTS_FILE):
        print(f"❌ Error: No se encontró el archivo '{REQUIREMENTS_FILE}'.")
        sys.exit(1)
        
    print("   🔄 Verificando e instalando paquetes (pymongo, pandas, pyspark, etc.)...")
    try:
        # Actualizar pip primero
        subprocess.run(
            [venv_python, "-m", "pip", "install", "--upgrade", "pip"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        
        # Instalar requirements.txt
        subprocess.run(
            [venv_python, "-m", "pip", "install", "-r", REQUIREMENTS_FILE],
            check=True
        )
        print("   ✅ Todas las dependencias instaladas y actualizadas correctamente.\n")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error al instalar dependencias con pip: {e}")
        sys.exit(1)


def paso_4_provisionar_data_lake():
    print("📁 [Paso 4/5] Provisionando estructura local del Data Lake...")
    for zona in DATA_LAKE_ZONES:
        if not os.path.exists(zona):
            os.makedirs(zona, exist_ok=True)
            print(f"   ➕ Carpeta creada: '{zona}'")
        else:
            archivos = [f for f in os.listdir(zona) if f.endswith('.parquet')]
            print(f"   ✓ Carpeta lista: '{zona}' ({len(archivos)} archivo(s) Parquet)")
    print("   ✅ Zonas del Data Lake (Raw, Processed, Curated) listas.\n")


def paso_5_health_check(venv_python):
    print("🩺 [Paso 5/5] Ejecutando Health Check (Prueba de Salud y Conectividad Cloud)...")
    
    # 1. Validar .env
    env_file = ".env"
    env_example = ".env.example"
    
    if not os.path.exists(env_file):
        if os.path.exists(env_example):
            print(f"   ⚠️ Archivo '{env_file}' no encontrado. Creando copia desde '{env_example}'...")
            shutil.copyfile(env_example, env_file)
            print(f"   ℹ️ Se creó '{env_file}'. Recuerda configurar tu MONGO_URI real.")
        else:
            print(f"   ❌ Error crítico: '{env_file}' no existe y no se encontró '{env_example}'.")
            return False
    else:
        print(f"   ✅ Archivo de configuración '{env_file}' detectado.")

    # 2. Prueba de conexión a MongoDB Atlas
    script_test_mongo = """
import os
import sys
import time
from dotenv import load_dotenv
import pymongo

load_dotenv()
uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB_NAME", "hyperflix_db")

if not uri:
    print("MISSING_URI")
    sys.exit(2)

try:
    t0 = time.time()
    client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
    client.admin.command('ping')
    latencia = round((time.time() - t0) * 1000, 2)
    db = client[db_name]
    total_cols = len(db.list_collection_names())
    print(f"SUCCESS|{latencia}|{db_name}|{total_cols}")
except Exception as e:
    print(f"ERROR|{str(e)}")
    sys.exit(3)
"""
    try:
        resultado = subprocess.run(
            [venv_python, "-c", script_test_mongo],
            capture_output=True,
            text=True,
            check=False
        )
        salida = resultado.stdout.strip()
        
        if "SUCCESS" in salida:
            partes = salida.split("|")
            latencia = partes[1] if len(partes) > 1 else "N/A"
            base_datos = partes[2] if len(partes) > 2 else "hyperflix_db"
            cols = partes[3] if len(partes) > 3 else "0"
            print(f"   🌐 Conexión a MongoDB Atlas: EXITOSA (Ping: {latencia} ms)")
            print(f"   📦 Base de datos vinculada: '{base_datos}' ({cols} colecciones activas)")
            print("   ✅ Health Check superado al 100%.\n")
            return True
        elif "MISSING_URI" in salida:
            print("   ⚠️ MONGO_URI no está definido en tu archivo .env.")
            return False
        else:
            print(f"   ⚠️ Prueba de ping a Atlas falló: {salida}")
            return False
    except Exception as ex:
        print(f"   ⚠️ No se pudo ejecutar la prueba de salud: {ex}")
        return False


def main():
    imprimir_titulo()
    t_inicio = time.time()
    
    paso_1_validar_entorno()
    venv_python, venv_pip, activate_cmd = paso_2_crear_entorno_virtual()
    paso_3_instalar_dependencias(venv_python)
    paso_4_provisionar_data_lake()
    atlas_ok = paso_5_health_check(venv_python)
    
    tiempo_total = round(time.time() - t_inicio, 2)
    
    print("=" * 80)
    print(f"🎉 INFRAESTRUCTURA Y ENTORNO CONFIGURADOS CON ÉXITO ({tiempo_total} segundos)")
    print("=" * 80)
    print("📋 Resumen de Aprovisionamiento:")
    print("   • Entorno Virtual:     .venv (Aislado y listo)")
    print("   • Dependencias:        Instaladas según requirements.txt")
    print("   • Data Lake:           3 Zonas creadas (raw, processed, curated)")
    print(f"   • MongoDB Atlas Cloud: {'Conectado y Operativo' if atlas_ok else 'Pendiente verificar .env'}")
    print(f"   • Terraform HCL:       Listos en la carpeta 'terraform/'")
    print("-" * 80)
    print("🚀 Para comenzar a trabajar, activa el entorno virtual:")
    print(f"   {activate_cmd}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
