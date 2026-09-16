import os
import sys
import pymongo
from dotenv import load_dotenv
from pymongo.errors import ConnectionFailure, ConfigurationError, OperationFailure

# Asegurar codificación UTF-8 en salida de consola para Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("MONGO_DB_NAME", "hyperflix_db1")

def conectar_mongodb(nombre_bd=DB_NAME):
    """
    Establece conexión con el clúster de MongoDB Atlas para HyperFlix.
    Retorna (client, db) o (None, None) en caso de fallo.
    """
    try:
        print(f"🔄 Intentando conectar a MongoDB Atlas ({nombre_bd})...")
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=7000)
        client.admin.command('ping')
        print("✅ ¡Conexión exitosa a MongoDB Atlas!")
        
        db = client[nombre_bd]
        return client, db
        
    except ConnectionFailure:
        print("❌ Error: No se pudo contactar al servidor de MongoDB Atlas.")
    except ConfigurationError as e:
        print(f"❌ Error de configuración: {e}")
    except OperationFailure as e:
        print(f"❌ Error de autenticación: {e}")
    except Exception as e:
        print(f"❌ Error inesperado al conectar: {e}")
    
    return None, None

def obtener_db(nombre_bd=DB_NAME):
    """Retorna la instancia de base de datos activa"""
    client, db = conectar_mongodb(nombre_bd)
    return db

if __name__ == "__main__":
    client, db = conectar_mongodb()
    if client:
        colecciones = db.list_collection_names()
        print(f"📁 Colecciones existentes en '{DB_NAME}': {colecciones}")
        client.close()
        print("🔒 Conexión cerrada.")