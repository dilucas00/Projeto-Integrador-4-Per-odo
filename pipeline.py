import os
import requests
from pymongo import MongoClient
from dotenv import load_dotenv

from datetime import datetime, timezone

# Carrega variáveis do arquivo .env (se existir)
load_dotenv()

# Informações do ThingSpeak
THINGSPEAK_CHANNEL_ID = os.getenv("THINGSPEAK_CHANNEL_ID")
THINGSPEAK_READ_API_KEY = os.getenv("THINGSPEAK_READ_API_KEY")
THINGSPEAK_URL = os.getenv("THINGSPEAK_URL",
                           f"https://api.thingspeak.com/channels/{THINGSPEAK_CHANNEL_ID}/feeds.json?api_key={THINGSPEAK_READ_API_KEY}&results=1")

# Informações do MongoDB
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "sensor_data")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")

def fetch_thingspeak_data():
    """Busca o último registro de temperatura e umidade do ThingSpeak."""
    print(f"Buscando dados em: {THINGSPEAK_URL}")
    try:
        response = requests.get(THINGSPEAK_URL)
        response.raise_for_status()  
        data = response.json()
        
    
        if data and 'feeds' in data and data['feeds']:
            feed = data['feeds'][0]
            
            # Converte a string de data/hora do ThingSpeak para um objeto datetime
            timestamp_str = feed.get('created_at')
            if timestamp_str:
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            else:
                timestamp = datetime.now(timezone.utc)
            
            # Cria o documento a ser inserido no MongoDB
            document = {
                "timestamp": timestamp,
                "temperatura": float(feed.get('field1', 0)),
                "umidade": float(feed.get('field2', 0)),
                "source_id": feed.get('entry_id')
            }
            print("Dados coletados com sucesso.")
            return document
        else:
            print("Nenhum feed encontrado no ThingSpeak.")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar dados do ThingSpeak: {e}")
        return None
    except Exception as e:
        print(f"Erro inesperado ao processar dados do ThingSpeak: {e}")
        return None

def store_in_mongodb(document):
    """Armazena o documento no MongoDB."""
    if not document:
        print("Documento vazio, nada a armazenar.")
        return

    print("Conectando ao MongoDB...")
    client = None
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Insere o documento
        result = collection.insert_one(document)
        print(f"Documento inserido com sucesso! ID: {result.inserted_id}")
        
    except Exception as e:
        print(f"Erro ao conectar ou inserir no MongoDB: {e}")
    finally:
        if client:
            client.close()
            print("Conexão com MongoDB fechada.")

if __name__ == "__main__":
    data_document = fetch_thingspeak_data()
    if data_document:
        store_in_mongodb(data_document)
    else:
        print("Pipeline encerrada sem armazenamento devido à falha na coleta de dados.")
