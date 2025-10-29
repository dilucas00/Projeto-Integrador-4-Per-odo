import requests
from pymongo import MongoClient

from datetime import datetime, timezone

# --- Configurações ---
# Informações do ThingSpeak
THINGSPEAK_CHANNEL_ID = "3096127"
THINGSPEAK_READ_API_KEY = "1UJ2EAZLMVTQY62M"
THINGSPEAK_URL = f"https://api.thingspeak.com/channels/{THINGSPEAK_CHANNEL_ID}/feeds.json?api_key={THINGSPEAK_READ_API_KEY}&results=1"

# Informações do MongoDB
MONGO_URI = "mongodb+srv://gabrieldilucas00_db_user:x4V27GRBTAnwAYjQ@clusterpi.5lal4hi.mongodb.net/?appName=ClusterPI"
DB_NAME = "sensor_data"
COLLECTION_NAME = "dht11_readings"

def fetch_thingspeak_data():
    """Busca o último registro de temperatura e umidade do ThingSpeak."""
    print(f"Buscando dados em: {THINGSPEAK_URL}")
    try:
        response = requests.get(THINGSPEAK_URL)
        response.raise_for_status()  # Levanta exceção para erros HTTP
        data = response.json()
        
        # O ThingSpeak retorna um array de feeds, pegamos o último (o único que pedimos com results=1)
        if data and 'feeds' in data and data['feeds']:
            feed = data['feeds'][0]
            
            # O DHT11 geralmente usa field1 para temperatura e field2 para umidade (ou vice-versa)
            # Assumindo que field1 é temperatura e field2 é umidade.
            # Se a ordem estiver errada, o usuário pode ajustar os nomes das chaves aqui.
            
            # Converte a string de data/hora do ThingSpeak para um objeto datetime
            timestamp_str = feed.get('created_at')
            if timestamp_str:
                # O ThingSpeak usa formato ISO 8601, mas com 'Z' no final para UTC
                # Python precisa de um pequeno ajuste para o fuso horário
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
