import requests
from datetime import datetime, timezone
from config import Config

class ThingSpeakCollector:
    def __init__(self, config: Config):
        self.config = config

    def fetch_data(self) -> dict | None:
        """Busca o último registro de temperatura e umidade do ThingSpeak."""
        url = self.config.THINGSPEAK_URL
        print(f"Buscando dados em: {url}")
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            if data and 'feeds' in data and data['feeds']:
                feed = data['feeds'][0]
                
                # Processamento de dados (Transformação)
                processed_data = self._process_feed(feed)
                print("Dados coletados com sucesso.")
                return processed_data
            else:
                print("Nenhum feed encontrado no ThingSpeak.")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar dados do ThingSpeak: {e}")
            return None
        except Exception as e:
            print(f"Erro inesperado ao processar dados do ThingSpeak: {e}")
            return None

    def _process_feed(self, feed: dict) -> dict:
        """Converte o feed bruto do ThingSpeak para o formato de documento do MongoDB."""
        timestamp_str = feed.get('created_at')
        if timestamp_str:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            timestamp = datetime.now(timezone.utc)
        
        # Cria o documento a ser inserido no MongoDB
        document = {
            "Timestamp": timestamp,
            "Temperatura C°": float(feed.get('field1', 0)),
            "Tmidade": float(feed.get('field2', 0)),
            "source_id": feed.get('entry_id')
        }
        return document