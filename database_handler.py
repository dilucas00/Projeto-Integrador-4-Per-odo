from pymongo import MongoClient
from config import Config

class MongoDBHandler:
    def __init__(self, config: Config):
        self.config = config
        self.client = None

    def store_data(self, document: dict | None):
        """Armazena o documento no MongoDB."""
        if not document:
            print("Documento vazio, nada a armazenar.")
            return

        print("Conectando ao MongoDB...")
        try:
            self.client = MongoClient(self.config.MONGO_URI)
            db = self.client[self.config.DB_NAME]
            collection = db[self.config.COLLECTION_NAME]
            
            result = collection.insert_one(document)
            print(f"Documento inserido com sucesso! ID: {result.inserted_id}")
            
        except Exception as e:
            print(f"Erro ao conectar ou inserir no MongoDB: {e}")
        finally:
            if self.client:
                self.client.close()
                print("Conexão com MongoDB fechada.")
