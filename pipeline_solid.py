from config import Config
from data_collector import ThingSpeakCollector
from database_handler import MongoDBHandler

def run_pipeline():
    """Função principal para orquestrar a coleta e o armazenamento de dados."""
    
    # Configuração
    config = Config()
    
    # Coleta de Dados
    collector = ThingSpeakCollector(config)
    data_document = collector.fetch_data()
    
    # Armazenamento de Dados
    if data_document:
        handler = MongoDBHandler(config)
        handler.store_data(data_document)
    else:
        print("Pipeline encerrada sem armazenamento devido à falha na coleta de dados.")

if __name__ == "__main__":
    run_pipeline()