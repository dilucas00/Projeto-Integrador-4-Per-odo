from config import Config
from data_collector import ThingSpeakCollector
from database_handler import MongoDBHandler
import time

def run_pipeline():
    """Função principal para orquestrar a coleta e o armazenamento de dados."""
    
    # 1. Configuração
    config = Config()
    
    # 2. Coleta de Dados
    collector = ThingSpeakCollector(config)
    data_document = collector.fetch_data()
    
    # 3. Armazenamento de Dados
    if data_document:
        handler = MongoDBHandler(config)
        handler.store_data(data_document)
    else:
        print("Pipeline encerrada sem armazenamento devido à falha na coleta de dados.")

if __name__ == "__main__":
    try:
        while True:
            run_pipeline()
            print("\n--- Aguardando 2 minutos para a próxima coleta... ---\n")
            time.sleep(120)
    except KeyboardInterrupt:
        print("\nPipeline interrompida pelo usuário.")
