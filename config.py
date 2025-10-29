import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # Configurações do ThingSpeak
    THINGSPEAK_CHANNEL_ID = os.getenv("THINGSPEAK_CHANNEL_ID")
    THINGSPEAK_READ_API_KEY = os.getenv("THINGSPEAK_READ_API_KEY")
    
    # URL padrão do ThingSpeak
    THINGSPEAK_URL = os.getenv(
        "THINGSPEAK_URL",
        f"https://api.thingspeak.com/channels/{THINGSPEAK_CHANNEL_ID}/feeds.json?api_key={THINGSPEAK_READ_API_KEY}&results=1"
    )

    # Configurações do MongoDB
    MONGO_URI = os.getenv("MONGO_URI")
    DB_NAME = os.getenv("DB_NAME", "sensor_data")
    COLLECTION_NAME = os.getenv("COLLECTION_NAME", "Dados Temperatura e Umidade - DHT11")