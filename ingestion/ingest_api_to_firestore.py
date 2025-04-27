import requests
from google.cloud import firestore
from datetime import datetime
import logging
import json
import uuid

logging.basicConfig(level=logging.INFO)

def load_config():
    """
    Reads settings from config/settings.json to configure API call and Firestore.
    """
    with open('config/settings.json', 'r') as f:
        return json.load(f)

def fetch_weather_data(latitude, longitude):
    """
    Calls Open Meteo public API and fetches hourly forecast data for a specific location.
    """
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={latitude}&longitude={longitude}"
        f"&hourly=temperature_2m,precipitation"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def transform_weather_data(api_response, location_name):
    """
    Transforms Open Meteo API JSON into a list of Firestore-ready documents.
    Each document corresponds to 1 hour's forecast.
    """
    documents = []
    times = api_response["hourly"]["time"]
    temperatures = api_response["hourly"]["temperature_2m"]
    precipitations = api_response["hourly"]["precipitation"]

    ingestion_timestamp = datetime.utcnow().isoformat()

    for time, temp, precip in zip(times, temperatures, precipitations):
        doc = {
            "location": location_name,
            "latitude": api_response["latitude"],
            "longitude": api_response["longitude"],
            "timestamp": datetime.fromisoformat(time).isoformat(),
            "temperature_celsius": temp,
            "precipitation_mm": precip,
            "ingestion_timestamp": ingestion_timestamp
        }
        documents.append(doc)
    
    return documents

def write_to_firestore(documents, collection_name):
    """
    Batch writes a list of documents to Firestore.
    Each document uses a random UUID as its document ID.
    """
    db = firestore.Client(database="weather-pipeline")
    batch = db.batch()
    collection_ref = db.collection(collection_name)

    for doc in documents:
        doc_id = str(uuid.uuid4()) 
        doc_ref = collection_ref.document(doc_id)
        batch.set(doc_ref, doc)
    
    batch.commit()
    logging.info(f"✅ Successfully wrote {len(documents)} documents to Firestore.")

def main():
    config = load_config()

    latitude = config["latitude"]
    longitude = config["longitude"]
    location_name = config["location_name"]
    collection_name = config["collection_name"]

    logging.info("📡 Fetching weather data from Open Meteo API...")
    api_response = fetch_weather_data(latitude, longitude)

    logging.info("🔄 Transforming weather data...")
    documents = transform_weather_data(api_response, location_name)

    logging.info("📝 Writing transformed data to Firestore...")
    write_to_firestore(documents, collection_name)

    logging.info("🎯 Ingestion completed successfully!")

if __name__ == "__main__":
    main()