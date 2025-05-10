from fastapi import FastAPI
from ingestion.ingest_api_to_firestore import main as ingest 

app = FastAPI()

@app.get("/")
def run_ingestion():
    ingest()
    return {"status": "✅ Ingestion complete"}