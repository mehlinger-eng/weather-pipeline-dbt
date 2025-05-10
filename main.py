from fastapi import FastAPI
from ingestion.ingest_api_to_firestore import main as ingest 

app = FastAPI()

@app.get("/")
def run_ingestion():
    try:
        ingest()
        return {"status": "✅ Ingestion complete"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "❌ Failed", "error": str(e)}, 500