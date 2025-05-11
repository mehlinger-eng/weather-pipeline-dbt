from fastapi import FastAPI
from ingestion.ingest_api_to_firestore import main as ingest
from sync.firestore_to_bigquery import main as sync

app = FastAPI()

@app.get("/api/ingest")
def run_ingestion():
    try:
        ingest()
        return {"status": "✅ Ingestion complete"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "❌ Failed", "error": str(e)}, 500
    
@app.get("/api/sync")
@app.post("/api/sync")
def run_sync():
    try:
        sync()
        return {"status": "✅ Sync complete"}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "❌ Sync failed", "error": str(e)}, 500