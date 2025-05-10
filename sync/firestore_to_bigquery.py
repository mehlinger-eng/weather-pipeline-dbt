# sync/firestore_to_bigquery.py

import logging
import json
from datetime import datetime
from google.cloud import firestore
from google.cloud import bigquery
from google.api_core.exceptions import NotFound

logging.basicConfig(level=logging.INFO)

GCP_PROJECT_ID = "virtual-voyage-457204-c3"
FIRESTORE_DATABASE = "weather-pipeline"
FIRESTORE_COLLECTION = "weather_forecasts"

BQ_DATASET = "weather_data"
BQ_TABLE = "weather_raw"

firestore_client = firestore.Client(project=GCP_PROJECT_ID, database=FIRESTORE_DATABASE)
bigquery_client = bigquery.Client(project=GCP_PROJECT_ID, location="US")

def fetch_documents_from_firestore():
    """
    Fetch documents that have not yet been ingested into BigQuery.
    """
    logging.info(f"📥 Fetching documents from Firestore collection '{FIRESTORE_COLLECTION}'...")
    collection_ref = firestore_client.collection(FIRESTORE_COLLECTION)
    query = collection_ref.where("bq_ingestion_timestamp", "==", None)
    docs = query.stream()
    documents = []
    for doc in docs:
        doc_dict = doc.to_dict()
        documents.append({
            "document_id": doc.id,
            "created_at": doc_dict.get("ingestion_timestamp"),
            "payload": doc_dict
        })
    logging.info(f"✅ Fetched {len(documents)} documents from Firestore.")
    return documents

def load_documents_into_bigquery(documents):
    if not documents:
        logging.info("⚠️ No documents to load. Skipping BigQuery insertion.")
        return
    
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

    rows_to_insert = []
    for doc in documents:
        rows_to_insert.append({
            "document_id": doc["document_id"],
            "created_at": doc["created_at"],
            "payload": json.dumps(doc["payload"])
        })

    logging.info(f"📤 Loading {len(rows_to_insert)} documents into BigQuery table '{table_id}'...")
    
    errors = bigquery_client.insert_rows_json(
        table_id,
        rows_to_insert,
        row_ids=[doc["document_id"] for doc in documents]
    )

    if errors:
        logging.error(f"❌ Encountered errors while inserting rows: {errors}")
    else:
        logging.info("🎯 Successfully loaded documents into BigQuery.")
        update_firestore_after_ingestion(documents)

def update_firestore_after_ingestion(documents):
    """
    After successful BigQuery insertion, update Firestore docs with bq_ingestion_timestamp and operation history.
    """
    batch = firestore_client.batch()
    now = datetime.utcnow().isoformat()

    for doc in documents:
        doc_ref = firestore_client.collection(FIRESTORE_COLLECTION).document(doc["document_id"])
        batch.update(doc_ref, {
            "bq_ingestion_timestamp": now,
            "operation_history": firestore.ArrayUnion([
                {
                    "operation": "UPDATE",
                    "timestamp": now,
                    "details": "Ingested into BigQuery"
                }
            ])
        })
    batch.commit()
    logging.info(f"✅ Updated Firestore documents after BigQuery ingestion.")

def main():
    logging.info("🚀 Starting Firestore to BigQuery sync...")
    documents = fetch_documents_from_firestore()
    load_documents_into_bigquery(documents)
    logging.info("✅ Firestore to BigQuery sync completed.")

if __name__ == "__main__":
    main()