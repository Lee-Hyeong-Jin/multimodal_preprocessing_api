from typing import Dict
import pika
import json
from app.core.config import settings
from opensearchpy import OpenSearch
from app.utils.embedding.embedding_test import embedding

INDEX = "029_multimodal_dwg_20250625"

client = OpenSearch(
    hosts=[settings.OPENSEARCH_HOST],
    http_auth=(settings.OPENSEARCH_USER, settings.OPENSEARCH_PASS),
    use_ssl=True,
    verify_certs=False
)

def build_chunk(data: Dict) -> Dict:
    textual_info = data.get("page_text", "")
    image_description = data.get("page_summary", "")

    textual_info_embedding = embedding(text=textual_info)
    textual_info_embedding__1024 = textual_info_embedding[:1024]

    image_description_embedding = embedding(text=image_description)
    image_description_embedding__1024 = image_description_embedding[:1024]

    chunk = {
        "drawing_id": data.get("drawing_id", ""),
        "summary": data.get("page_summary", ""),
        "image_path": data.get("image_path", ""),
        "image_type": data.get("image_type", ""),
        "image_description": image_description,
        "textual_info": textual_info,
        "info_project": data.get("info_project", ""),
        "info_title": data.get("info_title", ""),
        "info_dwg_no": data.get("info_dwg_no", ""),
        "info_rev": data.get("info_rev", ""),
        "info_scale": data.get("info_scale", ""),
        "parts": data.get("parts", []),
        "_dwg_filename": data.get("_dwg_filename", ""),
        "_dwg_filepath": data.get("_dwg_filepath", ""),
        "_drawing_id": data.get("_drawing_id", ""),
        "_num_images": data.get("_num_images", 0),
        "image_url": data.get("image_url", ""),
        "textual_info_embedding": textual_info_embedding,
        "textual_info_embedding__1024": textual_info_embedding__1024,
        "image_description_embedding": image_description_embedding,
        "image_description_embedding__1024": image_description_embedding__1024
    }
    return chunk

def index_to_opensearch(chunk: dict):
    global INDEX
    try:
        response = client.index(
            index=INDEX,
            body=chunk
        )
        print(f"[📌 Indexed] {response['_id']} → {response['result']}")
    except Exception as e:
        print(f"[❌ OpenSearch index error] {e}")

def callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        chunk = build_chunk(msg)
        index_to_opensearch(chunk)
        print("[✅ Received and indexed chunk]")
    except Exception as e:
        print(f"[❌ Error handling message] {e}")
        print(body)

def ensure_index_exists():
    global INDEX
    if not client.indices.exists(index=INDEX):
        print(f"[ℹ️  Index '{INDEX}' not found. Creating it...]")
        client.indices.create(
            index=INDEX,
            body={
                "settings": {
                    "index": {"knn": True},
                    "analysis": {
                        "analyzer": {
                            "nori_analyzer": {
                                "type": "custom",
                                "tokenizer": "nori_tokenizer",
                                "filter": ["lowercase"]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "drawing_id": {"type": "keyword"},
                        "summary": {"type": "text", "analyzer": "nori_analyzer"},
                        "image_path": {"type": "keyword"},
                        "image_type": {"type": "keyword"},
                        "image_description": {"type": "text", "analyzer": "nori_analyzer"},
                        "textual_info": {"type": "text", "analyzer": "nori_analyzer"},
                        "info_project": {"type": "keyword"},
                        "info_title": {"type": "text", "analyzer": "nori_analyzer"},
                        "info_dwg_no": {"type": "keyword"},
                        "info_rev": {"type": "keyword"},
                        "info_scale": {"type": "keyword"},
                        "parts": {"type": "nested"},
                        "_dwg_filename": {"type": "keyword"},
                        "_dwg_filepath": {"type": "keyword"},
                        "_drawing_id": {"type": "keyword"},
                        "_num_images": {"type": "integer"},
                        "image_url": {"type": "keyword"},
                        "textual_info_embedding": {
                            "type": "knn_vector", "dimension": 3072,
                            "method": {
                                "engine": "nmslib", "space_type": "cosinesimil", "name": "hnsw",
                                "parameters": {"ef_construction": 128, "m": 24}
                            }
                        },
                        "textual_info_embedding__1024": {
                            "type": "knn_vector", "dimension": 1024,
                            "method": {
                                "engine": "nmslib", "space_type": "cosinesimil", "name": "hnsw",
                                "parameters": {"ef_construction": 128, "m": 24}
                            }
                        },
                        "image_description_embedding": {
                            "type": "knn_vector", "dimension": 3072,
                            "method": {
                                "engine": "nmslib", "space_type": "cosinesimil", "name": "hnsw",
                                "parameters": {"ef_construction": 128, "m": 24}
                            }
                        },
                        "image_description_embedding__1024": {
                            "type": "knn_vector", "dimension": 1024,
                            "method": {
                                "engine": "nmslib", "space_type": "cosinesimil", "name": "hnsw",
                                "parameters": {"ef_construction": 128, "m": 24}
                            }
                        }
                    }
                }
            }
        )
        print(f"[✅ Index '{INDEX}' created.]")
    else:
        print(f"[✅ Index '{INDEX}' already exists.]")

def main():
    ensure_index_exists()
    username = settings.RABBITMQ_DEFAULT_USER
    password = settings.RABBITMQ_DEFAULT_PASS
    host = settings.RABBITMQ_HOST
    port = settings.RABBITMQ_PORT

    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(host=host, port=port, credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='pdf_metadata', durable=True)

    channel.basic_consume(queue="pdf_metadata", on_message_callback=callback, auto_ack=True)
    print("[*] Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()
