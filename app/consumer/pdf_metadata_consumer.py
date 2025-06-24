from typing import Dict, List
import pika
import json
import uuid
from pathlib import Path
from app.core.config import settings
from opensearchpy import OpenSearch
from app.utils.chunking.recursive_chunking import RecursiveStrategy
from app.utils.embedding.embedding_test import embedding

INDEX = "029_multimodal_manual_20250624"

print(settings.OPENSEARCH_HOST)
print(settings.OPENSEARCH_USER)
print(settings.OPENSEARCH_PASS)
client = OpenSearch(
    hosts=[settings.OPENSEARCH_HOST],
    http_auth=(settings.OPENSEARCH_USER, settings.OPENSEARCH_PASS),
    use_ssl=True,
    verify_certs=False  # 개발 중엔 False, 운영에서는 TLS 인증서 확인을 위해 True 권장
)

def build_chunk(data: Dict, chunk_size: int = 500, overlap_size: int = 100) -> List[Dict]:
    """
    page_summary 또는 page_text를 chunk 단위로 나누어 결과 리스트 반환
    """
    text = data.get("page_text", "")
    image_description = data.get("image_description", "")
    origin_path = Path(data.get("origin_path", ""))
    origin_file_name = origin_path.name
    origin_id = str(uuid.uuid4())  # 문서 단위 ID
    total_page = data.get("total_page")
    page_number = data.get("page_number")
    page_image_path = data.get("page_image_path", "")

    strategy = RecursiveStrategy()
    chunks = strategy.chunking(text, chunk_size=chunk_size, overlap=overlap_size)
    results = []
    for idx, chunk in enumerate(chunks):
        chunk_content = chunk["Content"]

        chunk_embedding = embedding(text=chunk_content)
        chunk_embedding__1024 = chunk_embedding[:1024]

        image_description_embedding = embedding(text=image_description)
        print(f">>{len(image_description_embedding)}")
        image_description_embedding__1024 = image_description_embedding[:1024]

        result = {
            "origin_id": origin_id,
            "chunk_id": idx,
            "chunk_content": chunk_content,
            "chunk_embedding": chunk_embedding,
            "chunk_embedding__1024": chunk_embedding__1024,
            "image_description_embedding": image_description_embedding,
            "image_description_embedding__1024": image_description_embedding__1024,
            "page_number": page_number,
            "total_page": total_page,
            "origin_file_name": origin_file_name,
            "origin_file_path": str(origin_path),
            "image_description": image_description,
            "page_image_path": page_image_path
        }
        results.append(result)

    return results

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
        chunked_data = build_chunk(msg)
        for chunk in chunked_data:
            index_to_opensearch(chunk, )
        print("[✅ Received and transformed]")
    except Exception as e:
        print(f"[❌ Error handling message] {e}")
        print(body)


def ensure_index_exists():
    global INDEX

    index_name = INDEX
    if not client.indices.exists(index=index_name):
        print(f"[ℹ️  Index '{index_name}' not found. Creating it...]")
        client.indices.create(
            index=index_name,
            body = {
                "settings": {
                    "index": {
                        "knn": True,
                    },
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
                        "chunk_content": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "image_description": {
                            "type": "text",
                            "analyzer": "nori_analyzer"
                        },
                        "chunk_embedding": {
                            "dimension": 3072,
                            "method": {
                                "engine": "nmslib",
                                "space_type": "cosinesimil",
                                "name": "hnsw",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 24
                                }
                            },
                            "type": "knn_vector"                        
                        },
                        "chunk_embedding__1024": {
                            "dimension": 1024,
                            "method": {
                                "engine": "nmslib",
                                "space_type": "cosinesimil",
                                "name": "hnsw",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 24
                                }
                            },
                            "type": "knn_vector"     
                        },
                        "image_description_embedding": {
                            "dimension": 3072,
                            "method": {
                                "engine": "nmslib",
                                "space_type": "cosinesimil",
                                "name": "hnsw",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 24
                                }
                            },
                            "type": "knn_vector"
                        },
                        "image_description_embedding__1024": {
                            "dimension": 1024,
                            "method": {
                                "engine": "nmslib",
                                "space_type": "cosinesimil",
                                "name": "hnsw",
                                "parameters": {
                                    "ef_construction": 128,
                                    "m": 24
                                }
                            },
                            "type": "knn_vector"
                        },
                        "page_number": { "type": "integer" },
                        "total_page": { "type": "integer" },
                        "origin_file_name": { "type": "keyword" },
                        "origin_file_path": { "type": "keyword" },
                        "page_image_path": { "type": "keyword" },
                        "origin_id": { "type": "keyword" },
                        "chunk_id": { "type": "keyword" }
                    }
                }
            }
        )
        print(f"[✅ Index '{index_name}' created.]")
    else:
        print(f"[✅ Index '{index_name}' already exists.]")

def main():
    ensure_index_exists()
    username = settings.RABBITMQ_DEFAULT_USER
    password = settings.RABBITMQ_DEFAULT_PASS

    print(f"{username}:{password}:::{settings.RABBITMQ_HOST}")
    credentials = pika.PlainCredentials(username, password)
    parameters = pika.ConnectionParameters(host="localhost", credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='pdf_metadata', durable=True)

    channel.basic_consume(queue="pdf_metadata", on_message_callback=callback, auto_ack=True)
    print("[*] Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()

