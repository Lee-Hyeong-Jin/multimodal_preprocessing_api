import pika
import json
import uuid
from pathlib import Path

def transform_message(msg: dict) -> dict:
    origin_path = Path(msg["origin_path"])
    origin_file_name = origin_path.name
    origin_id = str(uuid.uuid4())
    chunk_id = str(uuid.uuid4())

    return {
        "origin_id": origin_id,
        "chunk_id": chunk_id,
        "chunk_content": msg.get("page_summary", ""),
        "chunk_embedding": "",  # 추후 처리
        "chunk_embedding_1024": "",  # 추후 처리
        "page_number": msg.get("page_number"),
        "total_page": msg.get("total_page"),
        "origin_file_name": origin_file_name,
        "origin_file_path": str(origin_path)
    }

def callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        transformed = transform_message(msg)
        print("[✅ Received and transformed]")
        print(json.dumps(transformed, indent=2, ensure_ascii=False))
    except Exception as e:
        print(f"[❌ Error handling message] {e}")
        print(body)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host="rabbitmq"))
    channel = connection.channel()
    channel.queue_declare(queue="pdf_metadata", durable=True)

    channel.basic_consume(queue="pdf_metadata", on_message_callback=callback, auto_ack=True)
    print("[*] Waiting for messages...")
    channel.start_consuming()

if __name__ == "__main__":
    main()

