from typing import Dict, List
import pika
import json
import uuid
import logging
import sys
from pathlib import Path
from app.core.config import settings
from app.utils.chunking.recursive_chunking import RecursiveStrategy
from app.utils.embedding.embedding_test import embedding
import psycopg2
from psycopg2.extras import execute_values

# ✅ 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def build_chunk(data: Dict, chunk_size: int = 500, overlap_size: int = 100) -> List[Dict]:
    text = data.get("page_text", "")
    image_description = data.get("image_description", "")
    origin_path = Path(data.get("origin_path", ""))
    origin_file_name = origin_path.name
    origin_id = str(uuid.uuid4())
    total_page = data.get("total_page")
    page_number = data.get("page_number")
    page_image_path = data.get("page_image_path", "")

    strategy = RecursiveStrategy()
    chunks = strategy.chunking(text, chunk_size=chunk_size, overlap=overlap_size)

    results = []
    for idx, chunk in enumerate(chunks):
        chunk_content = chunk["Content"]

        chunk_embedding = embedding(text=chunk_content)[:1024]
        image_desc_embedding = embedding(text=image_description)[:1024]

        result = {
            "origin_id": origin_id,
            "chunk_id": idx,
            "chunk_content": chunk_content,
            "chunk_embedding": chunk_embedding,
            "chunk_embedding__1024": chunk_embedding,
            "image_description_embedding": image_desc_embedding,
            "image_description_embedding__1024": image_desc_embedding,
            "page_number": page_number,
            "total_page": total_page,
            "origin_file_name": origin_file_name,
            "origin_file_path": str(origin_path),
            "image_description": image_description,
            "page_image_path": page_image_path
        }
        results.append(result)

    return results


def insert_chunked_data(data: List[Dict]) -> bool:
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASS
    )
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO multimodal.manual_metadata_chunk_table (
            origin_id, chunk_id, chunk_content, chunk_embedding, chunk_embedding__1024,
            image_description_embedding, image_description_embedding__1024,
            page_number, total_page, origin_file_name, origin_file_path,
            image_description, page_image_path
        ) VALUES %s
    """

    values = [
        (
            row["origin_id"], row["chunk_id"], row["chunk_content"], row["chunk_embedding"],
            row["chunk_embedding__1024"], row["image_description_embedding"], row["image_description_embedding__1024"],
            row["page_number"], row["total_page"], row["origin_file_name"], row["origin_file_path"],
            row["image_description"], row["page_image_path"]
        ) for row in data
    ]

    try:
        execute_values(cursor, insert_query, values)
        conn.commit()
        logger.info(f"✅ Inserted {len(values)} chunks into PostgreSQL")
        return True
    except Exception as e:
        logger.error(f"❌ DB Insert Error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def callback(ch, method, properties, body):
    try:
        msg = json.loads(body)
        logger.info("📥 Received message from queue")
        chunked_data = build_chunk(msg)
        success = insert_chunked_data(chunked_data)

        if success:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("✅ Message processed and acknowledged")
        else:
            logger.warning("⚠️ Message processing failed — will be retried")
    except Exception as e:
        logger.exception(f"❌ Unexpected error while processing message: {e}")
        logger.warning("⚠️ Message will be retried due to processing error")


def create_table_if_not_exists():
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASS
    )
    cursor = conn.cursor()

    create_table_query = """
    CREATE SCHEMA IF NOT EXISTS multimodal;

    CREATE TABLE IF NOT EXISTS multimodal.manual_metadata_chunk_table (
        id SERIAL PRIMARY KEY,
        origin_id UUID,
        chunk_id INTEGER,
        chunk_content TEXT,
        chunk_embedding FLOAT8[],
        chunk_embedding__1024 FLOAT8[],
        image_description_embedding FLOAT8[],
        image_description_embedding__1024 FLOAT8[],
        page_number INTEGER,
        total_page INTEGER,
        origin_file_name TEXT,
        origin_file_path TEXT,
        image_description TEXT,
        page_image_path TEXT,
        created_date TIMESTAMPTZ DEFAULT NOW(),
        updated_date TIMESTAMPTZ DEFAULT NOW(),
        indexed_date TIMESTAMPTZ
    );

    CREATE OR REPLACE FUNCTION update_updated_date()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_date = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS set_updated_date ON multimodal.manual_metadata_chunk_table;

    CREATE TRIGGER set_updated_date
    BEFORE UPDATE ON multimodal.manual_metadata_chunk_table
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_date();
    """

    try:
        cursor.execute(create_table_query)
        conn.commit()
        logger.info("✅ Table check/creation complete")
    except Exception as e:
        logger.error(f"❌ Table creation error: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def main():
    create_table_if_not_exists()

    credentials = pika.PlainCredentials(
        settings.RABBITMQ_DEFAULT_USER,
        settings.RABBITMQ_DEFAULT_PASS
    )
    parameters = pika.ConnectionParameters(
        host=settings.RABBITMQ_HOST,
        port=settings.RABBITMQ_PORT,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='pdf_metadata', durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue="pdf_metadata", on_message_callback=callback, auto_ack=False)

    logger.info("📡 Worker started. Waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()

