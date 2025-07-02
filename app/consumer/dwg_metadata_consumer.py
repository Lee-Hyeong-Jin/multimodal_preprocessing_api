from typing import Dict
import pika
import json
import logging
import psycopg2
import sys
from psycopg2.extras import execute_values
from app.core.config import settings
from app.utils.embedding.embedding_test import embedding
from psycopg2.extras import Json

# ✅ 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def create_table_if_not_exists():
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASS
    )
    cursor = conn.cursor()
    create_query = """
    CREATE TABLE IF NOT EXISTS multimodal.dwg_metadata_chunk_table (
        id SERIAL PRIMARY KEY,
        drawing_id TEXT,
        summary TEXT,
        image_path TEXT,
        image_type TEXT,
        image_description TEXT,
        textual_info TEXT,
        info_project TEXT,
        info_title TEXT,
        info_dwg_no TEXT,
        info_rev TEXT,
        info_scale TEXT,
        parts JSONB,
        _dwg_filename TEXT,
        _dwg_filepath TEXT,
        _num_total_images INTEGER,
        image_url TEXT,
        textual_info_embedding FLOAT8[],
        textual_info_embedding__1024 FLOAT8[],
        image_description_embedding FLOAT8[],
        image_description_embedding__1024 FLOAT8[],
        created_date TIMESTAMPTZ DEFAULT NOW(),
        updated_date TIMESTAMPTZ DEFAULT NOW(),
        indexed_date TIMESTAMPTZ DEFAULT NULL
    );

    CREATE OR REPLACE FUNCTION update_updated_date()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_date = NOW();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS set_updated_date ON multimodal.dwg_metadata_chunk_table;

    CREATE TRIGGER set_updated_date
    BEFORE UPDATE ON multimodal.dwg_metadata_chunk_table
    FOR EACH ROW
    EXECUTE PROCEDURE update_updated_date();
    """
    try:
        cursor.execute(create_query)
        conn.commit()
        logger.info("✅ PostgreSQL table created or already exists")
    except Exception as e:
        logger.exception("❌ PostgreSQL table creation failed")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()


def insert_to_postgres(data: Dict) -> bool:
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASS
    )
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO multimodal.dwg_metadata_chunk_table (
            drawing_id, summary, image_path, image_type, image_description,
            textual_info, info_project, info_title, info_dwg_no, info_rev,
            info_scale, parts, _dwg_filename, _dwg_filepath,
            _num_total_images, image_url, textual_info_embedding, textual_info_embedding__1024,
            image_description_embedding, image_description_embedding__1024
        )
        VALUES %s
    """

    values = [(
        data.get("drawing_id", ""),
        data.get("summary", ""),
        data.get("image_path", ""),
        data.get("image_type", ""),
        data.get("image_description", ""),
        data.get("textual_info", ""),
        data.get("info_project", ""),
        data.get("info_title", ""),
        data.get("info_dwg_no", ""),
        data.get("info_rev", ""),
        data.get("info_scale", ""),
        Json(data.get("parts", [])),
        data.get("_dwg_filename", ""),
        data.get("_dwg_filepath", ""),
        data.get("_num_total_images", 0),
        data.get("image_url", ""),
        data.get("textual_info_embedding", []),
        data.get("textual_info_embedding__1024", []),
        data.get("image_description_embedding", []),
        data.get("image_description_embedding__1024", [])
    )]

    try:
        execute_values(cursor, insert_query, values)
        conn.commit()
        logger.info("✅ Drawing data inserted into PostgreSQL")
        return True
    except Exception as e:
        logger.exception("❌ PostgreSQL insert failed")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()


def build_chunk(data: Dict) -> Dict:
    textual_info = data.get("textual_info", "")
    image_description = data.get("image_description", "")

    textual_info_embedding = embedding(text=textual_info)
    textual_info_embedding__1024 = textual_info_embedding[:1024]

    image_description_embedding = embedding(text=image_description)
    image_description_embedding__1024 = image_description_embedding[:1024]

    return {
        "drawing_id": data.get("drawing_id", ""),
        "summary": data.get("summary", ""),
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
        "_num_total_images": data.get("_num_total_images", 0),
        "image_url": data.get("image_url", ""),
        "textual_info_embedding": textual_info_embedding,
        "textual_info_embedding__1024": textual_info_embedding__1024,
        "image_description_embedding": image_description_embedding,
        "image_description_embedding__1024": image_description_embedding__1024
    }


def callback(ch, method, properties, body):
    try:
        logger.info("📥 Received message from queue")
        msg = json.loads(body)
        chunk = build_chunk(msg)
        success = insert_to_postgres(chunk)

        if success:
            ch.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("✅ Message processed and acknowledged")
        else:
            logger.warning("⚠️ Insert failed — message will be retried (not acknowledged)")
    except Exception as e:
        logger.exception("❌ Unexpected error during message processing")


def main():
    create_table_if_not_exists()

    credentials = pika.PlainCredentials(settings.RABBITMQ_DEFAULT_USER, settings.RABBITMQ_DEFAULT_PASS)
    parameters = pika.ConnectionParameters(host=settings.RABBITMQ_HOST, port=settings.RABBITMQ_PORT, credentials=credentials)

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue='multimodal_drawing', durable=True)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue='multimodal_drawing', on_message_callback=callback, auto_ack=False)

    logger.info("🚀 Worker started. Waiting for messages...")
    channel.start_consuming()


if __name__ == "__main__":
    main()

