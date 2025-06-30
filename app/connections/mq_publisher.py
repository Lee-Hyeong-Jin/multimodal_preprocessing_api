import pika
import json
from app.core.config import settings

class MQPublisher:
    @staticmethod
    def publish(message: dict):
        credentials = pika.PlainCredentials(
            settings.RABBITMQ_DEFAULT_USER,
            settings.RABBITMQ_DEFAULT_PASS
        )
        parameters = pika.ConnectionParameters(
            host=settings.RABBITMQ_HOST,
            port=settings.RABBITMQ_PORT,
            credentials=credentials
        )

        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            channel.queue_declare(queue='pdf_metadata', durable=True)
            channel.basic_publish(
                exchange='',
                routing_key='pdf_metadata',
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
            )
        finally:
            if connection and connection.is_open:
                connection.close()

