import pika
import json
from app.core.config import settings

class MQPublisher:
    def __init__(self, host: str):
        username = settings.RABBITMQ_DEFAULT_USER
        password = settings.RABBITMQ_DEFAULT_PASS
        port = settings.RABBITMQ_PORT

        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(host=host, port=port, credentials=credentials)

        self.connection = pika.BlockingConnection(parameters)
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='pdf_metadata', durable=True)

    def publish(self, message: dict):
        self.channel.basic_publish(
            exchange='',
            routing_key='pdf_metadata',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # make message persistent
        )

    def close(self):
        self.connection.close()
