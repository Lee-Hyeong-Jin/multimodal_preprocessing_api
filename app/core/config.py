from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # VLM Settings
    VLM_URL: str = ""
    VLM_MODEL_NAME: str = "gpt-4o"
    VLM_API_KEY: str = ""

    # RabbitMQ Settings
    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_DEFAULT_USER: str = "guest"
    RABBITMQ_DEFAULT_PASS: str = "guest"

    # ObjectStorage(OS) Settings
    OS_ACCESS_KEY_ID: str = ""
    OS_SECRET_ACCESS_KEY: str = ""
    OS_BUCKET_NAME: str = ""
    OS_REGION: str = "" 
    OS_ENDPOINT_URL: str = ""

    # Opensearch Settings
    OPENSEARCH_HOST: str = ""
    OPENSEARCH_USER: str = ""
    OPENSEARCH_PASS: str = ""
    OPENSEARCH_INITIAL_ADMIN_PASSWORD: str = ""

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

