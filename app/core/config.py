from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    VLM_URL: str = ""
    VLM_MODEL_NAME: str = "gpt-4o"
    VLM_API_KEY: str = ""

    RABBITMQ_HOST: str = "localhost"
    RABBITMQ_DEFAULT_USER: str = "guest"
    RABBITMQ_DEFAULT_PASS: str = "guest"

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()

