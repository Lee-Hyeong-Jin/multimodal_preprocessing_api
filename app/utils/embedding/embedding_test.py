import openai
from app.core.config import settings

client = openai.OpenAI(api_key=settings.VLM_API_KEY)

def embedding(text: str) -> list[float]:
    try:
        response = client.embeddings.create(
            input=text,
            model="text-embedding-3-large"
        )
        return response.data[0].embedding
    except Exception as e:
        raise RuntimeError(f"[Embedding] OpenAI 임베딩 실패: {e}")

