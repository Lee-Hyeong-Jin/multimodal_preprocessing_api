from pydantic import BaseModel
from openai import OpenAI
import base64
from app.core.config import settings
import requests

class PageData(BaseModel):
    page_number: int
    page_text: str  
    page_summary: str
    has_image: bool
    total_page: int
    page_image_path: str

    @staticmethod
    def get_page_summary_prompt(img_b64: str):
        return [
            {"role": "system", "content": "You are a helpful assistant that summarizes scanned document images."},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}
                    },
                    {
                        "type": "text",
                        "text": "Please describe the attached image and table in full details, with a description of each object in the image.If the attached is a screenshot of a document chunk with multiple images in it, then you MUST describe details per image and table.Briefly summarize the above considerations in one sentence for each image and table. Answer in Korean only."
                    }
                ]
            }
        ]

    def make_page_summary(self):
        client = OpenAI(api_key=settings.VLM_API_KEY)

        try:
            response = requests.get(self.page_image_path)
            if response.status_code != 200:
                raise Exception(f"이미지 다운로드 실패: {response.status_code}")

            img_b64 = base64.b64encode(response.content).decode("utf-8")

            response = client.chat.completions.create(
                model=settings.VLM_MODEL_NAME,
                messages=self.get_page_summary_prompt(img_b64),
                temperature=0.3,
            )

            self.page_summary = response.choices[0].message.content.strip()

        except Exception as e:
            raise Exception(f"[Page {self.page_number}] OpenAI GPT 요약 실패: {e} (파일: {self.page_image_path})")

