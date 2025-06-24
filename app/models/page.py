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
                        "text": "이 이미지는 PDF 문서의 한 페이지입니다. 전체 내용을 이해한 뒤, 핵심 내용을 한 문단으로 요약해 주세요. 표나 이미지가 있어도 무시하지 말고 포함하세요."
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
            raise Exception(f"[Page {self.page_number}] OpenAI GPT 요약 실패: {e} (파일: {self.page_file_path})")

