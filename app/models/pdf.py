import fitz
from pathlib import Path
from typing import List
from pydantic import BaseModel
from app.models.page import PageData
from app.models.metadata import Metadata
from io import BytesIO
from app.utils.object_storage.minio import upload_image_to_os

class PDF(BaseModel):
    origin_file_path: str
    file_path: str
    pages: List[PageData] = []

    def preprocess(self):
        pdf_path = Path(self.file_path)
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        output_dir = "multimodal_manual" / pdf_path.parent / f"{pdf_path.stem}_pages"

        for i in range(total_pages):
            page = doc[i]
            has_image = bool(page.get_images(full=True))
            page_text = page.get_text()
        
            pix = page.get_pixmap(dpi=200)
            img_bytes = BytesIO(pix.tobytes("jpeg"))
            bucket_key = output_dir / pdf_path.name

            # S3에 업로드
            try:
                os_url = upload_image_to_os(img_bytes, key=str(bucket_key))
            except Exception as e:
                print(f"[❌ Page {i+1}] S3 업로드 실패: {e}")
                continue

            page_obj = PageData(
                page_number=i + 1,
                page_text=page_text,  # LLM 추출로 대체될 예정
                page_summary="",
                has_image=has_image,
                total_page=total_pages,
                page_image_path=os_url,
            )

            try:
                page_obj.make_page_summary()
            except Exception as e:
                print(f"[❌ Page {i+1}] 마크다운 추출 실패: {e}")
                continue

            self.pages.append(page_obj)

            try:
                metadata = Metadata.from_page(page_obj, self.origin_file_path)
                metadata.enqueue()
            except Exception as e:
                print(f"[⚠️ Page {i+1}] 메타데이터 전송 실패: {e}")

