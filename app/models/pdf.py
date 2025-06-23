import fitz
from pathlib import Path
from typing import List
from pydantic import BaseModel
from app.models.page import Page
from app.models.metadata import Metadata

class PDF(BaseModel):
    origin_file_path: str
    file_path: str
    pages: List[Page] = []

    def preprocess(self):
        pdf_path = Path(self.file_path)
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        output_dir = pdf_path.parent / f"{pdf_path.stem}_pages"
        output_dir.mkdir(parents=True, exist_ok=True)

        for i in range(total_pages):
            page = doc[i]
            has_image = bool(page.get_images(full=True))
            page_text = page.get_text()
        
            page_file_path = output_dir / f"{pdf_path.stem}_page_{i+1}.pdf"
            page_doc = fitz.open()
            page_doc.insert_pdf(doc, from_page=i, to_page=i)
            page_doc.save(page_file_path)

            page_obj = Page(
                page_number=i + 1,
                page_text=page_text,  # LLM 추출로 대체될 예정
                page_summary="",
                has_image=has_image,
                has_table=False,  # 추후 LLM이 판단
                total_page=total_pages,
                page_file_path=str(page_file_path),
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

