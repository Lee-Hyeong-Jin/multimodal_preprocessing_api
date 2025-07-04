from pydantic import BaseModel
from app.core.di import get_mq_publisher

class Metadata(BaseModel):
    origin_path: str
    page_number: int
    has_image: bool
    has_table: bool
    page_summary: str
    page_text: str
    total_page: int

    """
    page_number: int
    page_text: str  # 비워둘 수도 있음
    page_summary: str
    has_image: bool
    total_page: int
    has_table: bool
    page_file_path: str
    """
    @staticmethod
    def from_page(page, origin_path):
        return Metadata(
            origin_path=origin_path,
            page_number=page.page_number,
            has_image=page.has_image,
            has_table=page.has_table,
            page_summary=page.page_summary,
            page_text=page.page_text,
            total_page=page.total_page
        )

    def enqueue(self):
        mq_publisher = get_mq_publisher()
        if not mq_publisher:
            raise RuntimeError("MQPublisher is not initialized.")
        mq_publisher.publish(self.model_dump())

