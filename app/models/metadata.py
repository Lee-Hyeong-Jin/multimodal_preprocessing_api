from pydantic import BaseModel
from app.connections.mq_publisher import MQPublisher

class Metadata(BaseModel):
    origin_path: str
    page_number: int
    has_image: bool
    image_description: str
    page_text: str
    total_page: int
    page_image_path: str

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
            image_description=page.page_summary,
            page_text=page.page_text,
            total_page=page.total_page,
            page_image_path=page.page_image_path
        )

    def enqueue(self):
        MQPublisher.publish(self.model_dump())


