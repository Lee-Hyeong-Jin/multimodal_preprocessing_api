from fastapi import APIRouter
from app.models.pdf import PDF
from pydantic import BaseModel

router = APIRouter()

class ProcessRequest(BaseModel):
    file_path: str
    origin_path: str

@router.post("/process")
def process(request: ProcessRequest):
    pdf = PDF(origin_file_path=request.origin_path, file_path=request.file_path)
    pdf.preprocess()
    return {"message": f"{len(pdf.pages)} pages processed."}
