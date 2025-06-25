import boto3
from io import BytesIO
from app.core.config import settings


def upload_image_to_os(img: BytesIO, key:str, suffix: str = ".jpg") -> str:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=settings.OS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.OS_SECRET_ACCESS_KEY,
        #endpoint_url=settings.OS_S3_ENDPOINT_URL,
        region_name=settings.OS_REGION,
    )

    key = f"{key}{suffix}"
    img.seek(0)
    s3.upload_fileobj(img, settings.OS_BUCKET_NAME, key, ExtraArgs={"ContentType": "image/jpeg"})

    # return f"{settings.OS_ENDPOINT_URL}/{settings.OS_BUCKET_NAME}/{key}"
    return f"https://{settings.OS_BUCKET_NAME}.s3.{settings.OS_REGION}.amazonaws.com/{key}"

