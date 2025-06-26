# 1. Python 베이스 이미지
FROM python:3.11-slim

# 3. 작업 디렉토리 설정
WORKDIR /app

# 4. 의존성 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. 애플리케이션 코드 복사
COPY ./app ./app

# 6. 포트 노출 및 실행 커맨드
CMD ["python", "app/consumer/dwg_metadata_consumer.py"]

