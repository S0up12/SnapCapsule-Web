FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SNAPCAPSULE_DATA_DIR=/data \
    SNAPCAPSULE_DATABASE_DIR=/data/database \
    SNAPCAPSULE_CACHE_DIR=/data/cache \
    SNAPCAPSULE_IMPORTS_DIR=/data/imports

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libgl1 libglib2.0-0 p7zip-full unrar-free \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY src ./src

RUN mkdir -p /data/database /data/cache /data/imports

WORKDIR /app/src

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
