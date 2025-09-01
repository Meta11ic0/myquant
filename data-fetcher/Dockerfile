FROM python:3.11.9-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  PIP_NO_CACHE_DIR=1 

COPY requirements.txt .

RUN apt-get update && \
  apt-get install -y --no-install-recommends gcc libpq-dev build-essential && \
  pip install --no-cache-dir -r requirements.txt && \
  apt-get purge -y --auto-remove gcc build-essential && \
  rm -rf /var/lib/apt/lists/*

COPY src/ .

CMD ["python", "-u", "app.py"]
