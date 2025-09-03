FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1

# 复制并安装依赖
COPY requirements.txt .
RUN apt-get update && \
  apt-get install -y --no-install-recommends gcc && \
  pip install --no-cache-dir -r requirements.txt && \
  apt-get purge -y --auto-remove gcc && \
  rm -rf /var/lib/apt/lists/*

# 复制源码（开发时可以用 volume 挂载覆盖）
COPY src/ .

# 以 unbuffered 模式运行，日志能实时输出
CMD ["python", "-u", "app.py"]
