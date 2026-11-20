FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# 实际启动命令由 docker-compose 的 command 指定(网关/打印机/API 复用同一镜像)
CMD ["python", "-m", "app.gateway"]
