FROM node:20-alpine AS frontend-builder

WORKDIR /app/ui

COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci

COPY frontend/ ./
RUN npm run build


FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y supervisor && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=frontend-builder /app/ui/dist /app/ui/dist

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

EXPOSE 8000 8081

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
