FROM node:22-alpine AS frontend-builder

WORKDIR /frontend

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build


FROM python:3.12-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

FROM python:3.12-slim AS runner

WORKDIR /app

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY src ./src
COPY pyproject.toml .
COPY README.md .
COPY --from=frontend-builder /frontend/dist ./frontend/dist

RUN pip install --no-cache-dir --no-deps -e .

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

EXPOSE 8000

CMD ["uvicorn", "taskforge.main:app", "--host", "0.0.0.0", "--port", "8000"]

