FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml uv.lock ./

RUN pip install --upgrade pip \
    && pip install uv \
    && uv sync --locked

COPY . .

EXPOSE 8080

CMD ["sh", "-c", "uv run src/runner.py --host 0.0.0.0 --port ${PORT:-8080}"]