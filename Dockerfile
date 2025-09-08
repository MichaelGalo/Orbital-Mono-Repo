# Use the official lightweight Python image
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml ./

RUN pip install --upgrade pip \
    && pip install uv \
    && uv install  # <-- installs dependencies based on pyproject.toml, no uv.lock needed

COPY . .

EXPOSE 8080

CMD ["uv", "run", "src/runner.py"]
