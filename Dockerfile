FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml /app

RUN pip install --no-cache-dir .

COPY . /app

# Set the entry point for the container
CMD ["python", "-m", "solar_consumer.app"]
