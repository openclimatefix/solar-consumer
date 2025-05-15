# --- Dependency builder image --- #
FROM python:3.12-slim-bookworm AS build-deps

# Install build tools
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
RUN apt-get update && apt-get install -y git

# Project configuration
WORKDIR /app
COPY pyproject.toml /app/pyproject.toml

# UV settings
ENV UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    UV_LINK_MODE=copy

# Pre-install dependencies
RUN mkdir src && \
    uv sync --no-dev --no-install-project --no-editable

# Optional: clean up large test dirs (e.g., pandas)
RUN rm -rf /app/.venv/lib/python3.12/site-packages/**/tests

# --- App builder image --- #
FROM build-deps AS build-app

# Install full project
COPY . /app
RUN uv sync --no-dev --no-editable

# --- Final runtime image --- #
FROM python:3.12-slim-bookworm

WORKDIR /app
COPY --from=build-app --chown=app:app /app/.venv /app/.venv
COPY --from=build-app /app /app

# Entrypoint
ENTRYPOINT ["/app/.venv/bin/python", "-m", "solar_consumer.app"]
