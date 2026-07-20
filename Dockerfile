FROM python:3.12-slim

ARG UV_VERSION=0.11.17

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_CACHE_DIR=/tmp/uv-cache \
    UV_LINK_MODE=copy

RUN pip install --no-cache-dir "uv==${UV_VERSION}"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project \
    && rm -rf /root/.cache/uv

COPY config ./config
COPY producer ./producer

RUN useradd --create-home --uid 10001 producer \
    && chown -R producer:producer /app

USER producer

CMD ["uv", "run", "--frozen", "--no-dev", "python", "-m", "producer.run_producer"]
