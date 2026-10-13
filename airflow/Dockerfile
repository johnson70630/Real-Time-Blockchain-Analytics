FROM apache/airflow:3.1.8-python3.12

COPY --from=ghcr.io/astral-sh/uv:0.11.17 /uv /usr/local/bin/uv

USER root
COPY airflow/requirements-warehouse.txt /tmp/requirements-warehouse.txt
COPY pyproject.toml uv.lock /tmp/project-dependencies/
RUN apt-get update \
    && apt-get install --yes --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && python -m venv /opt/warehouse-venv \
    && /opt/warehouse-venv/bin/pip install --no-cache-dir \
        -r /tmp/requirements-warehouse.txt \
    && python -m venv /opt/project-venv \
    && cd /tmp/project-dependencies \
    && VIRTUAL_ENV=/opt/project-venv uv sync \
        --active --frozen --no-dev --no-install-project

USER airflow
