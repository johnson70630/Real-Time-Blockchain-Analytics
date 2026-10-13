FROM apache/airflow:3.1.8-python3.12

USER root
COPY requirements-warehouse.txt /tmp/requirements-warehouse.txt
RUN python -m venv /opt/warehouse-venv \
    && /opt/warehouse-venv/bin/pip install --no-cache-dir \
        -r /tmp/requirements-warehouse.txt

USER airflow
