FROM python:3.11-slim
ENV PYTHONUNBUFFERED=1 DAGSTER_HOME=/app/dagster_home TAXI_DB=/app/data/warehouse.duckdb TAXI_DATA_DIR=/app/data/raw
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p $DAGSTER_HOME data && cp dagster.yaml $DAGSTER_HOME/dagster.yaml && cd dbt && dbt parse --profiles-dir .
EXPOSE 3000
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-m", "taxi_platform.definitions"]
