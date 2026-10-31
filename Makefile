.PHONY: up test lint run docs
run:        ## load two sample months, build dbt, run checks
	python run_pipeline.py 2023-01 2023-02
up:         ## Dagster UI on http://localhost:3000
	docker compose up --build
test:
	pytest -q
lint:
	ruff check . && ruff format --check .
docs:       ## dbt docs site with column-level lineage
	cd dbt && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir .
