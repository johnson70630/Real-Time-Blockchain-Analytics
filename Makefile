.PHONY: kafka-up kafka-down producer producer-build producer-up producer-logs producer-down market-data market-data-build market-data-up market-data-logs market-data-down uniswap-pools uniswap-pools-bootstrap uniswap-recovery-bootstrap token-metadata aave-backfill chainlink-backfill bronze market-data-bronze market-data-silver silver aave-silver price-enrichment gold snowflake-sql snowflake-load dashboard data-lab freshness-check airflow-init airflow-up airflow-down airflow-logs airflow-test terraform-fmt terraform-init terraform-validate ci ci-python ci-spark ci-app ci-dbt ci-secrets ci-terraform ci-airflow quality test lint check clean-data

TERRAFORM_ROOTS := infra/terraform/bootstrap/state infra/terraform/environments/prod

kafka-up:
	docker compose up -d kafka

kafka-down:
	docker compose down

producer:
	uv run python -m producer.run_producer

producer-build:
	docker compose build producer

producer-up:
	docker compose up -d producer

producer-logs:
	docker compose logs -f producer

producer-down:
	docker compose stop producer

market-data:
	uv run python -m market_data.run_poller

market-data-build:
	docker compose build market-data-poller

market-data-up:
	docker compose up -d market-data-poller

market-data-logs:
	docker compose logs -f market-data-poller

market-data-down:
	docker compose stop market-data-poller

uniswap-pools:
	uv run python -m reference_data.run_uniswap_v3_pools

uniswap-pools-bootstrap:
	uv run python -m reference_data.run_uniswap_v3_pool_bootstrap

uniswap-recovery-bootstrap:
	uv run python -m backfill.run_uniswap_recovery

token-metadata:
	uv run python -m reference_data.run_token_metadata

aave-backfill:
	uv run python -m backfill.run_aave_v3

chainlink-backfill:
	uv run python -m backfill.run_chainlink

bronze:
	uv run python -m spark.write_swaps_bronze

market-data-bronze:
	uv run python -m spark.write_market_data_bronze

market-data-silver:
	uv run python -m spark.build_market_data_silver

silver:
	uv run python -m spark.build_swaps_silver

aave-silver:
	uv run python -m spark.build_aave_silver

price-enrichment:
	uv run python -m spark.build_price_enriched_silver

gold:
	uv run python -m spark.build_gold

snowflake-sql:
	uv run python -m warehouse.generate_sql

snowflake-load:
	uv run python -m warehouse.load_raw

dashboard:
	uv run python -m streamlit run dashboard/app.py

data-lab:
	uv run python -m streamlit run streamlit_app.py

freshness-check:
	uv run python -m orchestration.run_freshness_checks

airflow-init:
	docker compose -f airflow/docker-compose.yaml up airflow-init

airflow-up:
	docker compose -f airflow/docker-compose.yaml up -d

airflow-down:
	docker compose -f airflow/docker-compose.yaml down

airflow-logs:
	docker compose -f airflow/docker-compose.yaml logs -f airflow-api-server airflow-scheduler airflow-dag-processor

airflow-test:
	docker compose -f airflow/docker-compose.yaml run --rm --entrypoint python airflow-cli -m unittest discover -s /opt/airflow/tests -v

terraform-fmt:
	terraform fmt -check -recursive infra/terraform

terraform-init:
	@set -e; for root in $(TERRAFORM_ROOTS); do \
		terraform -chdir=$$root init -backend=false -input=false; \
	done

terraform-validate: terraform-init
	@set -e; for root in $(TERRAFORM_ROOTS); do \
		terraform -chdir=$$root validate; \
	done

ci-python:
	SPARK_LOCAL_IP=127.0.0.1 uv run pytest \
		--ignore=tests/test_chainlink_bootstrap.py \
		--ignore=tests/test_market_data_silver.py \
		--ignore=tests/test_price_enrichment.py \
		--ignore=tests/test_data_lab.py \
		--ignore=tests/test_aave_lending_page.py \
		--ignore=tests/test_pricing_lesson_pages.py \
		--ignore=tests/test_ui_polish.py \
		--ignore=tests/test_streamlit_deployment.py \
		--ignore=tests/test_operations_dashboard.py

ci-spark:
	SPARK_LOCAL_IP=127.0.0.1 uv run pytest \
		tests/test_chainlink_bootstrap.py \
		tests/test_market_data_silver.py \
		tests/test_price_enrichment.py

ci-app:
	uv run pytest \
		tests/test_data_lab.py \
		tests/test_aave_lending_page.py \
		tests/test_pricing_lesson_pages.py \
		tests/test_ui_polish.py \
		tests/test_streamlit_deployment.py \
		tests/test_operations_dashboard.py

ci-dbt:
	uv run dbt parse --project-dir dbt --profiles-dir ci/dbt \
		--target ci --no-partial-parse
	DBT_CI_OFFLINE_COMPILE=true uv run dbt compile \
		--project-dir dbt --profiles-dir ci/dbt --target ci \
		--no-populate-cache --no-introspect

ci-secrets:
	uv run python scripts/check_secrets.py --all-files

ci-terraform: terraform-fmt terraform-validate

release-test:
	uv run pytest tests/test_release_contracts.py tests/test_ci_contracts.py

ci-airflow:
	@mkdir -p dbt/logs dbt/target
	@set -e; trap '$(MAKE) airflow-down' EXIT; $(MAKE) airflow-test

ci: ci-secrets lint ci-python ci-spark ci-app ci-dbt ci-terraform ci-airflow

lint:
	uv run ruff check .

quality:
	uv run python -m tests.data_quality_check

test:
	uv run pytest

check: lint test

clean-data:
	rm -rf data/bronze data/silver data/gold data/checkpoints data/state
