.PHONY: kafka-up kafka-down producer producer-build producer-up producer-logs producer-down market-data market-data-build market-data-up market-data-logs market-data-down uniswap-pools uniswap-pools-bootstrap token-metadata aave-backfill chainlink-backfill bronze market-data-bronze market-data-silver silver aave-silver price-enrichment gold snowflake-sql dashboard data-lab quality test lint check clean-data

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

dashboard:
	uv run python -m streamlit run dashboard/app.py

data-lab:
	uv run python -m streamlit run app/app.py

lint:
	uv run ruff check .

quality:
	uv run python -m tests.data_quality_check

test:
	uv run pytest

check: lint test

clean-data:
	rm -rf data/bronze data/silver data/gold data/checkpoints data/state
