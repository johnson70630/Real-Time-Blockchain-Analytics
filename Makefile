.PHONY: kafka-up kafka-down producer bronze silver gold dashboard test lint check clean-data

kafka-up:
	docker compose up -d

kafka-down:
	docker compose down

producer:
	uv run python -m producer.run_producer

bronze:
	uv run python spark/write_swaps_bronze.py

silver:
	uv run python spark/build_swaps_silver.py

gold:
	uv run python spark/build_swaps_gold.py

dashboard:
	uv run streamlit run dashboard/app.py

lint:
	uv run ruff check .

test:
	uv run pytest

check: lint test

clean-data:
	rm -rf data/bronze data/silver data/gold data/checkpoints