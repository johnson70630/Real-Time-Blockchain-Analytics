.PHONY: kafka-up kafka-down producer producer-build producer-up producer-logs producer-down bronze silver gold dashboard quality test lint check clean-data

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

bronze:
	uv run python -m spark.write_swaps_bronze

silver:
	uv run python -m spark.build_swaps_silver

gold:
	uv run python -m spark.build_gold

dashboard:
	uv run python -m streamlit run dashboard/app.py

lint:
	uv run ruff check .

quality:
	uv run python -m tests.data_quality_check

test:
	uv run pytest

check: lint test

clean-data:
	rm -rf data/bronze data/silver data/gold data/checkpoints data/state
