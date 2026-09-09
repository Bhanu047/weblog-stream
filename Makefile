.PHONY: install test produce ingest sessions report kafka-up kafka-down demo clean

install:
	pip install -r requirements-dev.txt

test:
	pytest -q

# --- File-source path: no broker needed ------------------------------------

produce:
	python -m src.weblog.cli produce --count 3000

ingest:
	python -m src.weblog.cli ingest --source files --once

sessions:
	python -m src.weblog.cli sessions --source files --once

report:
	python -m src.weblog.cli report

demo: produce ingest sessions report

# --- Real broker -----------------------------------------------------------

kafka-up:
	docker compose up -d
	@echo "waiting for the broker to accept connections..."
	@until docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 --list >/dev/null 2>&1; do sleep 2; done
	@echo "kafka is up on localhost:9092"

kafka-down:
	docker compose down -v

clean:
	rm -rf data warehouse spark-warehouse metastore_db derby.log .pytest_cache
	find . -name __pycache__ -type d -exec rm -rf {} +
