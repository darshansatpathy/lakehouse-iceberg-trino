.PHONY: install up down wait check lint test verify bench clean

install:
	pip install -e ".[dev]"

up:
	docker compose up -d postgres minio create-bucket trino
	scripts/wait-for-trino.sh

down:
	docker compose down -v

wait:
	scripts/wait-for-trino.sh

lint:
	ruff check .
	ruff format --check .
	mypy

# With the stack up, LH_INTEGRATION=1 also runs the tests against it.
test:
	pytest --cov --cov-report=term-missing

check: lint test

verify:
	lh verify --check --report docs/verification.md

bench:
	lh bench --report docs/benchmark.md

clean:
	rm -rf transform/target transform/logs
