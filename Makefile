.PHONY: install test lint demo clean

install:
	pip install -e ".[dev]"

test:
	pytest -v

lint:
	ruff check src tests scripts

demo:
	python scripts/generate_sample_data.py --out data/input
	dataops-pipeline
	@echo "Re-running to show idempotency (expect read=0):"
	dataops-pipeline

clean:
	rm -rf data/ .pytest_cache .ruff_cache
