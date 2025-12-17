.PHONY: help venv install test test-vendor test-cov test-all clean lint format

## help - Display help about make targets for this Makefile
help:
	@cat Makefile | grep '^## ' --color=never | cut -c4- | sed -e "`printf 's/ - /\t- /;'`" | column -s "`printf '\t'`" -t

## venv - Create virtual environment
venv:
	python3 -m venv .venv
	.venv/bin/pip install --upgrade pip
	@echo ""
	@echo "Virtual environment created. Activate with:"
	@echo "  source .venv/bin/activate"

## install - Install package and dependencies in development mode
install:
	pip install -e .
	pip install pytest pytest-cov tox pre-commit ruff

## test - Run tests quickly
test:
	python -m pytest tests/ -v

## test-vendor - Run catalog vendor tests
test-vendor:
	python -m pytest tests/test_vendor/test_catalog_v1.py -v

## test-cov - Run tests with coverage report
test-cov:
	python -m pytest --cov=src --cov-report=term-missing --cov-report=html tests/ -v

## test-all - Run full test suite with tox (all Python/Pydantic versions)
test-all:
	tox

## lint - Run code quality checks
lint:
	pre-commit run --all-files

## format - Format code with ruff
format:
	ruff format src/ tests/

## clean - Remove build artifacts and cache
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	rm -rf .pytest_cache/
	rm -rf .tox/
	rm -rf htmlcov/
	rm -rf .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name '*.pyc' -delete
