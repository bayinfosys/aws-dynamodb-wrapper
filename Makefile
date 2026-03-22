.PHONY: build test test-unit install clean deploy

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache
	rm -rf *.egg-info

build: clean
	python3 -m build --wheel

test/deploy:
	twine upload --repository testpypi --verbose dist/*

test: test-unit

test-unit:
	python -m pytest tests/test_dynamodb_backend.py tests/test_dbitem.py -v

install:
	pip install -e ".[dev,dynamodb]"

deploy:
	twine upload dist/*
