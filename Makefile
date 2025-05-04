.PHONY: clean build test deploy

clean:
	rm -rf build dist src/*.egg-info

build: clean
	python3 -m build --wheel

test:
	pytest tests/

test/deploy:
	twine upload --repository testpypi --verbose dist/*

deploy:
	twine upload dist/*
