.PHONY: clean build test deploy

clean:
	rm -rf build dist src/*.egg-info

build: clean
	python setup.py sdist bdist_wheel

test:
	pytest tests/

test/deploy:
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*

deploy:
	twine upload dist/*
