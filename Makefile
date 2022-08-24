# remove all build, test, coverage and Python artifacts
.PHONY: clean distclean lint install develop
.DEFAULT_GOAL := lint

clean: clean-pyc clean-test

distclean: clean-build clean-pyc clean-test

clean-build: ## remove build artifacts
	pip uninstall esgpull -y
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

lint: ## check style with flake8 and mypy
	flake8 esgpull tests migrations
	mypy esgpull tests migrations

install: distclean
	pip install -e .

develop: distclean
	pip install -e .[dev]

test:
	pytest

covtest:
	pytest --cov-report term-missing:skip-covered --cov=esgpull -vv

fulltest:
	pytest --runslow --cov-report term-missing:skip-covered --cov=esgpull -vv
