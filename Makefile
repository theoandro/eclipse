ifneq (,$(wildcard .env))
	include .env
endif

SHELL := /bin/sh
.SHELLFLAGS = -ec
.SILENT:
.ONESHELL:
.DEFAULT_GOAL: help

help:
	echo "Please use \`make \033[36m<target>\033[0m\`"
	echo "\t where \033[36m<target>\033[0m is one of"
	grep -E '^\.PHONY: [a-zA-Z_-]+ .*?## .*$$' $(MAKEFILE_LIST) \
		| sort | awk 'BEGIN {FS = "(: |##)"}; {printf "• \033[36m%-30s\033[0m %s\n", $$2, $$3}'

.PHONY: install  ## Install dependencies
install:
	echo "[*] Installing dependencies with pip ..."
	pip install --no-cache-dir -e ".[build, dev]"

.PHONY: unit-tests  ## Run unit tests
unit-tests:
	echo "[*] Running unit tests..."
	pytest -m unit -p no:warnings -v --cov-report=html:reports/${PYTHON_VERSION}/pytest-report --junitxml=reports/${PYTHON_VERSION}/pytest-report.xml --cov-report=term --cov-report=xml:reports/${PYTHON_VERSION}/coverage.xml
	coverage report -m

.PHONY: build-package  ## build package locally
build-package:
	echo "[*] building wheel package..."
	python -m build --wheel
