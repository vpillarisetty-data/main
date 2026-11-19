PROJECTS := $(wildcard projects/*)
PY ?= python

.PHONY: install install-spark test lint evals demo
install:            ## all packages, editable, plus dev tools
	$(PY) -m pip install pyyaml pytest ruff $(foreach p,$(PROJECTS),-e ./$(p))
install-spark:      ## Spark 3.5 + Delta 3.2 for the integration tests (Python <= 3.12)
	$(PY) -m pip install "pyspark==3.5.3" "delta-spark==3.2.1" pyarrow
test:
	$(PY) -m pytest
lint:
	ruff check projects conftest.py
evals:
	catalog-agent-evals
demo:
	catalog-agent-demo
