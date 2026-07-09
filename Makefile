PYTHON ?= python

.PHONY: lint test pipeline synth bronze silver gold residual lineage

lint:
	$(PYTHON) -m ruff check src tests

test:
	$(PYTHON) -m pytest -q

pipeline:
	$(PYTHON) -m tech_cost_platform.pipeline

synth:
	$(PYTHON) -m tech_cost_platform.synth

bronze:
	$(PYTHON) -m tech_cost_platform.bronze

silver:
	$(PYTHON) -m tech_cost_platform.silver

gold:
	$(PYTHON) -m tech_cost_platform.pipeline --stage gold

residual:
	$(PYTHON) -m tech_cost_platform.residual

lineage:
	$(PYTHON) -m tech_cost_platform.lineage
