# Convenience targets. Python 3.10+ stdlib only, no pip dependencies.

PYTHON ?= python3
SERVICE_URL ?= http://localhost:8080

.PHONY: help example run fleet test loadgen smoke baseline burst offline adversarial

help:
	@echo "Targets:"
	@echo "  run            run our service, single process, on :8080"
	@echo "  fleet          run our service as a WORKERS-process fleet on :8080 (burst scale)"
	@echo "  test           run the unit + integration test suite"
	@echo "  loadgen        concurrent load generator (override RATE/CONCURRENCY/DURATION)"
	@echo "  example        run the example stub service on :8080 (reference only)"
	@echo "  smoke          30s baseline run against \$$SERVICE_URL + scorecard"
	@echo "  baseline       60s baseline"
	@echo "  burst          3min run with two 10x bursts"
	@echo "  offline        2min run with 20% of devices going offline + replaying"
	@echo "  adversarial    4min run combining burst + offline + clock skew"
	@echo
	@echo "Override SERVICE_URL=... DEVICES=... WORKERS=... as needed."

DEVICES ?= 50
WORKERS ?= 4
RATE ?= 0
CONCURRENCY ?= 64
DURATION ?= 30

run:
	$(PYTHON) -m service

fleet:
	$(PYTHON) -m service.fleet --workers $(WORKERS)

test:
	$(PYTHON) -m unittest discover -s tests

loadgen:
	$(PYTHON) tools/loadgen.py --target $(SERVICE_URL) --rate $(RATE) \
		--concurrency $(CONCURRENCY) --duration $(DURATION)

example:
	$(PYTHON) example_solution/service.py

smoke:
	$(PYTHON) eval/check.py smoke --target $(SERVICE_URL) --devices $(DEVICES)

baseline:
	$(PYTHON) eval/check.py baseline --target $(SERVICE_URL) --devices $(DEVICES)

burst:
	$(PYTHON) eval/check.py burst --target $(SERVICE_URL) --devices $(DEVICES)

offline:
	$(PYTHON) eval/check.py offline --target $(SERVICE_URL) --devices $(DEVICES)

adversarial:
	$(PYTHON) eval/check.py adversarial --target $(SERVICE_URL) --devices $(DEVICES)
