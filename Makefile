COMPOSE ?= docker compose

.PHONY: dev down logs smoke demo chaos bench

dev:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

smoke:
	./scripts/smoke.sh

demo:
	python3 scripts/demo.py

chaos:
	./scripts/chaos.sh

bench:
	python3 scripts/bench.py
