COMPOSE ?= docker compose

.PHONY: dev down logs smoke

dev:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f

smoke:
	./scripts/smoke.sh
