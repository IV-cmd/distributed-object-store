COMPOSE ?= docker compose

.PHONY: dev down logs test smoke demo chaos erasure bench

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

test:
	cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Debug
	cmake --build build --parallel 2
	ctest --test-dir build --output-on-failure

erasure:
	python3 scripts/erasure.py
