.PHONY: stack stack-down stack-config
# Default Go + Python local stack (see compose.yaml).
stack:
	docker compose -f compose.yaml up --build

stack-down:
	docker compose -f compose.yaml down

stack-config:
	docker compose -f compose.yaml config --quiet
