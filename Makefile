SHELL := /bin/bash

.PHONY: up down reset schemas logs

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f --tail=200

schemas:
	bash scripts/apply_schemas.sh

