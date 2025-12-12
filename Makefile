DC := docker compose run --rm

.PHONY: test lint

test-order:
	$(DC) order-service pytest -vv

lint:
	$(DC) order-service ruff check --fix . && ruff format .

create-migration-order:
	$(DC) order-service bash -c "alembic revision --autogenerate"

create-migration-payment:
	$(DC) payment-service alembic revision --autogenerate

create-migration-shipping:
	$(DC) shipping-service alembic revision --autogenerate

setup-database-order:
	$(DC) bash -c "alembic upgrade head"

setup-database-payment:
	$(DC) payment-service alembic upgrade head

setup-database-shipping:
	$(DC) shipping-service alembic upgrade head

start: setup-database-order
	$(DC) up -d

stop:
	$(DC) down