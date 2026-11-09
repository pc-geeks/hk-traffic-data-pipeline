.PHONY: install up down dims run-day benchmark export-bi test lint

install:            ## create a venv first: python -m venv .venv && source .venv/bin/activate
	pip install -e ".[dev]"

up:                 ## PostgreSQL only
	docker compose up -d warehouse

down:
	docker compose down

dims:               ## newest detector zip in data/landing
	hktraffic dims

run-day:            ## example: make run-day DATE=2026-10-28   (zips must be in data/landing/)
	hktraffic run-day --date $(DATE)

benchmark:          ## example: make benchmark DATE=2026-10-28
	hktraffic benchmark --date $(DATE)

export-bi:          ## CSV files for Power BI in data/bi/
	hktraffic export-bi --out data/bi

test:
	pytest -q

lint:
	ruff check .
