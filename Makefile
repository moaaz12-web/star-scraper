SHELL := /usr/bin/env bash

.DEFAULT_GOAL := help

VENV ?= .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
COMPOSE ?= docker compose
MODULE ?= github_star_crawler
PYTHONPATH ?= src

POSTGRES_USER ?= postgres
POSTGRES_PASSWORD ?= postgres
POSTGRES_DB ?= github
POSTGRES_PORT ?= 5432
ADMINER_PORT ?= 8080
DB_WAIT_TIMEOUT ?= 120
TARGET_REPOS ?= 100000
INTERVAL_HOURS ?= 24
DUMP_DIR ?= artifacts/db_dump
CRAWL_FLAGS ?=

ifneq (,$(wildcard .env))
include .env
export
endif

ifneq ($(strip $(TARGET_REPO_COUNT)),)
TARGET_REPOS := $(TARGET_REPO_COUNT)
endif
ifneq ($(strip $(LOOP_INTERVAL_HOURS)),)
INTERVAL_HOURS := $(LOOP_INTERVAL_HOURS)
endif

.PHONY: help init up run smoke loop status save down

help: ## Show simple command list
	@printf "%-8s %s\n" "init" "Install deps and create .env template"
	@printf "%-8s %s\n" "up" "Start Postgres/Adminer and apply schema"
	@printf "%-8s %s\n" "smoke" "Quick validation crawl (1,000 repos)"
	@printf "%-8s %s\n" "run" "One-time crawl (default 100,000 repos)"
	@printf "%-8s %s\n" "loop" "Continuous crawl mode"
	@printf "%-8s %s\n" "status" "Show row counts"
	@printf "%-8s %s\n" "save" "Export CSV/JSON artifacts"
	@printf "%-8s %s\n" "down" "Stop Docker services"

init: ## Install dependencies and create local .env template
	@command -v python3 >/dev/null 2>&1 || (echo "python3 not found"; exit 1)
	@$(COMPOSE) version >/dev/null 2>&1 || (echo "docker compose not available"; exit 1)
	@test -d $(VENV) || python3 -m venv $(VENV)
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	@test -f .env || cp .env.example .env
	@echo "Review .env and set GITHUB_TOKEN and DATABASE_URL"

up: ## Start Postgres/Adminer and initialize schema
	@[ -n "$$DATABASE_URL" ] || (echo "Missing DATABASE_URL in environment (.env or shell)"; exit 1)
	$(COMPOSE) up -d postgres adminer
	@echo "Waiting for postgres to become ready..."
	@deadline=$$(( $$(date +%s) + $(DB_WAIT_TIMEOUT) )); \
	while true; do \
		if $(COMPOSE) exec -T postgres pg_isready -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" >/dev/null 2>&1; then \
			echo "postgres is ready"; \
			break; \
		fi; \
		if [ "$$(date +%s)" -ge "$$deadline" ]; then \
			echo "postgres did not become ready within $(DB_WAIT_TIMEOUT)s"; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@if command -v psql >/dev/null 2>&1 && psql --version >/dev/null 2>&1; then \
		psql "$$DATABASE_URL" -f sql/schema.sql; \
	else \
		echo "Host psql unavailable; applying schema via docker compose postgres container"; \
		$(COMPOSE) exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" < sql/schema.sql; \
	fi

run: ## One-time crawl (override TARGET_REPOS and CRAWL_FLAGS)
	@test -x "$(PYTHON)" || (echo "Virtualenv missing. Run 'make init' first."; exit 1)
	@[ -n "$$DATABASE_URL" ] || (echo "Missing DATABASE_URL in environment (.env or shell)"; exit 1)
	@[ -n "$$GITHUB_TOKEN" ] || (echo "Missing GITHUB_TOKEN in environment (.env or shell)"; exit 1)
	@[ "$$GITHUB_TOKEN" != "ghp_xxx" ] || (echo "Set a real GITHUB_TOKEN value"; exit 1)
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m $(MODULE) --target-repos $(TARGET_REPOS) $(CRAWL_FLAGS)

smoke: ## Quick validation crawl using 1,000 repositories
	$(MAKE) run TARGET_REPOS=1000

loop: ## Continuous crawl mode (override INTERVAL_HOURS)
	@test -x "$(PYTHON)" || (echo "Virtualenv missing. Run 'make init' first."; exit 1)
	@[ -n "$$DATABASE_URL" ] || (echo "Missing DATABASE_URL in environment (.env or shell)"; exit 1)
	@[ -n "$$GITHUB_TOKEN" ] || (echo "Missing GITHUB_TOKEN in environment (.env or shell)"; exit 1)
	@[ "$$GITHUB_TOKEN" != "ghp_xxx" ] || (echo "Set a real GITHUB_TOKEN value"; exit 1)
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) -m $(MODULE) --continuous --interval-hours $(INTERVAL_HOURS)

status: ## Show table row counts
	@[ -n "$$DATABASE_URL" ] || (echo "Missing DATABASE_URL in environment (.env or shell)"; exit 1)
	@if command -v psql >/dev/null 2>&1 && psql --version >/dev/null 2>&1; then \
		psql "$$DATABASE_URL" -c "SELECT COUNT(*) FROM github.repositories;"; \
		psql "$$DATABASE_URL" -c "SELECT COUNT(*) FROM github.repo_star_snapshots;"; \
	else \
		echo "Host psql unavailable; using docker compose postgres container"; \
		$(COMPOSE) exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" -c "SELECT COUNT(*) FROM github.repositories;"; \
		$(COMPOSE) exec -T postgres psql -U "$(POSTGRES_USER)" -d "$(POSTGRES_DB)" -c "SELECT COUNT(*) FROM github.repo_star_snapshots;"; \
	fi

save: ## Export DB tables to CSV/JSON artifacts
	@test -x "$(PYTHON)" || (echo "Virtualenv missing. Run 'make init' first."; exit 1)
	@[ -n "$$DATABASE_URL" ] || (echo "Missing DATABASE_URL in environment (.env or shell)"; exit 1)
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/export_db_dump.py --database-url "$$DATABASE_URL" --output-dir "$(DUMP_DIR)"

down: ## Stop Docker services
	$(COMPOSE) down
