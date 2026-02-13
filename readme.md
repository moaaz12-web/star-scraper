# GitHub Stars Crawler (GraphQL + Postgres)

Crawler for collecting GitHub repository star counts through the GitHub GraphQL API and storing current + daily snapshot data in PostgreSQL.

## Minimal command model

The Makefile now has a small, fixed command set:

- `make init`: install Python deps and create `.env` template
- `make up`: start Postgres/Adminer and apply schema
- `make smoke`: quick validation crawl (1,000 repos)
- `make run`: one-time crawl (default 100,000 repos)
- `make loop`: continuous crawl mode
- `make status`: show table row counts
- `make save`: export CSV/JSON artifacts
- `make down`: stop Docker services

Run `make help` anytime to see this list.

## Prerequisites

- `python3`
- `docker compose`
- GitHub token with GraphQL access
- Optional: host `psql` (Docker fallback is built into `make up` and `make status`)

## Setup and run

1. Bootstrap local environment:

```bash
make init
```

2. Edit `.env` and set at least:

```bash
GITHUB_TOKEN=ghp_your_real_token_here
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/github
```

3. Start DB services and initialize schema:

```bash
make up
```

4. Run a quick test crawl:

```bash
make smoke
```

5. Run full crawl when ready:

```bash
make run
```

`make run` prints runtime metrics at completion, including `elapsed_s`, `repos_per_s`, and HTTP request counts.

6. Verify and export data:

```bash
make status
make save
```

7. Stop services:

```bash
make down
```

## Runtime overrides

```bash
# Crawl 25,000 repos
make run TARGET_REPOS=25000

# Continuous mode every 12 hours
make loop INTERVAL_HOURS=12

# Add extra crawler args
make run CRAWL_FLAGS="--schema-path sql/schema.sql"

# Export to custom folder
make save DUMP_DIR=artifacts/db_dump_manual
```

`.env` values still map through:
- `TARGET_REPO_COUNT` -> `TARGET_REPOS`
- `LOOP_INTERVAL_HOURS` -> `INTERVAL_HOURS`

## Artifacts

`make save` writes:

- `repositories.csv`
- `repo_star_snapshots.csv`
- `crawl_runs.csv`
- `summary.json`

Default output path: `artifacts/db_dump/`

## Adminer

When running, open `http://localhost:8080` and use:

- System: `PostgreSQL`
- Server: `postgres`
- Username: `${POSTGRES_USER}`
- Password: `${POSTGRES_PASSWORD}`
- Database: `${POSTGRES_DB}`

## Troubleshooting

- `Missing DATABASE_URL` or `Missing GITHUB_TOKEN`: set values in `.env` or shell env.
- Postgres startup timeout: run `docker compose logs postgres`.
- If `psql` is not installed on host, Makefile auto-falls back to containerized `psql`.
