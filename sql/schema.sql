CREATE SCHEMA IF NOT EXISTS github;

CREATE TABLE IF NOT EXISTS github.repositories (
    repo_node_id TEXT PRIMARY KEY,
    name_with_owner TEXT NOT NULL,
    owner_login TEXT NOT NULL,
    repo_name TEXT NOT NULL,
    url TEXT NOT NULL,
    is_fork BOOLEAN NOT NULL,
    is_archived BOOLEAN NOT NULL,
    is_private BOOLEAN NOT NULL,
    default_branch TEXT,
    primary_language TEXT,
    stargazer_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    pushed_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raw_payload JSONB NOT NULL DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_repositories_stargazer_count
    ON github.repositories (stargazer_count DESC);

CREATE INDEX IF NOT EXISTS idx_repositories_owner_login
    ON github.repositories (owner_login);

CREATE TABLE IF NOT EXISTS github.repo_star_snapshots (
    repo_node_id TEXT NOT NULL REFERENCES github.repositories(repo_node_id) ON DELETE CASCADE,
    snapshot_date DATE NOT NULL,
    stargazer_count INTEGER NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (repo_node_id, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_repo_star_snapshots_daily_stars
    ON github.repo_star_snapshots (snapshot_date, stargazer_count DESC);

CREATE TABLE IF NOT EXISTS github.crawl_runs (
    run_id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'running',
    target_repo_count INTEGER NOT NULL,
    repo_count INTEGER NOT NULL DEFAULT 0,
    partition_count INTEGER NOT NULL DEFAULT 0,
    split_partition_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::JSONB
);
