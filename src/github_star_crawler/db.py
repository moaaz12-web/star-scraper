from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg


REPO_UPSERT_SQL = """
INSERT INTO github.repositories (
    repo_node_id,
    name_with_owner,
    owner_login,
    repo_name,
    url,
    is_fork,
    is_archived,
    is_private,
    default_branch,
    primary_language,
    stargazer_count,
    created_at,
    updated_at,
    pushed_at,
    raw_payload,
    last_seen_at
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s,
    %s::jsonb,
    NOW()
)
ON CONFLICT (repo_node_id) DO UPDATE SET
    name_with_owner = EXCLUDED.name_with_owner,
    owner_login = EXCLUDED.owner_login,
    repo_name = EXCLUDED.repo_name,
    url = EXCLUDED.url,
    is_fork = EXCLUDED.is_fork,
    is_archived = EXCLUDED.is_archived,
    is_private = EXCLUDED.is_private,
    default_branch = EXCLUDED.default_branch,
    primary_language = EXCLUDED.primary_language,
    stargazer_count = EXCLUDED.stargazer_count,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    pushed_at = EXCLUDED.pushed_at,
    raw_payload = EXCLUDED.raw_payload,
    last_seen_at = NOW();
"""


SNAPSHOT_UPSERT_SQL = """
INSERT INTO github.repo_star_snapshots (
    repo_node_id,
    snapshot_date,
    stargazer_count,
    fetched_at
)
VALUES (%s, %s, %s, NOW())
ON CONFLICT (repo_node_id, snapshot_date) DO UPDATE SET
    stargazer_count = EXCLUDED.stargazer_count,
    fetched_at = EXCLUDED.fetched_at;
"""


class PostgresStore:
    def __init__(self, database_url: str) -> None:
        self.conn = psycopg.connect(database_url)

    def close(self) -> None:
        self.conn.close()

    def init_schema(self, schema_sql_path: str) -> None:
        sql = Path(schema_sql_path).read_text(encoding="utf-8")
        with self.conn.cursor() as cur:
            cur.execute(sql)
        self.conn.commit()

    def start_run(self, target_repo_count: int) -> int:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO github.crawl_runs (target_repo_count, status)
                VALUES (%s, 'running')
                RETURNING run_id
                """,
                (target_repo_count,),
            )
            run_id = int(cur.fetchone()[0])
        self.conn.commit()
        return run_id

    def finish_run(
        self,
        run_id: int,
        status: str,
        repo_count: int,
        partition_count: int,
        split_partition_count: int,
        metadata_json: str,
        error_message: str | None = None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE github.crawl_runs
                SET finished_at = NOW(),
                    status = %s,
                    repo_count = %s,
                    partition_count = %s,
                    split_partition_count = %s,
                    metadata = %s::jsonb,
                    error_message = %s
                WHERE run_id = %s
                """,
                (
                    status,
                    repo_count,
                    partition_count,
                    split_partition_count,
                    metadata_json,
                    error_message,
                    run_id,
                ),
            )
        self.conn.commit()

    def upsert_page(
        self,
        repo_rows: list[tuple[Any, ...]],
        snapshot_rows: list[tuple[Any, ...]],
    ) -> None:
        if not repo_rows:
            return
        try:
            with self.conn.cursor() as cur:
                cur.executemany(REPO_UPSERT_SQL, repo_rows)
                cur.executemany(SNAPSHOT_UPSERT_SQL, snapshot_rows)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

