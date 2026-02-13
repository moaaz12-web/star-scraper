#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime
import json
from pathlib import Path
from typing import Any

import psycopg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export crawler Postgres tables to CSV/JSON files."
    )
    parser.add_argument("--database-url", required=True, help="Postgres database URL.")
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Directory where dump files are written.",
    )
    return parser.parse_args()


def serialize_value(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"))
    return value


def export_query_to_csv(conn: psycopg.Connection, query: str, output_path: Path) -> int:
    row_count = 0
    with conn.cursor() as cur, output_path.open("w", encoding="utf-8", newline="") as fh:
        cur.execute(query)
        assert cur.description is not None
        columns = [col.name for col in cur.description]
        writer = csv.writer(fh)
        writer.writerow(columns)

        while True:
            rows = cur.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                writer.writerow([serialize_value(value) for value in row])
            row_count += len(rows)
    return row_count


def main() -> int:
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(args.database_url) as conn:
        repos_count = export_query_to_csv(
            conn,
            query="""
                SELECT *
                FROM github.repositories
                ORDER BY stargazer_count DESC, repo_node_id;
            """,
            output_path=output_dir / "repositories.csv",
        )

        snapshots_count = export_query_to_csv(
            conn,
            query="""
                SELECT *
                FROM github.repo_star_snapshots
                ORDER BY snapshot_date DESC, stargazer_count DESC, repo_node_id;
            """,
            output_path=output_dir / "repo_star_snapshots.csv",
        )

        runs_count = export_query_to_csv(
            conn,
            query="""
                SELECT *
                FROM github.crawl_runs
                ORDER BY run_id;
            """,
            output_path=output_dir / "crawl_runs.csv",
        )

    summary = {
        "tables": {
            "github.repositories": repos_count,
            "github.repo_star_snapshots": snapshots_count,
            "github.crawl_runs": runs_count,
        }
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(f"[dump] wrote {output_dir / 'repositories.csv'}")
    print(f"[dump] wrote {output_dir / 'repo_star_snapshots.csv'}")
    print(f"[dump] wrote {output_dir / 'crawl_runs.csv'}")
    print(f"[dump] wrote {output_dir / 'summary.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
