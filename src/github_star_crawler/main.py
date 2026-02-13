from __future__ import annotations

from dataclasses import replace
import argparse
import sys
import time

from .config import Settings
from .crawler import GitHubStarCrawler
from .db import PostgresStore
from .github_client import GitHubGraphQLClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Crawl GitHub repository star counts into Postgres using "
            "the GitHub GraphQL API."
        )
    )
    parser.add_argument(
        "--init-db",
        action="store_true",
        help="Apply sql/schema.sql before running the crawler.",
    )
    parser.add_argument(
        "--schema-path",
        default="sql/schema.sql",
        help="Path to schema SQL file.",
    )
    parser.add_argument(
        "--target-repos",
        type=int,
        default=None,
        help="Override TARGET_REPO_COUNT for this run.",
    )
    parser.add_argument(
        "--continuous",
        action="store_true",
        help="Run forever with a fixed interval (default 24h).",
    )
    parser.add_argument(
        "--interval-hours",
        type=float,
        default=None,
        help="Continuous mode interval in hours.",
    )
    return parser.parse_args()


def run_once(settings: Settings, args: argparse.Namespace) -> int:
    store = PostgresStore(settings.database_url)
    github = GitHubGraphQLClient(settings)
    started = time.monotonic()
    try:
        if args.init_db:
            store.init_schema(args.schema_path)
        crawler = GitHubStarCrawler(settings=settings, github=github, store=store)
        stats = crawler.crawl_once()
        elapsed = max(0.001, time.monotonic() - started)
        repos_per_second = stats.repos_persisted / elapsed
        req_per_second = github.http_request_count / elapsed
        print(
            "[crawler] completed "
            f"repos={stats.repos_persisted} "
            f"partitions={stats.partitions_processed} "
            f"splits={stats.partitions_split} "
            f"elapsed_s={elapsed:.1f} "
            f"repos_per_s={repos_per_second:.2f} "
            f"http_requests={github.http_request_count} "
            f"successful_queries={github.successful_query_count} "
            f"http_req_per_s={req_per_second:.2f}"
        )
        return 0
    finally:
        github.close()
        store.close()


def main() -> int:
    args = parse_args()
    settings = Settings.from_env()
    if args.target_repos is not None:
        settings = replace(settings, target_repo_count=args.target_repos)
    interval_hours = (
        args.interval_hours if args.interval_hours is not None else settings.loop_interval_hours
    )

    if not args.continuous:
        return run_once(settings, args)

    while True:
        started = time.monotonic()
        try:
            run_once(settings, args)
        except Exception as exc:
            print(f"[crawler] run failed: {exc}")
            retry_sleep = min(interval_hours * 3600.0, 900.0)
            print(f"[crawler] sleeping {retry_sleep:.0f}s before retry")
            time.sleep(retry_sleep)
            continue

        elapsed = time.monotonic() - started
        sleep_for = max(0.0, (interval_hours * 3600.0) - elapsed)
        print(f"[crawler] sleeping {sleep_for:.0f}s before next run")
        time.sleep(sleep_for)


if __name__ == "__main__":
    sys.exit(main())
