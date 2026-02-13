from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
import heapq
import itertools
import json
import sys
from typing import Any

from .config import Settings
from .db import PostgresStore
from .github_client import GitHubGraphQLClient
from .partitioning import SearchPartition


SEARCH_QUERY = """
query SearchRepositories($query: String!, $first: Int!, $after: String) {
  rateLimit {
    limit
    cost
    remaining
    resetAt
  }
  search(query: $query, type: REPOSITORY, first: $first, after: $after) {
    repositoryCount
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on Repository {
        id
        nameWithOwner
        name
        url
        isFork
        isArchived
        isPrivate
        stargazerCount
        createdAt
        updatedAt
        pushedAt
        owner {
          login
        }
        primaryLanguage {
          name
        }
        defaultBranchRef {
          name
        }
      }
    }
  }
}
"""


@dataclass
class CrawlStats:
    partitions_processed: int = 0
    partitions_split: int = 0
    partitions_skipped: int = 0
    repos_persisted: int = 0
    duplicate_repo_nodes: int = 0
    max_star_count: int = 0

    def to_json(self) -> str:
        return json.dumps(
            {
                "partitions_processed": self.partitions_processed,
                "partitions_split": self.partitions_split,
                "partitions_skipped": self.partitions_skipped,
                "repos_persisted": self.repos_persisted,
                "duplicate_repo_nodes": self.duplicate_repo_nodes,
                "max_star_count": self.max_star_count,
            },
            sort_keys=True,
        )


class GitHubStarCrawler:
    def __init__(
        self,
        settings: Settings,
        github: GitHubGraphQLClient,
        store: PostgresStore,
    ) -> None:
        self.settings = settings
        self.github = github
        self.store = store
        self._queue_counter = itertools.count()
        self._show_progress = sys.stdout.isatty()

    def crawl_once(self) -> CrawlStats:
        stats = CrawlStats()
        run_id = self.store.start_run(target_repo_count=self.settings.target_repo_count)
        seen_repo_nodes: set[str] = set()
        today = datetime.now(timezone.utc).date()
        if self._show_progress:
            self._print_progress(stats.repos_persisted, stats.partitions_processed)

        try:
            max_stars = self._max_stars()
            stats.max_star_count = max_stars
            queue: list[tuple[int, int, int, int, SearchPartition]] = []
            initial = SearchPartition(
                stars_min=self.settings.min_stars,
                stars_max=max_stars,
            )
            self._push_partition(queue, initial)

            while queue and stats.repos_persisted < self.settings.target_repo_count:
                _, _, _, _, partition = heapq.heappop(queue)
                stats.partitions_processed += 1

                remaining = self.settings.target_repo_count - stats.repos_persisted
                page_size = min(self.settings.page_size, remaining)
                query_text = partition.to_query(self.settings.search_base_qualifiers)
                first_search_block = self._search_page(
                    query_text=query_text,
                    page_size=page_size,
                    cursor=None,
                )
                partition_count = self._partition_count(first_search_block)
                if partition_count == 0:
                    stats.partitions_skipped += 1
                    self._print_progress(
                        stats.repos_persisted, stats.partitions_processed
                    )
                    continue

                if partition_count > self.settings.max_partition_results:
                    split_result = self._split_partition(partition, today)
                    if split_result is not None:
                        stats.partitions_split += 1
                        for child in split_result:
                            self._push_partition(queue, child)
                        self._print_progress(
                            stats.repos_persisted, stats.partitions_processed
                        )
                        continue

                persisted, duplicates = self._ingest_partition(
                    query_text=query_text,
                    row_limit=remaining,
                    snapshot_date=today,
                    seen_repo_nodes=seen_repo_nodes,
                    first_search_block=first_search_block,
                )
                stats.repos_persisted += persisted
                stats.duplicate_repo_nodes += duplicates
                self._print_progress(stats.repos_persisted, stats.partitions_processed)

                if (
                    not self._show_progress
                    and self.settings.log_every_n_partitions > 0
                    and stats.partitions_processed
                    % self.settings.log_every_n_partitions
                    == 0
                ):
                    print(
                        "[crawler] partitions="
                        f"{stats.partitions_processed}, repos={stats.repos_persisted}"
                    )

            status = (
                "succeeded"
                if stats.repos_persisted >= self.settings.target_repo_count
                else "partial"
            )
            self._finish_progress()
            self.store.finish_run(
                run_id=run_id,
                status=status,
                repo_count=stats.repos_persisted,
                partition_count=stats.partitions_processed,
                split_partition_count=stats.partitions_split,
                metadata_json=stats.to_json(),
            )
            return stats
        except Exception as exc:
            self._finish_progress()
            self.store.finish_run(
                run_id=run_id,
                status="failed",
                repo_count=stats.repos_persisted,
                partition_count=stats.partitions_processed,
                split_partition_count=stats.partitions_split,
                metadata_json=stats.to_json(),
                error_message=str(exc),
            )
            raise

    def _print_progress(self, persisted: int, partitions_processed: int) -> None:
        if not self._show_progress:
            return
        target = max(self.settings.target_repo_count, 1)
        ratio = min(1.0, persisted / target)
        bar_width = 30
        filled = int(ratio * bar_width)
        bar = "#" * filled + "-" * (bar_width - filled)
        pct = ratio * 100.0
        print(
            f"\r[crawler] [{bar}] {persisted}/{target} "
            f"({pct:5.1f}%) partitions={partitions_processed}",
            end="",
            flush=True,
        )

    def _finish_progress(self) -> None:
        if self._show_progress:
            print()

    def _push_partition(
        self,
        queue: list[tuple[int, int, int, int, SearchPartition]],
        partition: SearchPartition,
    ) -> None:
        # Max-heap behavior by pushing negative priority values.
        star_max, _, _ = partition.priority_key()
        star_width = partition.stars_max - partition.stars_min
        date_width = 0
        if partition.created_from and partition.created_to:
            date_width = (partition.created_to - partition.created_from).days
        heapq.heappush(
            queue,
            (-star_max, star_width, date_width, next(self._queue_counter), partition),
        )

    def _split_partition(
        self,
        partition: SearchPartition,
        today: date,
    ) -> tuple[SearchPartition, SearchPartition] | None:
        star_split = partition.split_stars()
        if star_split is not None:
            return star_split

        with_dates = partition.with_date_window(today=today)
        if with_dates != partition:
            return with_dates.split_dates()
        return partition.split_dates()

    def _max_stars(self) -> int:
        query_text = f"{self.settings.search_base_qualifiers} sort:stars-desc".strip()
        data = self.github.execute(
            SEARCH_QUERY,
            {"query": query_text, "first": 1, "after": None},
        )
        nodes = data["search"]["nodes"] or []
        for node in nodes:
            if node and node.get("stargazerCount") is not None:
                return int(node["stargazerCount"])
        raise RuntimeError("Could not determine maximum stargazer count from GitHub.")

    def _search_page(
        self,
        query_text: str,
        page_size: int,
        cursor: str | None,
    ) -> dict[str, Any]:
        data = self.github.execute(
            SEARCH_QUERY,
            {"query": query_text, "first": page_size, "after": cursor},
        )
        return data["search"]

    def _partition_count(self, search: dict[str, Any]) -> int:
        count = search.get("repositoryCount")
        if count is None:
            count = search.get("totalCount")
        if count is None:
            raise RuntimeError("Missing repository count in GraphQL response.")
        return int(count)

    def _ingest_partition(
        self,
        query_text: str,
        row_limit: int,
        snapshot_date: date,
        seen_repo_nodes: set[str],
        first_search_block: dict[str, Any] | None = None,
    ) -> tuple[int, int]:
        cursor: str | None = None
        persisted = 0
        duplicates = 0
        search_block = first_search_block

        while persisted < row_limit:
            if search_block is None:
                page_size = min(self.settings.page_size, row_limit - persisted)
                search_block = self._search_page(
                    query_text=query_text,
                    page_size=page_size,
                    cursor=cursor,
                )
            nodes = search_block["nodes"] or []
            if not nodes:
                break

            repo_rows: list[tuple[Any, ...]] = []
            snapshot_rows: list[tuple[Any, ...]] = []

            for node in nodes:
                if not node or not node.get("id"):
                    continue
                node_id = node["id"]
                if node_id in seen_repo_nodes:
                    duplicates += 1
                    continue
                seen_repo_nodes.add(node_id)

                repo_rows.append(self._build_repo_row(node))
                snapshot_rows.append(
                    (
                        node_id,
                        snapshot_date,
                        int(node["stargazerCount"]),
                    )
                )

            self.store.upsert_page(repo_rows=repo_rows, snapshot_rows=snapshot_rows)
            persisted += len(repo_rows)

            page_info = search_block["pageInfo"]
            if not page_info["hasNextPage"]:
                break
            cursor = page_info["endCursor"]
            if cursor is None:
                break
            search_block = None

        return persisted, duplicates

    def _build_repo_row(self, node: dict[str, Any]) -> tuple[Any, ...]:
        owner_login = ""
        owner = node.get("owner")
        if isinstance(owner, dict):
            owner_login = owner.get("login") or ""

        primary_language = None
        language = node.get("primaryLanguage")
        if isinstance(language, dict):
            primary_language = language.get("name")

        default_branch = None
        branch = node.get("defaultBranchRef")
        if isinstance(branch, dict):
            default_branch = branch.get("name")

        return (
            node["id"],
            node["nameWithOwner"],
            owner_login,
            node["name"],
            node["url"],
            bool(node["isFork"]),
            bool(node["isArchived"]),
            bool(node["isPrivate"]),
            default_branch,
            primary_language,
            int(node["stargazerCount"]),
            node["createdAt"],
            node["updatedAt"],
            node.get("pushedAt"),
            json.dumps(node, separators=(",", ":"), sort_keys=True),
        )
