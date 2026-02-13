from __future__ import annotations

from dataclasses import dataclass
import os


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


@dataclass(frozen=True)
class Settings:
    github_token: str
    database_url: str
    target_repo_count: int
    page_size: int
    min_stars: int
    max_partition_results: int
    max_retries: int
    base_backoff_seconds: float
    request_timeout_seconds: int
    min_remaining_points: int
    min_request_interval_seconds: float
    search_base_qualifiers: str
    loop_interval_hours: float
    log_every_n_partitions: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            github_token=_require_env("GITHUB_TOKEN"),
            database_url=_require_env("DATABASE_URL"),
            target_repo_count=_env_int("TARGET_REPO_COUNT", 100_000),
            page_size=_env_int("PAGE_SIZE", 100),
            min_stars=_env_int("MIN_STARS", 0),
            max_partition_results=_env_int("MAX_PARTITION_RESULTS", 1000),
            max_retries=_env_int("MAX_RETRIES", 8),
            base_backoff_seconds=_env_float("BASE_BACKOFF_SECONDS", 1.5),
            request_timeout_seconds=_env_int("REQUEST_TIMEOUT_SECONDS", 40),
            min_remaining_points=_env_int("MIN_REMAINING_POINTS", 100),
            min_request_interval_seconds=_env_float(
                "MIN_REQUEST_INTERVAL_SECONDS", 0.35
            ),
            search_base_qualifiers=os.getenv(
                "SEARCH_BASE_QUALIFIERS",
                "is:public fork:false archived:false",
            ),
            loop_interval_hours=_env_float("LOOP_INTERVAL_HOURS", 24.0),
            log_every_n_partitions=_env_int("LOG_EVERY_N_PARTITIONS", 20),
        )

