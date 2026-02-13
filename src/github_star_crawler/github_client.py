from __future__ import annotations

from datetime import datetime, timezone
import random
import time
from typing import Any

import requests

from .config import Settings


RETRYABLE_HTTP_CODES = {403, 429, 500, 502, 503, 504}
MAX_SLEEP_SECONDS = 3600


class GitHubGraphQLClient:
    endpoint = "https://api.github.com/graphql"

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {settings.github_token}",
                "Accept": "application/vnd.github+json",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )
        self._last_request_at = 0.0
        self.http_request_count = 0
        self.successful_query_count = 0

    def close(self) -> None:
        self.session.close()

    def execute(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, self.settings.max_retries + 1):
            self._respect_min_request_interval()
            try:
                self.http_request_count += 1
                response = self.session.post(
                    self.endpoint,
                    json={"query": query, "variables": variables},
                    timeout=self.settings.request_timeout_seconds,
                )
            except requests.RequestException as exc:
                last_error = exc
                self._sleep(self._compute_backoff(attempt), "network_error")
                continue

            if response.status_code in RETRYABLE_HTTP_CODES:
                wait = self._wait_from_headers(response.headers)
                if wait is None:
                    wait = self._compute_backoff(attempt)
                self._sleep(wait, f"http_{response.status_code}")
                continue

            if response.status_code >= 400:
                raise RuntimeError(
                    f"GitHub GraphQL request failed ({response.status_code}): "
                    f"{response.text}"
                )

            try:
                payload = response.json()
            except ValueError as exc:
                last_error = exc
                self._sleep(self._compute_backoff(attempt), "invalid_json")
                continue

            data = payload.get("data") or {}
            rate_limit = data.get("rateLimit")
            errors = payload.get("errors") or []
            if errors:
                if self._errors_are_retryable(errors):
                    wait = self._wait_from_rate_limit(rate_limit)
                    if wait is None:
                        wait = self._wait_from_headers(response.headers)
                    if wait is None:
                        wait = self._compute_backoff(attempt)
                    self._sleep(wait, "graphql_retryable_error")
                    continue
                raise RuntimeError(f"GitHub GraphQL errors: {errors}")

            self._sleep_if_rate_low(rate_limit)
            self.successful_query_count += 1
            return data

        raise RuntimeError(f"GraphQL request failed after retries: {last_error}")

    def _respect_min_request_interval(self) -> None:
        elapsed = time.monotonic() - self._last_request_at
        remaining = self.settings.min_request_interval_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)
        self._last_request_at = time.monotonic()

    def _errors_are_retryable(self, errors: list[dict[str, Any]]) -> bool:
        for error in errors:
            error_type = str(error.get("type", "")).upper()
            message = str(error.get("message", "")).lower()
            if "rate limit" in message:
                return True
            if "secondary rate limit" in message:
                return True
            if "timeout" in message:
                return True
            if "abuse" in message:
                return True
            if error_type in {"RATE_LIMITED", "SERVICE_UNAVAILABLE", "ABUSE_DETECTED"}:
                return True
        return False

    def _compute_backoff(self, attempt: int) -> float:
        jitter = random.uniform(0.2, 0.8)
        delay = self.settings.base_backoff_seconds * (2 ** (attempt - 1))
        return min(delay + jitter, MAX_SLEEP_SECONDS)

    def _wait_from_rate_limit(self, rate_limit: dict[str, Any] | None) -> float | None:
        if not rate_limit:
            return None
        reset_at_raw = rate_limit.get("resetAt")
        if not reset_at_raw:
            return None
        try:
            reset_at = datetime.fromisoformat(reset_at_raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        now = datetime.now(timezone.utc)
        return max(0.0, (reset_at - now).total_seconds()) + random.uniform(0.2, 1.0)

    def _wait_from_headers(self, headers: requests.structures.CaseInsensitiveDict) -> float | None:
        retry_after = headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after)) + random.uniform(0.2, 1.0)
            except ValueError:
                return None
        reset_epoch = headers.get("X-RateLimit-Reset")
        if not reset_epoch:
            return None
        try:
            seconds_until_reset = float(reset_epoch) - time.time()
        except ValueError:
            return None
        return max(0.0, seconds_until_reset) + random.uniform(0.2, 1.0)

    def _sleep_if_rate_low(self, rate_limit: dict[str, Any] | None) -> None:
        if not rate_limit:
            return
        remaining = int(rate_limit.get("remaining", 0))
        if remaining > self.settings.min_remaining_points:
            return
        wait = self._wait_from_rate_limit(rate_limit)
        if wait is not None:
            self._sleep(wait, "low_rate_budget")

    def _sleep(self, seconds: float, reason: str) -> None:
        bounded = max(0.0, min(seconds, MAX_SLEEP_SECONDS))
        if bounded > 0:
            print(f"[rate-limit] sleeping {bounded:.1f}s ({reason})")
            time.sleep(bounded)
