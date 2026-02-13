from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta


EARLIEST_GITHUB_DATE = date(2008, 1, 1)


@dataclass(frozen=True)
class SearchPartition:
    stars_min: int
    stars_max: int
    created_from: date | None = None
    created_to: date | None = None

    def with_date_window(
        self,
        today: date,
        earliest: date = EARLIEST_GITHUB_DATE,
    ) -> "SearchPartition":
        if self.created_from and self.created_to:
            return self
        return SearchPartition(
            stars_min=self.stars_min,
            stars_max=self.stars_max,
            created_from=earliest,
            created_to=today,
        )

    def split_stars(self) -> tuple["SearchPartition", "SearchPartition"] | None:
        if self.stars_min >= self.stars_max:
            return None
        midpoint = (self.stars_min + self.stars_max) // 2
        high = SearchPartition(
            stars_min=midpoint + 1,
            stars_max=self.stars_max,
            created_from=self.created_from,
            created_to=self.created_to,
        )
        low = SearchPartition(
            stars_min=self.stars_min,
            stars_max=midpoint,
            created_from=self.created_from,
            created_to=self.created_to,
        )
        return high, low

    def split_dates(self) -> tuple["SearchPartition", "SearchPartition"] | None:
        if not self.created_from or not self.created_to:
            return None
        if self.created_from >= self.created_to:
            return None

        span_days = (self.created_to - self.created_from).days
        midpoint = self.created_from + timedelta(days=span_days // 2)
        first = SearchPartition(
            stars_min=self.stars_min,
            stars_max=self.stars_max,
            created_from=self.created_from,
            created_to=midpoint,
        )
        second = SearchPartition(
            stars_min=self.stars_min,
            stars_max=self.stars_max,
            created_from=midpoint + timedelta(days=1),
            created_to=self.created_to,
        )
        return first, second

    def is_single_day(self) -> bool:
        return bool(
            self.created_from and self.created_to and self.created_from == self.created_to
        )

    def to_query(self, base_qualifiers: str) -> str:
        qualifiers: list[str] = [base_qualifiers.strip()]
        qualifiers.append(f"stars:{self.stars_min}..{self.stars_max}")
        if self.created_from and self.created_to:
            qualifiers.append(
                f"created:{self.created_from.isoformat()}..{self.created_to.isoformat()}"
            )
        qualifiers.append("sort:stars-desc")
        return " ".join(part for part in qualifiers if part)

    def priority_key(self) -> tuple[int, int, int]:
        # Higher stars first, then tighter ranges.
        width = self.stars_max - self.stars_min
        date_width = 0
        if self.created_from and self.created_to:
            date_width = (self.created_to - self.created_from).days
        return self.stars_max, -width, -date_width

    def label(self) -> str:
        created = ""
        if self.created_from and self.created_to:
            created = (
                f", created={self.created_from.isoformat()}.."
                f"{self.created_to.isoformat()}"
            )
        return f"stars={self.stars_min}..{self.stars_max}{created}"

