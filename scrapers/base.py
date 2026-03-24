from __future__ import annotations

from abc import ABC, abstractmethod


class BaseScraper(ABC):
    """Abstract base class for all listing scrapers."""

    def __init__(self, city: str) -> None:
        self.city = city

    @abstractmethod
    async def scrape(self) -> list[dict]:
        """Scrape listings and return a list of raw dicts.

        Each dict should contain enough information for normalizer.py to
        produce a unified listing.  Implementations must handle their own
        errors gracefully and never raise uncaught exceptions.
        """
