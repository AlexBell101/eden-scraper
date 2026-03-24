from __future__ import annotations

import asyncio
import re
from datetime import datetime, timezone
from typing import Any

import feedparser
import httpx

from scrapers.base import BaseScraper

# ---------------------------------------------------------------------------
# City -> RSS URL mapping
# ---------------------------------------------------------------------------

_CITY_RSS_URLS: dict[str, str] = {
    "San Francisco": "https://sfbay.craigslist.org/search/sfc/apa?format=rss",
    "Oakland": "https://sfbay.craigslist.org/search/eby/apa?format=rss",
    "Berkeley": "https://sfbay.craigslist.org/search/eby/apa?format=rss&query=berkeley",
}

_DEFAULT_RSS_URL = "https://sfbay.craigslist.org/search/apa?format=rss"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

_REQUEST_TIMEOUT = 30  # seconds
_POLITE_DELAY = 1.0    # seconds between requests


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_price_from_title(title: str) -> str | None:
    """Pull the first dollar-prefixed number from a Craigslist title.

    Craigslist titles commonly look like:
        "$2500 2br - Sunny apartment..."
        "$1,800/mo - Studio near BART"
    """
    match = re.search(r"\$([\d,]+)", title)
    return match.group(0) if match else None


def _extract_bedrooms_from_title(title: str) -> str | None:
    """Pull bedroom count from a Craigslist title.

    Handles patterns like "2br", "1 bed", "studio".
    """
    title_lower = title.lower()
    if "studio" in title_lower:
        return "studio"
    match = re.search(r"(\d+(?:\.\d+)?)\s*(?:br|bed|bedroom|bdrm)", title_lower)
    return match.group(0) if match else None


def _parse_craigslist_id(link: str) -> str:
    """Extract the numeric post ID from a Craigslist URL."""
    match = re.search(r"/(\d+)\.html", link)
    return match.group(1) if match else link


def _entry_to_raw(entry: Any, city: str) -> dict:
    """Convert a feedparser entry to a raw dict for the normalizer."""
    link: str = getattr(entry, "link", "") or ""
    title: str = getattr(entry, "title", "") or ""
    summary: str = getattr(entry, "summary", "") or ""
    published: str | None = None

    if hasattr(entry, "published_parsed") and entry.published_parsed:
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            published = dt.date().isoformat()
        except Exception:
            published = getattr(entry, "published", None)
    else:
        published = getattr(entry, "published", None)

    price_raw = _extract_price_from_title(title)
    bedrooms_raw = _extract_bedrooms_from_title(title)

    # Some Craigslist entries expose geo coordinates in tags.
    neighborhood: str | None = None
    if hasattr(entry, "tags") and entry.tags:
        for tag in entry.tags:
            term = getattr(tag, "term", "") or ""
            if term and term not in ("apts", "apa"):
                neighborhood = term
                break

    return {
        "id": _parse_craigslist_id(link),
        "guid": link,
        "link": link,
        "title": title,
        "description": summary,
        "summary": summary,
        "price": price_raw,
        "bedrooms_raw": bedrooms_raw,
        "city": city,
        "neighborhood": neighborhood,
        "published": published,
        # Fields not available from RSS but kept for schema compatibility.
        "bathrooms": None,
        "sqft": None,
        "address": None,
        "pet_policy": None,
        "pet_friendly": None,
        "amenities": [],
        "images": [],
        "lease_terms": None,
        "furnished": None,
        "parking": None,
        "laundry": None,
    }


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class CraigslistScraper(BaseScraper):
    """Scrapes Craigslist apartment RSS feeds for a given city."""

    def __init__(self, city: str) -> None:
        super().__init__(city)
        self.rss_url = _CITY_RSS_URLS.get(city, _DEFAULT_RSS_URL)

    async def scrape(self) -> list[dict]:
        print(f"[Eden Craigslist] Scraping {self.city} from {self.rss_url}")
        raw_listings: list[dict] = []

        try:
            async with httpx.AsyncClient(
                headers=_HEADERS,
                timeout=_REQUEST_TIMEOUT,
                follow_redirects=True,
            ) as client:
                response = await client.get(self.rss_url)
                response.raise_for_status()
                content = response.text
        except httpx.HTTPStatusError as exc:
            print(
                f"[Eden Craigslist] HTTP error for {self.city}: "
                f"{exc.response.status_code} {exc.request.url}"
            )
            return []
        except httpx.RequestError as exc:
            print(f"[Eden Craigslist] Request error for {self.city}: {exc}")
            return []

        await asyncio.sleep(_POLITE_DELAY)

        try:
            feed = feedparser.parse(content)
        except Exception as exc:
            print(f"[Eden Craigslist] Feed parse error for {self.city}: {exc}")
            return []

        if feed.bozo and feed.bozo_exception:
            # bozo=True means the feed is not well-formed XML, but feedparser
            # still attempts a partial parse — continue with whatever we got.
            print(
                f"[Eden Craigslist] Malformed feed for {self.city} "
                f"(bozo exception: {feed.bozo_exception}); "
                "attempting partial parse."
            )

        entries = getattr(feed, "entries", []) or []
        print(f"[Eden Craigslist] Found {len(entries)} entries for {self.city}")

        for entry in entries:
            try:
                raw = _entry_to_raw(entry, self.city)
                raw_listings.append(raw)
            except Exception as exc:
                link = getattr(entry, "link", "<unknown>")
                print(
                    f"[Eden Craigslist] Failed to parse entry {link}: {exc}"
                )
                continue

        return raw_listings
