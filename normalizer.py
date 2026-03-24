from __future__ import annotations

import re
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_rent(value: Any) -> int:
    """Parse a rent value from various string formats into an integer.

    Accepts:
        "$2,500/mo", "$2500", "2500", 2500, "2,500", "$1,800 / month"
    Returns 0 if parsing fails.
    """
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    # Remove currency symbols, commas, and trailing rate indicators.
    digits = re.sub(r"[^\d.]", "", text)
    try:
        return int(float(digits))
    except (ValueError, TypeError):
        return 0


def _parse_bedrooms(value: Any) -> float:
    """Parse a bedroom count from various string formats into a float.

    Accepts:
        "studio" -> 0.0
        "1br", "1 bed", "1 bedroom", "1" -> 1.0
        "2br", "2 bed" -> 2.0
        "3/2" -> 3.0  (takes the first number)
    Returns 0.0 if parsing fails.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().lower()
    if "studio" in text:
        return 0.0
    # Extract leading number.
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        return float(match.group(1))
    return 0.0


def _parse_bathrooms(value: Any) -> float:
    """Parse a bathroom count from various formats."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if match:
        return float(match.group(1))
    return 0.0


def _parse_sqft(value: Any) -> int | None:
    """Parse square footage; return None if not available."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    match = re.search(r"(\d[\d,]*)", text)
    if match:
        try:
            return int(match.group(1).replace(",", ""))
        except ValueError:
            return None
    return None


def _parse_bool(value: Any) -> bool | None:
    """Parse a boolean-ish value; return None if ambiguous."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("yes", "true", "1", "allowed", "ok", "okay"):
        return True
    if text in ("no", "false", "0", "not allowed", "none"):
        return False
    return None


def _ensure_list(value: Any) -> list:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        # Split on common delimiters.
        return [v.strip() for v in re.split(r"[,;|]", value) if v.strip()]
    return [value]


# ---------------------------------------------------------------------------
# Source-specific field extractors
# ---------------------------------------------------------------------------

def _extract_craigslist(raw: dict) -> dict:
    """Extract fields from a raw Craigslist entry."""
    title: str = raw.get("title", "")
    description: str = raw.get("description", "") or raw.get("summary", "")

    # Craigslist titles often look like: "$2500 / 1br - some description"
    rent_match = re.search(r"\$?([\d,]+)\s*/?\s*(?:mo|month)?", title)
    rent_raw = rent_match.group(0) if rent_match else raw.get("price")

    bed_match = re.search(r"(\d+(?:\.\d+)?)\s*(?:br|bed|bedroom|bdrm)", title, re.I)
    bed_raw = bed_match.group(0) if bed_match else ("studio" if "studio" in title.lower() else None)

    return {
        "title": title,
        "description": description,
        "url": raw.get("link") or raw.get("url", ""),
        "source_id": raw.get("id") or raw.get("guid") or raw.get("link", ""),
        "rent_raw": rent_raw,
        "bedrooms_raw": bed_raw,
        "bathrooms_raw": raw.get("bathrooms"),
        "sqft_raw": raw.get("sqft"),
        "address": raw.get("address"),
        "neighborhood": raw.get("neighborhood"),
        "city": raw.get("city", ""),
        "pet_policy": raw.get("pet_policy"),
        "pet_friendly": raw.get("pet_friendly"),
        "amenities": raw.get("amenities", []),
        "images": raw.get("images", []),
        "lease_terms": raw.get("lease_terms"),
        "furnished": raw.get("furnished"),
        "parking": raw.get("parking"),
        "laundry": raw.get("laundry"),
        "date_posted": raw.get("published") or raw.get("date_posted"),
    }


def _extract_generic(raw: dict) -> dict:
    """Fallback extractor for unknown or future sources."""
    return {
        "title": raw.get("title", ""),
        "description": raw.get("description", "") or raw.get("body", ""),
        "url": raw.get("url") or raw.get("link", ""),
        "source_id": (
            raw.get("id")
            or raw.get("source_id")
            or raw.get("guid")
            or raw.get("url", "")
        ),
        "rent_raw": raw.get("rent") or raw.get("price"),
        "bedrooms_raw": raw.get("bedrooms") or raw.get("beds"),
        "bathrooms_raw": raw.get("bathrooms") or raw.get("baths"),
        "sqft_raw": raw.get("sqft") or raw.get("square_feet"),
        "address": raw.get("address"),
        "neighborhood": raw.get("neighborhood"),
        "city": raw.get("city", ""),
        "pet_policy": raw.get("pet_policy"),
        "pet_friendly": raw.get("pet_friendly"),
        "amenities": raw.get("amenities", []),
        "images": raw.get("images", []),
        "lease_terms": raw.get("lease_terms"),
        "furnished": raw.get("furnished"),
        "parking": raw.get("parking"),
        "laundry": raw.get("laundry"),
        "date_posted": raw.get("date_posted") or raw.get("published"),
    }


_EXTRACTORS = {
    "craigslist": _extract_craigslist,
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_listing(raw: dict, source: str) -> dict:
    """Map any raw listing dict to the unified listing schema.

    Args:
        raw: Raw listing data from a scraper.
        source: Source identifier string (e.g. "craigslist").

    Returns:
        A dict conforming to the unified listing schema.
    """
    extractor = _EXTRACTORS.get(source, _extract_generic)
    fields = extractor(raw)

    # Derive pet_friendly from pet_policy text if not explicitly set.
    pet_friendly = _parse_bool(fields.get("pet_friendly"))
    pet_policy = fields.get("pet_policy")
    if pet_friendly is None and pet_policy:
        policy_lower = str(pet_policy).lower()
        if any(kw in policy_lower for kw in ("allowed", "ok", "welcome", "yes", "friendly")):
            pet_friendly = True
        elif any(kw in policy_lower for kw in ("no pet", "not allowed", "none")):
            pet_friendly = False

    return {
        "source": source,
        "source_id": str(fields.get("source_id") or ""),
        "url": fields.get("url") or "",
        "title": fields.get("title") or "",
        "description": fields.get("description") or "",
        "rent": _parse_rent(fields.get("rent_raw")),
        "bedrooms": _parse_bedrooms(fields.get("bedrooms_raw")),
        "bathrooms": _parse_bathrooms(fields.get("bathrooms_raw")),
        "sqft": _parse_sqft(fields.get("sqft_raw")),
        "address": fields.get("address") or None,
        "neighborhood": fields.get("neighborhood") or None,
        "city": fields.get("city") or "",
        "pet_policy": pet_policy or None,
        "pet_friendly": pet_friendly,
        "amenities": _ensure_list(fields.get("amenities")),
        "images": _ensure_list(fields.get("images")),
        "lease_terms": fields.get("lease_terms") or None,
        "furnished": _parse_bool(fields.get("furnished")),
        "parking": fields.get("parking") or None,
        "laundry": fields.get("laundry") or None,
        "date_posted": fields.get("date_posted") or None,
        "is_active": True,
        "raw_data": raw,
    }
