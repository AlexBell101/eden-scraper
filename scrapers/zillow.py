from __future__ import annotations

import httpx
import asyncio
import os
from datetime import date
from typing import Optional

RAPIDAPI_HOST = "zillow-real-estate-api.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}/v1"


def _headers() -> dict:
    return {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": os.environ.get("RAPIDAPI_KEY", ""),
    }


# ── Search helpers ────────────────────────────────────────────────────────────

async def _search_page(
    client: httpx.AsyncClient,
    params: dict,
    page: int,
    use_coords: bool,
) -> list[dict]:
    """Fetch one page from the appropriate search endpoint."""
    endpoint = f"{BASE_URL}/search/coordinates" if use_coords else f"{BASE_URL}/search"
    try:
        resp = await client.get(endpoint, headers=_headers(), params={**params, "page": str(page)})
        resp.raise_for_status()
        body = resp.json()
        results = (
            body.get("data", {}).get("results")
            or body.get("results")
            or []
        )
        return results if isinstance(results, list) else []
    except httpx.HTTPStatusError as e:
        print(f"[Eden Zillow] HTTP {e.response.status_code} on page {page}")
        return []
    except Exception as e:
        print(f"[Eden Zillow] Error on page {page}: {e}")
        return []


async def search_by_coordinates(
    north: float, south: float, east: float, west: float,
    status: str = "for_rent",
    beds_min: int = 1,
    max_price: Optional[int] = None,
    pages: int = 3,
) -> list[dict]:
    """Search using the map bounding box — most precise, no location string needed."""
    params: dict = {
        "north": str(north),
        "south": str(south),
        "east": str(east),
        "west": str(west),
        "status": status,
        "beds_min": str(beds_min),
        "page_size": "40",
        "result_type": "list",
        "sort": "relevance",
        "zoom": "13",
    }
    if max_price:
        params["price_max"] = str(max_price)

    listings: list[dict] = []
    async with httpx.AsyncClient(timeout=30) as client:
        for page in range(1, pages + 1):
            print(f"[Eden Zillow] Coordinate search — page {page}")
            results = await _search_page(client, params, page, use_coords=True)
            if not results:
                print(f"[Eden Zillow] No results on page {page}, stopping")
                break
            listings.extend(results)
            print(f"[Eden Zillow] Got {len(results)} listings from page {page}")
            await asyncio.sleep(1.0)
    return listings


async def search_by_location(
    location: str,
    status: str = "for_rent",
    beds_min: int = 1,
    max_price: Optional[int] = None,
    pages: int = 3,
) -> list[dict]:
    """Search by city/neighbourhood string — fallback when no bounds are set."""
    params: dict = {
        "location": location,
        "status": status,
        "beds_min": str(beds_min),
        "page_size": "40",
        "result_type": "list",
        "sort": "newest",
    }
    if max_price:
        params["price_max"] = str(max_price)

    listings: list[dict] = []
    async with httpx.AsyncClient(timeout=30) as client:
        for page in range(1, pages + 1):
            print(f"[Eden Zillow] Location search '{location}' — page {page}")
            results = await _search_page(client, params, page, use_coords=False)
            if not results:
                print(f"[Eden Zillow] No results on page {page}, stopping")
                break
            listings.extend(results)
            print(f"[Eden Zillow] Got {len(results)} listings from page {page}")
            await asyncio.sleep(1.0)
    return listings


# ── Normalisation ─────────────────────────────────────────────────────────────

def normalize_listing(raw: dict, detail: Optional[dict] = None) -> Optional[dict]:
    """Normalise a zillow-real-estate-api result into our schema.

    The new API returns 350+ fields; we extract what's useful for scoring.
    """
    try:
        data = {**raw, **(detail or {})}

        # ── Identity ──────────────────────────────────────────────────────────
        zpid = str(data.get("zpid") or "")
        if not zpid:
            return None

        # Skip non-residential types
        home_type = str(data.get("home_type") or data.get("homeType") or "").upper()
        if any(t in home_type for t in ["LAND", "LOT", "MANUFACTURED"]):
            return None

        # ── Price ─────────────────────────────────────────────────────────────
        price = data.get("price") or data.get("unformatted_price") or data.get("unformattedPrice")
        if isinstance(price, str):
            price = int("".join(filter(str.isdigit, price))) if price else None
        elif isinstance(price, float):
            price = int(price)

        # ── Beds / baths ──────────────────────────────────────────────────────
        beds = data.get("beds") or data.get("bedrooms")
        baths = data.get("baths") or data.get("bathrooms")

        # ── Sqft ──────────────────────────────────────────────────────────────
        sqft = data.get("sqft") or data.get("living_area") or data.get("livingArea")
        if isinstance(sqft, str):
            sqft = int("".join(filter(str.isdigit, sqft))) if sqft else None

        # ── Address ───────────────────────────────────────────────────────────
        addr_raw = data.get("address") or {}
        if isinstance(addr_raw, dict):
            street    = addr_raw.get("street_address") or addr_raw.get("streetAddress") or ""
            city      = addr_raw.get("city") or ""
            state     = addr_raw.get("state") or ""
            zipcode   = addr_raw.get("zipcode") or addr_raw.get("zip") or ""
        else:
            # flat string address
            street = str(addr_raw)
            city = data.get("city") or ""
            state = data.get("state") or ""
            zipcode = data.get("zipcode") or ""

        parts = [p for p in [street, city, state] if p]
        address = ", ".join(parts)

        # ── Location ──────────────────────────────────────────────────────────
        lat = data.get("latitude") or data.get("lat")
        lng = data.get("longitude") or data.get("longitude") or data.get("lng")

        # ── Neighborhood ──────────────────────────────────────────────────────
        neighborhood = (
            data.get("neighborhood")
            or data.get("neighborhoodName")
            or data.get("subdivision")
            or city
            or ""
        )

        # ── Images ────────────────────────────────────────────────────────────
        images: list[str] = []
        # New API: photos is a list of objects with url variants
        photos = data.get("photos") or []
        for p in photos:
            if isinstance(p, dict):
                url = (
                    p.get("url")
                    or p.get("medium_url")
                    or p.get("small_url")
                    or ""
                )
                if url:
                    images.append(url)
            elif isinstance(p, str):
                images.append(p)
        # Fall back to flat image fields
        for field in ["image_url", "imgSrc", "img_src", "thumbnail"]:
            val = data.get(field)
            if val and isinstance(val, str):
                images.append(val)
        images = list(dict.fromkeys(i for i in images if i))

        # ── Description ───────────────────────────────────────────────────────
        description = data.get("description") or data.get("editorial_summary") or ""

        # ── Amenities — much richer in new API ───────────────────────────────
        amenities: list[str] = []
        # New API may have facts/features as a dict or list
        facts = data.get("facts") or data.get("home_facts") or {}
        if isinstance(facts, dict):
            for k, v in facts.items():
                if v and str(v).lower() not in ("none", "false", "0", "unknown"):
                    amenities.append(f"{k}: {v}")
        elif isinstance(facts, list):
            amenities = [str(f) for f in facts if f]

        # Also pull any explicit amenities list
        for field in ["amenities", "homeFeatures", "atAGlanceFacts"]:
            val = data.get(field)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        amenities.append(item.get("factLabel") or item.get("factValue") or str(item))
                    elif isinstance(item, str):
                        amenities.append(item)

        # ── Schools (new API) ─────────────────────────────────────────────────
        schools = data.get("schools") or []
        school_summary: list[str] = []
        for s in schools[:5]:
            if isinstance(s, dict):
                name  = s.get("name") or ""
                level = s.get("level") or s.get("type") or ""
                rating = s.get("rating") or s.get("gs_rating") or ""
                if name:
                    school_summary.append(f"{name} ({level}, rating {rating}/10)")
        if school_summary:
            amenities.extend(school_summary)

        # ── Pet policy ────────────────────────────────────────────────────────
        pet_policy_raw = data.get("pet_policy") or data.get("petPolicy") or ""
        if not pet_policy_raw and description:
            pet_keywords = ["pet", "dog", "cat", "animal"]
            for s in description.split("."):
                if any(k in s.lower() for k in pet_keywords):
                    pet_policy_raw = s.strip()
                    break

        pet_friendly: Optional[bool] = None
        if pet_policy_raw:
            negative = ["no pet", "no dog", "no cat", "not allow", "not accept"]
            pet_friendly = not any(n in str(pet_policy_raw).lower() for n in negative)

        # ── Laundry ───────────────────────────────────────────────────────────
        amenities_str = " ".join(str(a) for a in amenities).lower() + description.lower()
        laundry = "unknown"
        if "in-unit" in amenities_str or "in unit" in amenities_str or "washer/dryer" in amenities_str:
            laundry = "in-unit"
        elif "laundry" in amenities_str or "washer" in amenities_str:
            laundry = "shared"

        # ── Parking ───────────────────────────────────────────────────────────
        parking = None
        parking_data = data.get("parking") or {}
        if isinstance(parking_data, dict) and parking_data:
            parking = "available"
        elif "parking" in amenities_str or "garage" in amenities_str:
            parking = "available"

        # ── HOA (new API) ─────────────────────────────────────────────────────
        hoa = data.get("hoa") or data.get("hoa_fee") or {}
        if isinstance(hoa, dict):
            hoa_fee = hoa.get("fee") or hoa.get("monthly_fee")
            if hoa_fee:
                amenities.append(f"HOA: ${hoa_fee}/month")

        # ── URL ───────────────────────────────────────────────────────────────
        detail_url = data.get("url") or data.get("detail_url") or data.get("detailUrl") or ""
        if detail_url and not detail_url.startswith("http"):
            detail_url = f"https://www.zillow.com{detail_url}"
        if not detail_url and zpid:
            detail_url = f"https://www.zillow.com/homedetails/{zpid}_zpid/"

        # ── Date posted ───────────────────────────────────────────────────────
        days_on = data.get("days_on_zillow") or data.get("daysOnZillow") or data.get("days_on_market")
        date_posted = None
        if days_on is not None:
            from datetime import timedelta
            try:
                date_posted = (date.today() - timedelta(days=int(days_on))).isoformat()
            except (ValueError, TypeError):
                pass

        # ── Lease terms ───────────────────────────────────────────────────────
        lease_terms = data.get("lease_terms") or data.get("leaseTerms") or None

        # ── Furnished ─────────────────────────────────────────────────────────
        furnished: Optional[bool] = None
        if "furnished" in amenities_str:
            furnished = True
        elif "unfurnished" in amenities_str:
            furnished = False

        return {
            "source": "zillow",
            "source_id": zpid,
            "url": detail_url,
            "title": f"{beds}bd/{baths}ba in {neighborhood or city}",
            "description": description,
            "rent": price,
            "bedrooms": float(beds) if beds is not None else None,
            "bathrooms": float(baths) if baths is not None else None,
            "sqft": sqft,
            "address": address,
            "neighborhood": neighborhood,
            "city": city,
            "lat": float(lat) if lat else None,
            "lng": float(lng) if lng else None,
            "pet_policy": pet_policy_raw or None,
            "pet_friendly": pet_friendly,
            "amenities": amenities[:30],   # more room now
            "images": images[:10],
            "lease_terms": lease_terms,
            "furnished": furnished,
            "parking": parking,
            "laundry": laundry,
            "date_posted": date_posted,
            "raw_data": raw,
        }

    except Exception as e:
        print(f"[Eden Zillow] Error normalizing listing: {e}")
        return None


# ── Location cleaning (used for fallback text search) ─────────────────────────

def _clean_location(label: str) -> str:
    """
    Trim a verbose Mapbox geocoding label down to something Zillow accepts.
    e.g. "South Bay, Los Angeles, California, United States" → "South Bay, CA"
    """
    label = label.replace(", United States", "").strip()
    parts = [p.strip() for p in label.split(",")]
    if len(parts) >= 2:
        state_abbrevs = {
            "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
            "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
            "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
            "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
            "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
            "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
            "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
            "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
            "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
            "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
            "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
            "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
            "Wisconsin": "WI", "Wyoming": "WY",
        }
        state = parts[-1].strip()
        abbrev = state_abbrevs.get(state, state)
        return f"{parts[0]}, {abbrev}"
    return label


# ── Main entry point ──────────────────────────────────────────────────────────

async def scrape_for_user(user: dict) -> list[dict]:
    """Scrape Zillow tailored to a specific user's preferences."""
    max_rent   = user.get("max_rent")
    min_beds   = int(user.get("min_bedrooms") or 1)
    listing_type = user.get("listing_type") or "for_rent"
    # New API uses "for_rent" / "for_sale" — same values, no mapping needed

    bounds = user.get("search_bounds")
    use_coords = bounds and isinstance(bounds, dict) and all(
        k in bounds for k in ("sw_lat", "sw_lng", "ne_lat", "ne_lng")
    )

    if use_coords:
        print(
            f"[Eden Zillow] Coordinate search for {user.get('email')} — "
            f"bounds: N{bounds['ne_lat']:.3f} S{bounds['sw_lat']:.3f} "
            f"E{bounds['ne_lng']:.3f} W{bounds['sw_lng']:.3f}, "
            f"{listing_type}, max ${max_rent}, {min_beds}+ beds"
        )
        raw_listings = await search_by_coordinates(
            north=float(bounds["ne_lat"]),
            south=float(bounds["sw_lat"]),
            east=float(bounds["ne_lng"]),
            west=float(bounds["sw_lng"]),
            status=listing_type,
            beds_min=min_beds,
            max_price=max_rent,
            pages=3,
        )
    else:
        # Fall back to location string
        raw_city = user.get("target_city") or "San Francisco, CA"
        location = _clean_location(raw_city)
        print(
            f"[Eden Zillow] Location search for {user.get('email')} — "
            f"'{location}', {listing_type}, max ${max_rent}, {min_beds}+ beds"
        )
        raw_listings = await search_by_location(
            location=location,
            status=listing_type,
            beds_min=min_beds,
            max_price=max_rent,
            pages=3,
        )

    if not raw_listings:
        print(f"[Eden Zillow] No raw listings returned")
        return []

    normalized: list[dict] = []
    for raw in raw_listings[:60]:   # slightly higher cap — new API has better data
        if not isinstance(raw, dict):
            continue
        listing = normalize_listing(raw)
        if listing:
            normalized.append(listing)

    print(f"[Eden Zillow] Normalized {len(normalized)} listings")
    return normalized
