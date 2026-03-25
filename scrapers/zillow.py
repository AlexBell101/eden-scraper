from __future__ import annotations

import httpx
import asyncio
import os
from datetime import date
from typing import Optional

RAPIDAPI_HOST = "zillow-scraper-api.p.rapidapi.com"
BASE_URL = f"https://{RAPIDAPI_HOST}"

def _headers() -> dict:
    return {
        "x-rapidapi-host": RAPIDAPI_HOST,
        "x-rapidapi-key": os.environ.get("RAPIDAPI_KEY", ""),
        "Content-Type": "application/json",
    }


async def search_rentals(location: str, max_price: int = None, beds_min: int = 1, listing_type: str = "for_rent", pages: int = 3) -> list[dict]:
    """Search Zillow for rental listings in a given location."""
    listings = []

    async with httpx.AsyncClient(timeout=30) as client:
        for page in range(1, pages + 1):
            params = {
                "location": location,
                "listing_type": listing_type,
                "beds_min": str(beds_min),
                "page": str(page),
            }
            if max_price:
                params["max_price"] = str(max_price)

            try:
                print(f"[Eden Zillow] Searching {location} — page {page}")
                resp = await client.get(f"{BASE_URL}/zillow/search", headers=_headers(), params=params)
                resp.raise_for_status()
                data = resp.json()

                # Unwrap nested data envelope if present
                data_obj = data.get("data", data) if isinstance(data.get("data"), dict) else data
                results = (
                    data_obj.get("listings")
                    or data_obj.get("results")
                    or data_obj.get("props")
                    or []
                )
                if not isinstance(results, list):
                    results = []
                if not results:
                    print(f"[Eden Zillow] No results on page {page}, stopping pagination")
                    break

                listings.extend(results)
                print(f"[Eden Zillow] Got {len(results)} listings from page {page}")

                # Rate limit — be respectful
                await asyncio.sleep(1.0)

            except httpx.HTTPStatusError as e:
                print(f"[Eden Zillow] HTTP error on page {page}: {e.response.status_code}")
                break
            except Exception as e:
                print(f"[Eden Zillow] Error on page {page}: {e}")
                break

    return listings


async def get_property_details(zpid: str) -> Optional[dict]:
    """Fetch full property details by zpid."""
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{BASE_URL}/zillow/property",
                headers=_headers(),
                params={"zpid": zpid},
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("data", data)
        except Exception as e:
            print(f"[Eden Zillow] Error fetching details for zpid {zpid}: {e}")
            return None


def normalize_listing(raw: dict, detail: Optional[dict] = None) -> Optional[dict]:
    """Normalize a Zillow search result + optional detail into our schema."""
    try:
        # Skip listings with no price AND no image (likely empty/invalid results)
        if not raw.get("price") and not raw.get("image_url"):
            return None

        # Skip non-rental home types
        home_type = str(raw.get("home_type") or raw.get("homeType") or "").upper()
        if any(t in home_type for t in ["LAND", "LOT", "MANUFACTURED"]):
            return None

        # zpid is the unique identifier
        zpid = str(raw.get("zpid") or raw.get("id") or "")
        # If zpid looks like coordinates, hash address instead for stable dedup
        if not zpid or "--" in zpid:
            addr_key = str(raw.get("address", "")) + str(raw.get("price", "")) + str(raw.get("bedrooms", ""))
            zpid = str(abs(hash(addr_key)))
        if not zpid:
            return None

        # Merge detail data if available
        data = {**raw, **(detail or {})}

        # Price — for rentals this is monthly rent
        price = data.get("price") or data.get("unformattedPrice") or data.get("listingPrice")
        if isinstance(price, str):
            price = int("".join(filter(str.isdigit, price))) if price else None
        elif isinstance(price, float):
            price = int(price)

        # Beds/baths
        beds = data.get("bedrooms") or data.get("beds") or data.get("bedrooms_count")
        if beds is None:
            beds_str = data.get("bedsAndBaths", "")
            beds = float(beds_str.split("bd")[0].strip()) if "bd" in str(beds_str) else None

        baths = data.get("bathrooms") or data.get("baths") or data.get("bathrooms_count")
        if baths is None:
            baths_str = data.get("bedsAndBaths", "")
            baths = float(baths_str.split("ba")[0].split("/")[-1].strip()) if "ba" in str(baths_str) else None

        # Sqft
        sqft = data.get("living_area_sqft") or data.get("livingArea") or data.get("sqft") or data.get("area")
        if isinstance(sqft, str):
            sqft = int("".join(filter(str.isdigit, sqft))) if sqft else None

        # Address
        address_parts = []
        if data.get("streetAddress"):
            address_parts.append(data["streetAddress"])
        elif data.get("address"):
            addr = data["address"]
            if isinstance(addr, dict):
                address_parts.append(addr.get("streetAddress", ""))
            else:
                address_parts.append(str(addr))

        city = data.get("city", "")
        state = data.get("state", "")
        if city:
            address_parts.append(city)
        if state:
            address_parts.append(state)

        address = ", ".join(p for p in address_parts if p)

        # Neighborhood
        neighborhood = (
            data.get("neighborhood")
            or data.get("neighborhoodName")
            or data.get("subdivisionName")
            or data.get("city")
            or ""
        )

        # Location
        lat = data.get("latitude") or data.get("lat")
        lng = data.get("longitude") or data.get("lng") or data.get("lon")

        # Use address string directly if available
        if not address and data.get("address"):
            address = str(data["address"])

        # Images — capture all possible image fields
        images = []
        for img_field in ["image_url", "imgSrc", "img_src", "thumbnail"]:
            val = data.get(img_field)
            if val and isinstance(val, str):
                images.append(val)
        if data.get("photos"):
            for p in data["photos"]:
                if isinstance(p, dict):
                    url = p.get("url") or p.get("mixedSources", {}).get("jpeg", [{}])[0].get("url", "")
                    if url:
                        images.append(url)
                elif isinstance(p, str):
                    images.append(p)
        images = list(dict.fromkeys(i for i in images if i))  # dedupe, preserve order

        # Amenities
        amenities = []
        if data.get("homeFeatures"):
            amenities = data["homeFeatures"] if isinstance(data["homeFeatures"], list) else []
        elif data.get("amenities"):
            amenities = data["amenities"] if isinstance(data["amenities"], list) else []
        elif data.get("atAGlanceFacts"):
            amenities = [f.get("factValue", "") for f in data["atAGlanceFacts"] if isinstance(f, dict)]

        # Pet policy
        description = data.get("description") or data.get("editorialSummary") or ""
        pet_keywords = ["pet", "dog", "cat", "animal"]
        pet_policy_raw = ""
        if description:
            sentences = description.split(".")
            for s in sentences:
                if any(k in s.lower() for k in pet_keywords):
                    pet_policy_raw = s.strip()
                    break

        pet_friendly = None
        if pet_policy_raw:
            negative = ["no pet", "no dog", "no cat", "not allow", "not accept"]
            pet_friendly = not any(n in pet_policy_raw.lower() for n in negative)

        # Laundry
        laundry = "unknown"
        amenities_str = " ".join(str(a) for a in amenities).lower() + description.lower()
        if "in-unit" in amenities_str or "in unit" in amenities_str or "washer/dryer" in amenities_str:
            laundry = "in-unit"
        elif "laundry" in amenities_str or "washer" in amenities_str:
            laundry = "shared"

        # Parking
        parking = None
        if "parking" in amenities_str or "garage" in amenities_str:
            parking = "available"

        # URL
        detail_url = data.get("detail_url") or data.get("detailUrl") or data.get("url") or ""
        if detail_url and not detail_url.startswith("http"):
            detail_url = f"https://www.zillow.com{detail_url}"

        # Date posted
        days_on = data.get("days_on_zillow") or data.get("daysOnZillow") or data.get("timeOnZillow")
        date_posted = None
        if days_on is not None:
            from datetime import timedelta
            date_posted = (date.today() - timedelta(days=int(days_on))).isoformat()

        return {
            "source": "zillow",
            "source_id": zpid,
            "url": detail_url,
            "title": data.get("statusText") or f"{beds}bd/{baths}ba in {neighborhood or city}",
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
            "amenities": amenities[:20],  # cap at 20
            "images": images[:10],         # cap at 10
            "lease_terms": data.get("leaseTerms") or None,
            "furnished": None,
            "parking": parking,
            "laundry": laundry,
            "date_posted": date_posted,
            "raw_data": raw,
        }

    except Exception as e:
        print(f"[Eden Zillow] Error normalizing listing: {e}")
        return None


def _clean_location(label: str) -> str:
    """
    Trim a verbose Mapbox geocoding label down to something Zillow accepts.
    e.g. "South Bay, Los Angeles, California, United States"
         → "South Bay, CA"
    """
    # Strip country
    label = label.replace(", United States", "").strip()
    parts = [p.strip() for p in label.split(",")]
    if len(parts) >= 2:
        # Abbreviate the last part (state) if it's a full state name
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


def _in_bounds(listing: dict, bounds: dict) -> bool:
    """Return True if the listing's coordinates fall inside the user's map bounding box.

    If the listing has no coordinates, we let it through (fail open) so a lack
    of lat/lng data never silently drops a real result.
    """
    lat = listing.get("lat")
    lng = listing.get("lng")
    if lat is None or lng is None:
        return True  # no coords → can't verify, keep it
    try:
        return (
            float(bounds["sw_lat"]) <= float(lat) <= float(bounds["ne_lat"])
            and float(bounds["sw_lng"]) <= float(lng) <= float(bounds["ne_lng"])
        )
    except (KeyError, TypeError, ValueError):
        return True  # malformed bounds → keep listing


async def scrape_for_user(user: dict) -> list[dict]:
    """Scrape Zillow tailored to a specific user's preferences."""
    raw_city = user.get("target_city") or "San Francisco, CA"
    location = _clean_location(raw_city)  # normalise even manually-typed cities
    max_rent = user.get("max_rent")
    min_bedrooms = user.get("min_bedrooms") or 1

    # If user has search_bounds, prefer its label (it has state context from Mapbox)
    bounds = user.get("search_bounds")
    if bounds and isinstance(bounds, dict):
        raw_label = bounds.get("label", "")
        if raw_label:
            location = _clean_location(raw_label)

    listing_type = user.get("listing_type") or "for_rent"
    print(f"[Eden Zillow] Scraping for user {user.get('email')} — '{location}', {listing_type}, max ${max_rent}, {min_bedrooms}+ beds")

    raw_listings = await search_rentals(
        location=location,
        max_price=max_rent,
        beds_min=min_bedrooms,
        listing_type=listing_type,
        pages=3,
    )

    if not raw_listings:
        print(f"[Eden Zillow] No raw listings found for {location}")
        return []

    # Normalize without fetching details (zpid from this API is lat/lng, not real zpid)
    normalized = []
    out_of_area = 0
    for raw in raw_listings[:40]:
        if not isinstance(raw, dict):
            continue
        listing = normalize_listing(raw, None)
        if not listing:
            continue

        # Geo-fence: discard listings outside the user's drawn map bounds.
        # This prevents the Zillow API from returning off-target national results.
        if bounds and isinstance(bounds, dict):
            if not _in_bounds(listing, bounds):
                out_of_area += 1
                print(
                    f"[Eden Zillow] Skipping out-of-area listing: "
                    f"{listing.get('city', '?')} "
                    f"(lat={listing.get('lat')}, lng={listing.get('lng')})"
                )
                continue

        normalized.append(listing)

    if out_of_area:
        print(f"[Eden Zillow] Dropped {out_of_area} out-of-area listings for {location}")
    print(f"[Eden Zillow] Normalized {len(normalized)} listings for {location}")
    return normalized
