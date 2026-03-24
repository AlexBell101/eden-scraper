from __future__ import annotations

import asyncio
from datetime import datetime

from config import TARGET_CITIES
from db import (
    get_active_users,
    get_unscored_listings,
    get_user_criteria,
    save_score,
    upsert_listings,
)
from normalizer import normalize_listing
from scorer import score_listing
from scrapers.craigslist import CraigslistScraper

_SCORE_RATE_LIMIT_DELAY = 0.5  # seconds between Claude API calls


async def _scrape_all_cities(cities: list[str]) -> list[dict]:
    """Run all scrapers for the given cities and return normalized listings."""
    all_normalized: list[dict] = []

    for city in cities:
        scrapers = [
            CraigslistScraper(city),
            # Additional scrapers for this city can be added here.
        ]
        for scraper in scrapers:
            source_name = type(scraper).__name__.lower().replace("scraper", "")
            try:
                raw_listings = await scraper.scrape()
                print(
                    f"[Eden] Scraped {len(raw_listings)} raw listings "
                    f"from {source_name} / {city}"
                )
            except Exception as exc:
                print(
                    f"[Eden] Scraper {source_name} / {city} crashed: {exc}"
                )
                continue

            for raw in raw_listings:
                try:
                    normalized = normalize_listing(raw, source_name)
                    all_normalized.append(normalized)
                except Exception as exc:
                    print(
                        f"[Eden] Failed to normalize listing "
                        f"(source={source_name}, city={city}): {exc}"
                    )
                    continue

    return all_normalized


def _should_skip_for_user(listing: dict, user: dict) -> bool:
    """Return True if this listing should be skipped for a given user."""
    # Skip if rent exceeds user's max_rent.
    max_rent = user.get("max_rent")
    listing_rent = listing.get("rent", 0)
    if max_rent and listing_rent and listing_rent > max_rent:
        return True

    # Skip if user requires pet-friendly but listing explicitly disallows pets.
    pet_required = user.get("pet_required", False)
    pet_friendly = listing.get("pet_friendly")
    if pet_required and pet_friendly is False:
        return True

    return False


async def run() -> None:
    print(f"[Eden Scraper] Starting run at {datetime.now()}")
    print(f"[Eden Scraper] Target cities: {', '.join(TARGET_CITIES)}")

    # ------------------------------------------------------------------
    # Step 1 & 2: Scrape all sources and normalize.
    # ------------------------------------------------------------------
    all_listings = await _scrape_all_cities(TARGET_CITIES)
    print(f"[Eden] Total normalized listings: {len(all_listings)}")

    # ------------------------------------------------------------------
    # Step 3: Upsert to DB if we scraped anything.
    # ------------------------------------------------------------------
    new_listing_ids: list[str] = []
    if all_listings:
        try:
            new_listings = upsert_listings(all_listings)
            new_listing_ids = [l["id"] for l in new_listings if l.get("id")]
            print(f"[Eden] New listings inserted: {len(new_listings)}")
        except Exception as exc:
            print(f"[Eden] DB upsert failed: {exc}")

    # ------------------------------------------------------------------
    # Step 4: For each active user, score ALL unscored listings in DB.
    # ------------------------------------------------------------------
    users = get_active_users()
    print(f"[Eden] Active users: {len(users)}")

    total_scored = 0
    total_skipped = 0

    for user in users:
        user_id: str = user.get("id", "")
        user_name: str = user.get("email") or user_id

        criteria = get_user_criteria(user_id)
        if not criteria:
            print(f"[Eden] Skipping user {user_name} — no criteria defined.")
            continue

        # Score all unscored listings (not just newly scraped ones)
        unscored = get_unscored_listings(user_id)
        print(
            f"[Eden] User {user_name}: {len(unscored)} unscored "
            f"listings to evaluate."
        )

        for listing in unscored:
            listing_id = listing.get("id", "<unknown>")

            if _should_skip_for_user(listing, user):
                total_skipped += 1
                print(
                    f"[Eden]   Skipping listing {listing_id} "
                    f"for user {user_name} (rent/pet filter)."
                )
                continue

            try:
                score = await score_listing(listing, criteria, user)
                save_score(score)
                total_scored += 1
                print(
                    f"[Eden]   Scored listing {listing_id} "
                    f"for user {user_name}: {score['overall_score']:.1f}/10"
                )
            except Exception as exc:
                print(
                    f"[Eden]   Failed to score listing {listing_id} "
                    f"for user {user_name}: {exc}"
                )
                continue

            # Rate-limit Claude API calls.
            await asyncio.sleep(_SCORE_RATE_LIMIT_DELAY)

    # ------------------------------------------------------------------
    # Step 5: Summary.
    # ------------------------------------------------------------------
    print(
        f"\n[Eden Scraper] Run complete at {datetime.now()}\n"
        f"  Listings scraped : {len(all_listings)}\n"
        f"  New listings     : {len(new_listing_ids)}\n"
        f"  Listings scored  : {total_scored}\n"
        f"  Listings skipped : {total_skipped}\n"
    )


if __name__ == "__main__":
    asyncio.run(run())
