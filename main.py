from __future__ import annotations

import asyncio
from datetime import datetime

from db import (
    get_active_users,
    get_unscored_listings,
    get_user_criteria,
    save_score,
    upsert_listings,
)
from scorer import score_listing
from scrapers.zillow import scrape_for_user

_SCORE_RATE_LIMIT_DELAY = 0.5  # seconds between Claude API calls


def _should_skip_for_user(listing: dict, user: dict) -> bool:
    """Return True if this listing should be skipped for a given user."""
    max_rent = user.get("max_rent")
    listing_rent = listing.get("rent", 0)
    if max_rent and listing_rent and listing_rent > max_rent:
        return True

    pet_required = user.get("pet_required", False)
    pet_friendly = listing.get("pet_friendly")
    if pet_required and pet_friendly is False:
        return True

    return False


async def run() -> None:
    print(f"[Eden Scraper] Starting run at {datetime.now()}")

    users = get_active_users()
    print(f"[Eden] Active users: {len(users)}")

    total_scraped = 0
    total_new = 0
    total_scored = 0
    total_skipped = 0

    for user in users:
        user_id: str = user.get("id", "")
        user_name: str = user.get("email") or user_id

        # ------------------------------------------------------------------
        # Step 1: Scrape Zillow tailored to this user's preferences
        # ------------------------------------------------------------------
        try:
            raw_listings = await scrape_for_user(user)
            total_scraped += len(raw_listings)
        except Exception as exc:
            print(f"[Eden] Zillow scrape failed for {user_name}: {exc}")
            raw_listings = []

        # ------------------------------------------------------------------
        # Step 2: Upsert listings to DB
        # ------------------------------------------------------------------
        if raw_listings:
            try:
                new_listings = upsert_listings(raw_listings)
                total_new += len(new_listings)
                print(f"[Eden] {len(new_listings)} new listings upserted for {user_name}")
            except Exception as exc:
                print(f"[Eden] DB upsert failed for {user_name}: {exc}")

        # ------------------------------------------------------------------
        # Step 3: Score all unscored listings for this user
        # ------------------------------------------------------------------
        criteria = get_user_criteria(user_id)
        if not criteria:
            print(f"[Eden] Skipping scoring for {user_name} — no criteria defined.")
            continue

        unscored = get_unscored_listings(user_id)
        print(f"[Eden] User {user_name}: {len(unscored)} unscored listings to evaluate.")

        for listing in unscored:
            listing_id = listing.get("id", "<unknown>")

            if _should_skip_for_user(listing, user):
                total_skipped += 1
                print(f"[Eden]   Skipping listing {listing_id} for {user_name} (rent/pet filter).")
                continue

            try:
                score = await score_listing(listing, criteria, user)
                save_score(score)
                total_scored += 1
                print(f"[Eden]   Scored listing {listing_id} for {user_name}: {score['overall_score']:.1f}/10")
            except Exception as exc:
                print(f"[Eden]   Failed to score listing {listing_id} for {user_name}: {exc}")
                continue

            await asyncio.sleep(_SCORE_RATE_LIMIT_DELAY)

    print(
        f"\n[Eden Scraper] Run complete at {datetime.now()}\n"
        f"  Listings scraped : {total_scraped}\n"
        f"  New listings     : {total_new}\n"
        f"  Listings scored  : {total_scored}\n"
        f"  Listings skipped : {total_skipped}\n"
    )


if __name__ == "__main__":
    asyncio.run(run())
