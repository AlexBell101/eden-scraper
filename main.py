from __future__ import annotations

import asyncio
from datetime import datetime

from db import (
    get_active_users,
    get_active_households,
    get_household_unscored_listings,
    get_unscored_listings,
    get_user_criteria,
    save_score,
    save_household_score,
    upsert_listings,
)
from household_scorer import score_listing_for_household
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

    # ------------------------------------------------------------------
    # Phase 2: Household scoring
    # ------------------------------------------------------------------
    households = get_active_households()
    print(f"\n[Eden] Active households: {len(households)}")

    total_household_scored = 0

    for household in households:
        household_id = household["id"]
        household_name = household.get("name", household_id)
        raw_members = household.get("household_members", [])

        if len(raw_members) < 2:
            print(f"[Eden] Skipping household '{household_name}' — needs ≥2 members.")
            continue

        # Build members list with user profile + criteria
        members = []
        for m in raw_members:
            uid = m["user_id"]
            user_profile = next((u for u in users if u["id"] == uid), None)
            if not user_profile:
                continue
            criteria = get_user_criteria(uid)
            if criteria:
                members.append({"user": user_profile, "criteria": criteria})

        if len(members) < 2:
            print(f"[Eden] Skipping household '{household_name}' — not enough members with criteria.")
            continue

        unscored = get_household_unscored_listings(household_id)
        print(f"[Eden] Household '{household_name}': {len(unscored)} unscored listings.")

        for listing in unscored:
            listing_id = listing.get("id", "<unknown>")
            try:
                result = await score_listing_for_household(listing, household_id, members)
                if result:
                    save_household_score(result)
                    total_household_scored += 1
                    print(f"[Eden]   Household scored {listing_id}: {result['household_score']:.1f}/10 (compromise: {result['compromise_rating']:.1f})")
            except Exception as exc:
                print(f"[Eden]   Household scoring failed for {listing_id}: {exc}")

            await asyncio.sleep(_SCORE_RATE_LIMIT_DELAY)

    print(
        f"\n[Eden Scraper] Run complete at {datetime.now()}\n"
        f"  Listings scraped       : {total_scraped}\n"
        f"  New listings           : {total_new}\n"
        f"  Individual scores      : {total_scored}\n"
        f"  Listings skipped       : {total_skipped}\n"
        f"  Household scores       : {total_household_scored}\n"
    )


if __name__ == "__main__":
    asyncio.run(run())
