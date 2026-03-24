from __future__ import annotations

from functools import lru_cache
from typing import Any

from supabase import create_client, Client

from config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY


@lru_cache(maxsize=1)
def get_client() -> Client:
    """Return a cached Supabase client using the service role key."""
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def upsert_listings(listings: list[dict]) -> list[dict]:
    """Upsert listings to the DB and return the ones that were newly inserted.

    Deduplication is performed on the (source, source_id) composite key.
    The function compares before/after counts per (source, source_id) pair
    to identify brand-new rows.
    """
    if not listings:
        return []

    client = get_client()

    # Collect existing (source, source_id) pairs so we can find new ones.
    source_id_pairs = [
        {"source": l["source"], "source_id": l["source_id"]} for l in listings
    ]

    existing_ids: set[tuple[str, str]] = set()
    try:
        for pair in source_id_pairs:
            resp = (
                client.table("listings")
                .select("source, source_id")
                .eq("source", pair["source"])
                .eq("source_id", pair["source_id"])
                .execute()
            )
            for row in resp.data or []:
                existing_ids.add((row["source"], row["source_id"]))
    except Exception as exc:
        print(f"[Eden DB] Warning: could not fetch existing listings: {exc}")

    # Upsert all listings using on_conflict resolution.
    try:
        client.table("listings").upsert(
            listings, on_conflict="source,source_id"
        ).execute()
    except Exception as exc:
        print(f"[Eden DB] Error upserting listings: {exc}")
        return []

    # Identify and return the newly inserted rows.
    new_listings = [
        l
        for l in listings
        if (l["source"], l["source_id"]) not in existing_ids
    ]
    return new_listings


def get_active_users() -> list[dict]:
    """Return all user profiles."""
    client = get_client()
    try:
        resp = client.table("profiles").select("*").execute()
        return resp.data or []
    except Exception as exc:
        print(f"[Eden DB] Error fetching active users: {exc}")
        return []


def get_user_criteria(user_id: str) -> list[dict]:
    """Return scoring criteria for a user, ordered by sort_order."""
    client = get_client()
    try:
        resp = (
            client.table("criteria")
            .select("*")
            .eq("user_id", user_id)
            .order("sort_order", desc=False)
            .execute()
        )
        return resp.data or []
    except Exception as exc:
        print(f"[Eden DB] Error fetching criteria for user {user_id}: {exc}")
        return []


def get_unscored_listings(user_id: str, listing_ids: list[str] | None = None) -> list[dict]:
    """Return listings that do not yet have a score for user_id.

    If listing_ids is provided, only check those listings.
    If None, check all active listings in the DB.
    """
    client = get_client()
    try:
        if listing_ids is not None and len(listing_ids) == 0:
            return []

        # Fetch IDs already scored for this user.
        scored_query = (
            client.table("scores")
            .select("listing_id")
            .eq("user_id", user_id)
        )
        if listing_ids:
            scored_query = scored_query.in_("listing_id", listing_ids)

        scored_resp = scored_query.execute()
        scored_ids: set[str] = {
            row["listing_id"] for row in (scored_resp.data or [])
        }

        # Fetch active listings
        listings_query = (
            client.table("listings")
            .select("*")
            .eq("is_active", True)
        )
        if listing_ids:
            listings_query = listings_query.in_("id", listing_ids)

        listings_resp = listings_query.execute()
        all_listings = listings_resp.data or []

        return [l for l in all_listings if l["id"] not in scored_ids]
    except Exception as exc:
        print(
            f"[Eden DB] Error fetching unscored listings for user {user_id}: {exc}"
        )
        return []


def save_score(score: dict) -> None:
    """Upsert a score record to the scores table."""
    client = get_client()
    try:
        client.table("scores").upsert(
            score, on_conflict="user_id,listing_id"
        ).execute()
    except Exception as exc:
        print(f"[Eden DB] Error saving score: {exc}")


def get_active_households() -> list[dict]:
    """Return all households with their members and each member's profile."""
    client = get_client()
    try:
        resp = client.table("households").select(
            "id, name, household_members(user_id, role)"
        ).execute()
        return resp.data or []
    except Exception as exc:
        print(f"[Eden DB] Error fetching households: {exc}")
        return []


def get_household_unscored_listings(household_id: str) -> list[dict]:
    """Return listings not yet scored for this household."""
    client = get_client()
    try:
        scored_resp = (
            client.table("scores")
            .select("listing_id")
            .eq("household_id", household_id)
            .execute()
        )
        scored_ids = {row["listing_id"] for row in (scored_resp.data or [])}

        listings_resp = (
            client.table("listings")
            .select("*")
            .eq("is_active", True)
            .execute()
        )
        all_listings = listings_resp.data or []
        return [l for l in all_listings if l["id"] not in scored_ids]
    except Exception as exc:
        print(f"[Eden DB] Error fetching unscored listings for household {household_id}: {exc}")
        return []


def save_household_score(score: dict) -> None:
    """Save a household score to the scores table."""
    client = get_client()
    try:
        # Save one score row per member + one for the household
        for user_id, member_data in score.get("member_scores", {}).items():
            row = {
                "user_id": user_id,
                "listing_id": score["listing_id"],
                "household_id": score["household_id"],
                "overall_score": member_data.get("overall_score", 5.0),
                "criteria_scores": member_data.get("criteria_scores", {}),
                "above_threshold": member_data.get("overall_score", 0) >= 5.0,
                "claude_reasoning": score.get("household_narrative", ""),
                "red_flags": member_data.get("red_flags", []),
                "highlights": member_data.get("highlights", []),
            }
            client.table("scores").upsert(row, on_conflict="user_id,listing_id").execute()
    except Exception as exc:
        print(f"[Eden DB] Error saving household score: {exc}")
