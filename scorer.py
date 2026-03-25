from __future__ import annotations

import json
import logging
import re

import anthropic

from config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

_MODEL = "claude-opus-4-5"

_FALLBACK_SCORE = 5.0
_FALLBACK_REASONING = "Score could not be computed due to an API or parse error."


def _build_prompt(listing: dict, criteria: list[dict], user: dict) -> str:
    """Construct the scoring prompt for Claude."""
    criteria_lines = []
    for c in criteria:
        weight = c.get("weight", 1)
        cid = c.get("id", "")
        name = c.get("name", "")
        description = c.get("description", "")
        criteria_lines.append(
            f"  - id: {cid}, name: {name}, weight: {weight}, description: {description}"
        )
    criteria_block = "\n".join(criteria_lines) if criteria_lines else "  (none)"

    user_city = user.get("target_city") or user.get("city", "N/A")
    max_rent = user.get("max_rent", "N/A")
    pet_type = user.get("pet_type", "N/A")
    pet_required = user.get("pet_required", False)
    listing_type = user.get("listing_type", "for_rent")
    is_rental = listing_type == "for_rent"
    display_name = user.get("display_name") or user.get("email", "").split("@")[0].title() or "the user"
    vibe_text = user.get("vibe_text", "")

    amenities = listing.get("amenities") or []
    amenities_str = ", ".join(amenities) if amenities else "N/A"

    mode_context = (
        "The user is looking to RENT. Score lease flexibility, pet policy, and monthly affordability highly."
        if is_rental else
        "The user is looking to BUY. Focus on long-term value, location appreciation, home quality, and ownership costs. Ignore lease flexibility criteria."
    )

    price_label = "Rent" if is_rental else "Price"
    price_value = listing.get("rent", "N/A")

    prompt = f"""You are a property listing evaluator for Eden. Score the following listing against a user's criteria.

## Mode
{mode_context}

## User Profile
- Name: {display_name}
- City preference: {user_city}
- Max {"rent" if is_rental else "price"}: {max_rent}
- Pet type: {pet_type}
- Pet required: {pet_required}
- In their own words: {vibe_text if vibe_text else "(no vibe set)"}

## Scoring Criteria (each has an id, name, weight, and description)
{criteria_block}

## Listing Details
- Title: {listing.get('title', 'N/A')}
- URL: {listing.get('url', 'N/A')}
- {price_label}: ${price_value}
- Bedrooms: {listing.get('bedrooms', 'N/A')}
- Bathrooms: {listing.get('bathrooms', 'N/A')}
- Sqft: {listing.get('sqft', 'N/A')}
- City: {listing.get('city', 'N/A')}
- Neighborhood: {listing.get('neighborhood', 'N/A')}
- Address: {listing.get('address', 'N/A')}
- Pet Policy: {listing.get('pet_policy', 'N/A')}
- Pet Friendly: {listing.get('pet_friendly', 'N/A')}
- Amenities: {amenities_str}
- Laundry: {listing.get('laundry', 'N/A')}
- Parking: {listing.get('parking', 'N/A')}
- Furnished: {listing.get('furnished', 'N/A')}
- Lease Terms: {listing.get('lease_terms', 'N/A')}
- Date Posted: {listing.get('date_posted', 'N/A')}
- Description: {listing.get('description', 'N/A')[:1000]}

## Instructions
Score each criterion on a scale of 1–10 (10 = perfect match, 1 = very poor match).
Return ONLY valid JSON in this exact format:

{{
  "criteria_scores": {{
    "<criterion_id>": {{
      "score": <1-10>,
      "reasoning": "<brief explanation>"
    }}
  }},
  "overall_summary": "<2-3 sentence summary written directly to {display_name} by name, referencing specific things they care about from their vibe and criteria. Be personal and specific, not generic.>",
  "red_flags": ["<flag1>", "<flag2>"],
  "highlights": ["<highlight1>", "<highlight2>"]
}}

Do not include any text outside the JSON object."""
    return prompt


def _calculate_weighted_score(
    criteria: list[dict], criteria_scores: dict
) -> float:
    """Compute a weighted average score from individual criterion scores."""
    total_weight = 0.0
    weighted_sum = 0.0

    for c in criteria:
        cid = str(c.get("id", ""))
        weight = float(c.get("weight", 1))
        score_entry = criteria_scores.get(cid)
        if score_entry and isinstance(score_entry, dict):
            raw_score = score_entry.get("score")
            if raw_score is not None:
                try:
                    weighted_sum += float(raw_score) * weight
                    total_weight += weight
                except (TypeError, ValueError):
                    pass

    if total_weight == 0:
        return _FALLBACK_SCORE
    return round(weighted_sum / total_weight, 2)


async def score_listing(
    listing: dict, criteria: list[dict], user: dict
) -> dict:
    """Score a listing against a user's criteria using Claude.

    Returns a score dict ready to be saved to the DB.
    Falls back to a score of 5.0 if Claude returns an error or invalid JSON.
    """
    listing_id = listing.get("id", "")
    user_id = user.get("id", "")

    prompt = _build_prompt(listing, criteria, user)

    criteria_scores: dict = {}
    overall_summary: str = _FALLBACK_REASONING
    red_flags: list[str] = []
    highlights: list[str] = []
    overall_score: float = _FALLBACK_SCORE
    parse_error: bool = False

    try:
        message = _client.messages.create(
            model=_MODEL,
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw_text = message.content[0].text.strip()

        # Strip markdown code fences if present.
        if raw_text.startswith("```"):
            raw_text = re.sub(r"^```[a-z]*\n?", "", raw_text)
            raw_text = re.sub(r"\n?```$", "", raw_text)

        parsed = json.loads(raw_text)

        criteria_scores = parsed.get("criteria_scores", {})
        overall_summary = parsed.get("overall_summary", "")
        red_flags = parsed.get("red_flags", [])
        highlights = parsed.get("highlights", [])
        overall_score = _calculate_weighted_score(criteria, criteria_scores)

    except json.JSONDecodeError as exc:
        parse_error = True
        print(
            f"[Eden Scorer] JSON parse error for listing {listing_id} "
            f"/ user {user_id}: {exc}"
        )
        overall_summary = _FALLBACK_REASONING
        overall_score = _FALLBACK_SCORE

    except anthropic.APIError as exc:
        parse_error = True
        print(
            f"[Eden Scorer] Anthropic API error for listing {listing_id} "
            f"/ user {user_id}: {exc}"
        )
        overall_summary = _FALLBACK_REASONING
        overall_score = _FALLBACK_SCORE

    except Exception as exc:
        parse_error = True
        print(
            f"[Eden Scorer] Unexpected error scoring listing {listing_id} "
            f"/ user {user_id}: {exc}"
        )
        overall_summary = _FALLBACK_REASONING
        overall_score = _FALLBACK_SCORE

    threshold = float(user.get("score_threshold") or user.get("threshold") or 7.0)

    return {
        "user_id": user_id,
        "listing_id": listing_id,
        "overall_score": overall_score,
        "criteria_scores": criteria_scores,
        "claude_reasoning": overall_summary,
        "red_flags": red_flags,
        "highlights": highlights,
        "above_threshold": overall_score >= threshold,
    }
