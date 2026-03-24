from __future__ import annotations

import json
import logging
import re
from typing import Optional

import anthropic

from config import ANTHROPIC_API_KEY

logger = logging.getLogger(__name__)

_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
_MODEL = "claude-opus-4-5"


def _build_household_prompt(
    listing: dict,
    members: list[dict],  # [{"user": {...}, "criteria": [...]}]
) -> str:
    member_blocks = []
    for m in members:
        user = m["user"]
        criteria = m["criteria"]
        name = user.get("display_name") or user.get("email", "").split("@")[0].title()
        listing_type = user.get("listing_type", "for_rent")
        is_rental = listing_type == "for_rent"

        vibe_text = user.get("vibe_text", "")
        criteria_lines = "\n".join([
            f"  - {c['name']} (weight {c['weight']:.0%}): {c.get('description', '')}"
            for c in criteria
        ])
        member_blocks.append(
            f"### {name}\n"
            f"- Looking to: {'rent' if is_rental else 'buy'}\n"
            f"- Max {'rent' if is_rental else 'price'}: {user.get('max_rent') or 'no cap'}\n"
            f"- Pet required: {user.get('pet_required', False)}\n"
            f"- In their own words: {vibe_text if vibe_text else '(no vibe set)'}\n"
            f"- Criteria:\n{criteria_lines}"
        )

    members_section = "\n\n".join(member_blocks)

    amenities = listing.get("amenities") or []
    amenities_str = ", ".join(amenities) if amenities else "N/A"

    return f"""You are Eden's household scoring engine — a personal real estate advisor who knows both people intimately.

## The Household
{members_section}

## Listing
- Address: {listing.get('address', 'N/A')}
- Neighborhood: {listing.get('neighborhood', 'N/A')}
- City: {listing.get('city', 'N/A')}
- Price: ${listing.get('rent', 'N/A')}
- Bedrooms: {listing.get('bedrooms', 'N/A')} | Bathrooms: {listing.get('bathrooms', 'N/A')}
- Sqft: {listing.get('sqft', 'N/A')}
- Pet Policy: {listing.get('pet_policy', 'N/A')}
- Amenities: {amenities_str}
- Laundry: {listing.get('laundry', 'N/A')}
- Parking: {listing.get('parking', 'N/A')}
- Lease Terms: {listing.get('lease_terms', 'N/A')}
- Description: {str(listing.get('description', 'N/A'))[:800]}

## Instructions
Score this listing for EACH household member against their individual criteria (0-10 per criterion).
Then write a household narrative in the voice of a brilliant personal real estate advisor who knows both people.
The narrative should:
- Call each person by name
- Reference their vibe text to make commentary personal and specific (e.g. if someone mentions loving gardens, call that out)
- Note who this listing naturally appeals to more on the surface
- Surface specific details that will win over the person who might not immediately love it
- Flag any dealbreakers for either person
- Be warm, direct, and specific — never generic

Return ONLY valid JSON:

{{
  "members": {{
    "<user_id>": {{
      "overall_score": <float 0-10>,
      "criteria_scores": {{
        "<criterion_id>": {{
          "score": <float 0-10>,
          "reasoning": "<1 sentence>"
        }}
      }},
      "red_flags": ["<flag>"],
      "highlights": ["<highlight>"]
    }}
  }},
  "household_score": <weighted average of member overall scores, float 0-10>,
  "household_narrative": "<3-4 sentence personal narrative calling each person by name>",
  "compromise_rating": <float 0-10, how well this works for BOTH people simultaneously>
}}"""


def _parse_json(text: str) -> Optional[dict]:
    text = text.strip()
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass
    return None


async def score_listing_for_household(
    listing: dict,
    household_id: str,
    members: list[dict],  # [{"user": {...}, "criteria": [...]}]
) -> Optional[dict]:
    """Score a listing for a household and return blended + per-member scores."""

    if not members:
        return None

    prompt = _build_household_prompt(listing, members)

    try:
        response = _client.messages.create(
            model=_MODEL,
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
            system="You are Eden's household scoring engine. Be precise, personal, and return only valid JSON.",
        )
        result = _parse_json(response.content[0].text)
        if not result:
            logger.error("Failed to parse household score response")
            return None

        return {
            "household_id": household_id,
            "listing_id": listing["id"],
            "household_score": result.get("household_score", 5.0),
            "compromise_rating": result.get("compromise_rating", 5.0),
            "household_narrative": result.get("household_narrative", ""),
            "member_scores": result.get("members", {}),
        }

    except Exception as e:
        logger.error(f"Household scoring failed: {e}")
        return None
