#!/usr/bin/env python3
"""
Hyderabad Flat Hunter — Automated rental listing scanner.
Searches NoBroker (via API + HTML fallback), 99acres, MagicBricks,
Housing.com, SquareYards.
Sends verified, bachelor-friendly listings to Telegram with photos.
"""

import base64
import json
import os
import re
import sys
import time
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

STATE_FILE = Path(__file__).parent / "state.json"

AREAS = {
    "kondapur":  {"lat": 17.4633, "lng": 78.3564},
    "gachibowli": {"lat": 17.4401, "lng": 78.3489},
    "kokapet":   {"lat": 17.3948, "lng": 78.3319},
}

BUDGET = {"2bhk": 40000, "3bhk": 65000}
MIN_FLOOR = 3
IST = timezone(timedelta(hours=5, minutes=30))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

REJECT_KEYWORDS = [
    "inactive", "sold out", "not available", "expired",
    "deactivated", "this property is no longer available",
    "this listing has expired", "property has been rented",
]

session = requests.Session()
session.headers.update(HEADERS)

# ─── State Management ─────────────────────────────────────────────────────────

def load_state():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"sent": [], "rejected": [], "last_run": None}


def save_state(state):
    state["last_run"] = datetime.now(IST).isoformat()
    STATE_FILE.write_text(json.dumps(state, indent=2, ensure_ascii=False))


def is_already_processed(state, listing_id):
    all_ids = set()
    for item in state.get("sent", []):
        all_ids.add(item.get("id"))
    for item in state.get("rejected", []):
        all_ids.add(item.get("id"))
    return listing_id in all_ids


# ─── Telegram ─────────────────────────────────────────────────────────────────

def tg(method, payload):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/{method}"
    try:
        resp = session.post(url, json=payload, timeout=15)
        result = resp.json()
        if not result.get("ok"):
            print(f"  [Telegram] {method} failed: {result.get('description', 'unknown')}")
        return result.get("ok", False)
    except Exception as e:
        print(f"  [Telegram] Error: {e}")
        return False


def tg_send_message(text):
    return tg("sendMessage", {
        "chat_id": TELEGRAM_CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
        "disable_web_page_preview": True,
    })


def tg_send_photo(photo_url, caption):
    return tg("sendPhoto", {
        "chat_id": TELEGRAM_CHAT_ID,
        "photo": photo_url,
        "parse_mode": "HTML",
        "caption": caption[:1024],
    })


def tg_send_media_group(images, caption):
    if not images:
        return tg_send_message(caption)
    if len(images) < 2:
        return tg_send_photo(images[0], caption)

    media = [{"type": "photo", "media": images[0],
              "caption": caption[:1024], "parse_mode": "HTML"}]
    for img in images[1:5]:
        media.append({"type": "photo", "media": img})

    ok = tg("sendMediaGroup", {"chat_id": TELEGRAM_CHAT_ID, "media": media})
    if not ok:
        # Fallback: single photo
        ok = tg_send_photo(images[0], caption)
        if not ok:
            # Fallback: text only
            return tg_send_message(caption)
    return ok


# ─── Scoring ──────────────────────────────────────────────────────────────────

def score_listing(listing):
    score = 50

    locality = listing.get("locality", "").lower()
    if "kondapur" in locality:
        score += 15
    elif "gachibowli" in locality:
        score += 12
    elif "kokapet" in locality:
        score += 8

    rent = listing.get("rent", 0)
    bhk = listing.get("bhk", "3bhk")
    max_budget = BUDGET.get(bhk, 65000)
    if 0 < rent <= max_budget * 0.75:
        score += 10
    elif 0 < rent <= max_budget * 0.9:
        score += 5

    floor = listing.get("floor", 0)
    if floor >= 15:
        score += 12
    elif floor >= 10:
        score += 8
    elif floor >= MIN_FLOOR:
        score += 4

    if listing.get("bachelor_verified"):
        score += 10

    if listing.get("gated"):
        score += 5

    furn = listing.get("furnishing", "").lower()
    if "fully" in furn:
        score += 5
    elif "semi" in furn:
        score += 2

    if len(listing.get("images", [])) >= 3:
        score += 3

    return min(score, 100)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def safe_int(val, default=0):
    """Convert various types to int safely."""
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        cleaned = re.sub(r'[^\d]', '', val)
        return int(cleaned) if cleaned else default
    return default


def extract_images_from_photos(photos, max_count=5):
    """Extract image URLs from NoBroker-style photo arrays."""
    images = []
    if not isinstance(photos, list):
        return images
    for photo in photos[:max_count]:
        url = None
        if isinstance(photo, str):
            url = photo
        elif isinstance(photo, dict):
            # NoBroker photo structure: {imagesMap: {large: ["url"]}}
            img_map = photo.get("imagesMap", {})
            for size in ["large", "medium", "original", "thumbnail"]:
                val = img_map.get(size)
                if isinstance(val, list) and val:
                    url = val[0]
                    break
                elif isinstance(val, str) and val:
                    url = val
                    break
            if not url:
                url = photo.get("url") or photo.get("photoUrl") or photo.get("src")
        if url and url.startswith("http"):
            images.append(url)
    return images


# ─── Debug helpers ────────────────────────────────────────────────────────────

def _find_property_list(data, depth=0):
    """Recursively search a dict/list for arrays of property-like dicts."""
    if depth > 5:
        return None
    if isinstance(data, list) and len(data) > 0:
        # Check if items look like property listings
        if isinstance(data[0], dict) and any(
            k in data[0] for k in ["propertyId", "id", "rent", "price", "type"]
        ):
            return data
    if isinstance(data, dict):
        # Check known keys first
        for key in ["cardData", "data", "results", "properties", "listings",
                     "searchResults", "propertyList", "list"]:
            if key in data:
                result = _find_property_list(data[key], depth + 1)
                if result:
                    return result
        # Then check all keys
        for key, val in data.items():
            if isinstance(val, (dict, list)):
                result = _find_property_list(val, depth + 1)
                if result:
                    return result
    return None


def _debug_dict_structure(data, prefix="", depth=0):
    """Print dict/list structure for debugging (shallow)."""
    if depth > 3:
        return
    if isinstance(data, dict):
        for k, v in list(data.items())[:10]:
            if isinstance(v, list):
                item_type = type(v[0]).__name__ if v else "empty"
                print(f"  [DEBUG] {prefix}.{k}: list[{len(v)}] of {item_type}")
                if v and isinstance(v[0], dict):
                    print(f"  [DEBUG]   first item keys: {list(v[0].keys())[:8]}")
            elif isinstance(v, dict):
                print(f"  [DEBUG] {prefix}.{k}: dict keys={list(v.keys())[:6]}")
                _debug_dict_structure(v, f"{prefix}.{k}", depth + 1)
            elif isinstance(v, str) and len(v) > 100:
                print(f"  [DEBUG] {prefix}.{k}: str[{len(v)}]")
            else:
                print(f"  [DEBUG] {prefix}.{k}: {type(v).__name__} = {str(v)[:80]}")


# ═══════════════════════════════════════════════════════════════════════════════
# NoBroker — Primary source (API + HTML fallback)
# ═══════════════════════════════════════════════════════════════════════════════

def search_nobroker(area_name, bhk="3bhk"):
    """
    Try multiple approaches to get NoBroker listings:
    1. Internal JSON API
    2. __NEXT_DATA__ from search page
    3. HTML link extraction (needs verification)
    """
    # Approach 1: API
    listings = _nobroker_api(area_name, bhk)
    if listings:
        return listings

    # Approach 2: HTML with __NEXT_DATA__ parsing
    listings = _nobroker_html(area_name, bhk)
    return listings


def _nobroker_api(area_name, bhk):
    """Try NoBroker's internal API for JSON data."""
    listings = []
    coords = AREAS.get(area_name, {})
    if not coords:
        return listings

    bhk_api = "BHK2" if bhk == "2bhk" else "BHK3"
    max_rent = BUDGET.get(bhk, 65000)

    search_data = [
        {"field_name": "latitude", "value": coords["lat"], "comparison": "equals"},
        {"field_name": "longitude", "value": coords["lng"], "comparison": "equals"},
    ]
    search_param = base64.b64encode(json.dumps(search_data).encode()).decode()

    # Try multiple API endpoints
    api_endpoints = [
        {
            "url": "https://www.nobroker.in/api/v1/property/filter/region/rent/hyderabad",
            "params": {
                "pageNo": 1,
                "searchParam": search_param,
                "type": bhk_api,
                "budget": f"0,{max_rent}",
                "sharedAccomodation": 0,
                "radius": 2.0,
            },
        },
        {
            "url": f"https://www.nobroker.in/api/v3/multi/property/RENT/filter",
            "params": {
                "city": "hyderabad",
                "locality": area_name,
                "type": bhk_api,
                "budget": f",{max_rent}",
                "pageNo": 1,
                "sharedAccomodation": 0,
            },
        },
    ]

    print(f"  [NoBroker API] Fetching {bhk} in {area_name}...")

    # First, visit the main page to get cookies
    try:
        session.get("https://www.nobroker.in/", timeout=10)
    except Exception:
        pass

    for endpoint in api_endpoints:
        try:
            resp = session.get(
                endpoint["url"],
                params=endpoint["params"],
                timeout=20,
                headers={
                    **HEADERS,
                    "Accept": "application/json, text/plain, */*",
                    "Referer": f"https://www.nobroker.in/{bhk}-flats-for-rent-in-{area_name}_hyderabad",
                    "X-Requested-With": "XMLHttpRequest",
                },
            )

            ct = resp.headers.get("Content-Type", "")
            print(f"  [NoBroker API] {endpoint['url'].split('/')[-1]} → HTTP {resp.status_code}, CT: {ct[:40]}")
            print(f"  [NoBroker API] Body preview: {resp.text[:200]}")

            if resp.status_code != 200:
                continue

            if "json" not in ct and "javascript" not in ct:
                # Might be HTML (bot block page)
                continue

            data = resp.json()

            # Debug: print response structure
            if isinstance(data, dict):
                print(f"  [NoBroker API] Response keys: {list(data.keys())[:10]}")
                for k, v in data.items():
                    if isinstance(v, list):
                        print(f"  [NoBroker API]   {k}: list[{len(v)}]" +
                              (f" first keys: {list(v[0].keys())[:6]}" if v and isinstance(v[0], dict) else ""))
                    elif isinstance(v, dict):
                        print(f"  [NoBroker API]   {k}: dict keys={list(v.keys())[:6]}")

            # Navigate to property list
            properties = []
            if isinstance(data, dict):
                for key in ["data", "cardData", "results", "properties", "otherParams"]:
                    val = data.get(key)
                    if isinstance(val, list) and val:
                        properties = val
                        break
                    elif isinstance(val, dict):
                        for sub_key in ["cardData", "data", "results"]:
                            sub_val = val.get(sub_key)
                            if isinstance(sub_val, list) and sub_val:
                                properties = sub_val
                                break
                        if properties:
                            break
            elif isinstance(data, list):
                properties = data

            if properties:
                print(f"  [NoBroker API] Found {len(properties)} properties to parse")
                for prop in properties:
                    listing = _parse_nobroker_property(prop, area_name, bhk)
                    if listing:
                        listings.append(listing)

                if listings:
                    print(f"  [NoBroker API] Got {len(listings)} qualified in {area_name}")
                    return listings[:15]

        except requests.exceptions.JSONDecodeError:
            print(f"  [NoBroker API] Non-JSON response")
        except Exception as e:
            print(f"  [NoBroker API] Error: {e}")

    return listings[:15]


def _nobroker_html(area_name, bhk):
    """Scrape NoBroker search page — try __NEXT_DATA__ first, then links."""
    listings = []
    url = f"https://www.nobroker.in/{bhk}-flats-for-rent-in-{area_name}_hyderabad"
    print(f"  [NoBroker HTML] Fetching {bhk} in {area_name}...")

    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            print(f"  [NoBroker HTML] HTTP {resp.status_code}")
            return listings

        html_text = resp.text
        soup = BeautifulSoup(html_text, "html.parser")
        print(f"  [NoBroker HTML] Page size: {len(html_text)} bytes")

        # ── Try __NEXT_DATA__ (Next.js server data) ──
        next_tag = soup.find("script", id="__NEXT_DATA__")
        if next_tag and next_tag.string:
            try:
                nd = json.loads(next_tag.string)
                page_props = nd.get("props", {}).get("pageProps", {})
                print(f"  [NoBroker HTML] __NEXT_DATA__ pageProps keys: {list(page_props.keys())[:15]}")

                # Deep-search for any list of property-like dicts
                cards = _find_property_list(page_props)

                if cards:
                    for prop in cards:
                        listing = _parse_nobroker_property(prop, area_name, bhk)
                        if listing:
                            listings.append(listing)
                    print(f"  [NoBroker HTML] Parsed {len(listings)} from __NEXT_DATA__")
                    if listings:
                        return listings[:15]
                else:
                    print(f"  [NoBroker HTML] __NEXT_DATA__ found but no property arrays")
                    # Dump structure for debugging
                    _debug_dict_structure(page_props, "pageProps", depth=3)

            except (json.JSONDecodeError, KeyError, TypeError) as e:
                print(f"  [NoBroker HTML] __NEXT_DATA__ parse error: {e}")
        else:
            print(f"  [NoBroker HTML] No __NEXT_DATA__ tag found")

        # ── Try embedded JSON in any script tag ──
        for script in soup.find_all("script"):
            text = script.string or ""
            if '"propertyId"' not in text:
                continue
            # Try to extract property objects
            try:
                # Find array-like JSON containing property objects
                matches = re.findall(
                    r'\{[^{}]*"propertyId"\s*:\s*"([a-f0-9]{20,})"[^{}]*"rent"\s*:\s*(\d+)[^{}]*\}',
                    text
                )
                for prop_id, rent_str in matches:
                    rent = int(rent_str)
                    max_rent = BUDGET.get(bhk, 65000)
                    if rent > max_rent or rent < 5000:
                        continue
                    lid = f"nb_{prop_id[:20]}"
                    if not any(l["id"] == lid for l in listings):
                        detail_url = (
                            f"https://www.nobroker.in/property/"
                            f"{bhk[0]}-bhk-apartment-for-rent-in-{area_name}"
                            f"-hyderabad-for-rs-{rent}/{prop_id}/detail"
                        )
                        listings.append({
                            "id": lid,
                            "url": detail_url,
                            "rent": rent,
                            "source": "NoBroker",
                            "bhk": bhk,
                            "locality": area_name.title(),
                            "project": "Unknown",
                            "sqft": 0,
                            "floor": 0,
                            "furnishing": "Unknown",
                            "bachelor_verified": False,
                            "images": [],
                            "deposit": 0,
                            "gated": False,
                            "active": True,
                            "needs_verification": True,
                        })
            except Exception:
                pass

        # ── Fallback: extract links + parse card context ──
        if not listings:
            links = soup.find_all("a", href=re.compile(r"/property/.*?/detail"))

            # Debug: print HTML context around first link
            if links:
                first_link = links[0]
                parent = first_link.parent
                if parent:
                    # Go up to find the card container
                    for _ in range(5):
                        if parent.parent and parent.parent.name not in ["body", "html", "[document]"]:
                            parent = parent.parent
                        else:
                            break
                    card_text = parent.get_text(" ", strip=True)[:300]
                    print(f"  [NoBroker HTML] Card context: {card_text}")

            for link in links:
                href = link.get("href", "")
                pid_match = re.search(r"/([a-f0-9]{20,})/detail", href)
                if not pid_match:
                    continue

                pid = pid_match.group(1)
                lid = f"nb_{pid[:20]}"
                if any(l["id"] == lid for l in listings):
                    continue

                full_url = urljoin("https://www.nobroker.in", href)

                # Try to extract data from the link's card context
                card_data = _extract_card_data(link, bhk, area_name)

                listings.append({
                    "id": lid,
                    "url": full_url,
                    "source": "NoBroker",
                    "bhk": bhk,
                    "locality": area_name.title(),
                    "needs_verification": card_data.get("rent", 0) == 0,
                    **card_data,
                })

            # Also extract from script tag propertyId patterns
            for script in soup.find_all("script"):
                text = script.string or ""
                pids = re.findall(r'"propertyId"\s*:\s*"([a-f0-9]{20,})"', text)
                for pid in pids:
                    lid = f"nb_{pid[:20]}"
                    if not any(l["id"] == lid for l in listings):
                        detail_url = (
                            f"https://www.nobroker.in/property/"
                            f"{bhk[0]}-bhk-apartment-for-rent-in-{area_name}"
                            f"-hyderabad/{pid}/detail"
                        )
                        listings.append({
                            "id": lid,
                            "url": detail_url,
                            "source": "NoBroker",
                            "bhk": bhk,
                            "locality": area_name.title(),
                            "needs_verification": True,
                            "project": "Unknown",
                            "rent": 0,
                            "sqft": 0,
                            "floor": 0,
                            "furnishing": "Unknown",
                            "bachelor_verified": False,
                            "images": [],
                            "deposit": 0,
                            "gated": False,
                            "active": True,
                        })

        print(f"  [NoBroker HTML] Found {len(listings)} listings in {area_name}")

    except Exception as e:
        print(f"  [NoBroker HTML] Error: {e}")
        traceback.print_exc()

    return listings[:15]


def _extract_card_data(link_tag, bhk, area_name):
    """Try to extract listing data from the HTML card surrounding a link."""
    data = {
        "project": "Unknown",
        "rent": 0,
        "sqft": 0,
        "floor": 0,
        "furnishing": "Unknown",
        "bachelor_verified": False,
        "images": [],
        "deposit": 0,
        "gated": False,
        "active": True,
    }

    # Walk up to find card container (usually 3-5 levels up)
    card = link_tag
    for _ in range(6):
        if card.parent and card.parent.name not in ["body", "html", "[document]"]:
            card = card.parent
        else:
            break

    card_text = card.get_text(" ", strip=True)
    card_html = str(card)

    # Extract rent (look for ₹ or numeric values near "rent")
    rent_match = re.search(r'₹\s*([\d,]+)', card_text)
    if not rent_match:
        rent_match = re.search(r'([\d,]+)\s*/\s*month', card_text, re.I)
    if not rent_match:
        rent_match = re.search(r'(?:rent|price)\s*:?\s*₹?\s*([\d,]+)', card_text, re.I)
    if rent_match:
        data["rent"] = int(rent_match.group(1).replace(",", ""))

    # Extract sqft
    sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', card_text, re.I)
    if sqft_match:
        data["sqft"] = int(sqft_match.group(1).replace(",", ""))

    # Extract floor
    floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of\s*\d+)', card_text, re.I)
    if floor_match:
        data["floor"] = int(floor_match.group(1))

    # Check bachelor
    if re.search(r'bachelor|anyone|all\s*tenant', card_text, re.I):
        data["bachelor_verified"] = True

    # Check furnishing
    if "fully furnished" in card_text.lower():
        data["furnishing"] = "Fully Furnished"
    elif "semi" in card_text.lower() and "furnished" in card_text.lower():
        data["furnishing"] = "Semi-Furnished"
    elif "unfurnished" in card_text.lower():
        data["furnishing"] = "Unfurnished"

    # Extract images from card
    for img in card.find_all("img"):
        src = img.get("src", "") or img.get("data-src", "")
        if src and src.startswith("http") and "logo" not in src.lower():
            data["images"].append(src)

    # Extract project name from URL pattern or card text
    href = link_tag.get("href", "")
    name_match = re.search(r'in-([a-z-]+(?:-[a-z]+)*)-hyderabad', href)
    if name_match:
        raw = name_match.group(1).replace("-", " ").title()
        # Clean up "Kondapur" etc from project name
        for area in AREAS:
            raw = raw.replace(area.title(), "").strip()
        if raw and len(raw) > 3:
            data["project"] = raw

    return data


def _parse_nobroker_property(prop, area_name, bhk):
    """Parse a single NoBroker property dict (from API or __NEXT_DATA__)."""
    if not isinstance(prop, dict):
        return None

    prop_id = prop.get("propertyId", prop.get("id", ""))
    if not prop_id:
        return None

    rent = safe_int(prop.get("rent", 0))
    max_rent = BUDGET.get(bhk, 65000)
    if rent > max_rent or rent < 3000:
        return None

    # Tenant preference
    tenant = str(prop.get("tenantPreference", prop.get("leasetype", ""))).lower()
    bachelor_ok = any(kw in tenant for kw in ["bachelor", "anyone", "all", "single"])
    family_only = "family" in tenant and not bachelor_ok
    if family_only:
        return None

    # Floor
    floor = safe_int(prop.get("floor", prop.get("floorNo", 0)))

    # Images
    images = extract_images_from_photos(prop.get("photos", prop.get("images", [])))

    # OG image fallback
    photo_url = prop.get("photoUrl", prop.get("thumbnailImage", ""))
    if photo_url and photo_url.startswith("http") and photo_url not in images:
        images.insert(0, photo_url)

    # Project name
    project = prop.get("society", prop.get("title", prop.get("buildingName", "Unknown")))
    if isinstance(project, dict):
        project = project.get("name", "Unknown")
    if not project or project == "null":
        project = "Unknown"

    # Sqft
    sqft = safe_int(prop.get("propertySize", prop.get("carpet_area",
                    prop.get("builtUpArea", prop.get("superBuiltupArea", 0)))))

    # Furnishing
    furnishing = str(prop.get("furnishing", prop.get("furnishingType", "Unfurnished")))

    # Deposit
    deposit = safe_int(prop.get("deposit", 0))

    # Gated community
    prop_type = str(prop.get("type", prop.get("propertyType", ""))).lower()
    gated = any(kw in prop_type for kw in ["apartment", "gated"])

    # Build detail URL
    detail_url = (
        f"https://www.nobroker.in/property/"
        f"{bhk[0]}-bhk-apartment-for-rent-in-{area_name}"
        f"-hyderabad-for-rs-{rent}/{prop_id}/detail"
    )

    return {
        "id": f"nb_{str(prop_id)[:20]}",
        "url": detail_url,
        "rent": rent,
        "sqft": sqft,
        "floor": floor,
        "furnishing": furnishing,
        "bachelor_verified": bachelor_ok,
        "project": project,
        "locality": area_name.title(),
        "images": images[:5],
        "deposit": deposit,
        "source": "NoBroker",
        "bhk": bhk,
        "gated": gated,
        "active": True,
        "needs_verification": False,  # complete data from API/JSON
    }


def verify_nobroker(listing):
    """Verify a NoBroker listing by fetching its detail page."""
    url = listing["url"]
    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"    [verify] HTTP {resp.status_code}")
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS:
            if kw in text:
                print(f"    [verify] Rejected: found '{kw}'")
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        rent = safe_int(re.search(r'(?:rent|₹|rs\.?)\s*:?\s*([\d,]+)', page_text, re.I))
        if isinstance(rent, re.Match):
            rent = safe_int(rent.group(1))

        rent_match = re.search(r'(?:rent|₹|rs\.?)\s*:?\s*([\d,]+)', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(sqft_match.group(1).replace(",", "")) if sqft_match else 0

        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of\s*\d+)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        bachelor_ok = bool(re.search(r'bachelor|anyone|single\s*men', page_text, re.I))
        if not bachelor_ok and re.search(r'lease\s*type\s*:?\s*family', page_text, re.I):
            return None

        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text or "semi furnished" in text:
            furnishing = "Semi-Furnished"

        images = []
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            images.append(og_img["content"])
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if "nobroker" in src and ("large" in src or "original" in src):
                if src not in images:
                    images.append(src)

        locality = listing.get("locality", "")
        for a in AREAS:
            if a in page_text.lower():
                locality = a.title()
                break

        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        project_match = re.search(r'in\s+(.+?)(?:,|\s+for\s+)', title, re.I)
        project = project_match.group(1).strip() if project_match else listing.get("project", "Unknown")

        return {
            **listing,
            "rent": rent if rent > 0 else listing.get("rent", 0),
            "sqft": sqft if sqft > 0 else listing.get("sqft", 0),
            "floor": floor if floor > 0 else listing.get("floor", 0),
            "furnishing": furnishing,
            "bachelor_verified": bachelor_ok,
            "project": project,
            "locality": locality,
            "images": images[:5] if images else listing.get("images", []),
            "active": True,
            "needs_verification": False,
        }

    except Exception as e:
        print(f"    [verify] Error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 99acres
# ═══════════════════════════════════════════════════════════════════════════════

def search_99acres(area_name, bhk="3bhk"):
    listings = []
    bhk_slug = bhk.replace("bhk", "-bhk")
    url = f"https://www.99acres.com/{bhk_slug}-flats-for-rent-in-{area_name}-hyderabad-ffid"
    print(f"  [99acres] Fetching {bhk} in {area_name}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [99acres] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        # Try __NEXT_DATA__ first
        next_tag = soup.find("script", id="__NEXT_DATA__")
        if next_tag and next_tag.string:
            try:
                nd = json.loads(next_tag.string)
                # 99acres structure varies; try common paths
                page_props = nd.get("props", {}).get("pageProps", {})
                cards = (
                    page_props.get("searchData", {}).get("results", [])
                    or page_props.get("listings", [])
                    or page_props.get("data", [])
                )
                for prop in cards:
                    if not isinstance(prop, dict):
                        continue
                    prop_id = str(prop.get("id", prop.get("propertyId", "")))
                    if not prop_id:
                        continue

                    rent = safe_int(prop.get("rent", prop.get("price", 0)))
                    max_rent = BUDGET.get(bhk, 65000)
                    if rent > max_rent or rent < 3000:
                        continue

                    prop_url = prop.get("url", prop.get("propertyUrl", ""))
                    if prop_url and not prop_url.startswith("http"):
                        prop_url = urljoin("https://www.99acres.com", prop_url)

                    images = []
                    for img in prop.get("images", prop.get("photos", []))[:5]:
                        if isinstance(img, dict):
                            images.append(img.get("url", img.get("src", "")))
                        elif isinstance(img, str):
                            images.append(img)
                    images = [i for i in images if i.startswith("http")]

                    listings.append({
                        "id": f"99a_{prop_id}",
                        "url": prop_url or url,
                        "rent": rent,
                        "sqft": safe_int(prop.get("area", prop.get("builtupArea", 0))),
                        "floor": safe_int(prop.get("floor", 0)),
                        "furnishing": str(prop.get("furnishing", "Unknown")),
                        "bachelor_verified": False,
                        "project": str(prop.get("society", prop.get("projectName", "Unknown"))),
                        "locality": area_name.title(),
                        "images": images,
                        "deposit": safe_int(prop.get("securityDeposit", 0)),
                        "source": "99acres",
                        "bhk": bhk,
                        "gated": False,
                        "active": True,
                        "needs_verification": False,
                    })

                if listings:
                    print(f"  [99acres] Parsed {len(listings)} from __NEXT_DATA__")
                    return listings[:10]

            except (json.JSONDecodeError, KeyError, TypeError):
                pass

        # Fallback: extract links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            match = re.search(r'/(\d{8,})', href)
            if match and "rent" in href.lower():
                full_url = urljoin("https://www.99acres.com", href)
                lid = f"99a_{match.group(1)}"
                if not any(l["id"] == lid for l in listings):
                    listings.append({
                        "id": lid,
                        "url": full_url,
                        "source": "99acres",
                        "bhk": bhk,
                        "locality": area_name.title(),
                        "needs_verification": True,
                        "project": "Unknown",
                        "rent": 0, "sqft": 0, "floor": 0,
                        "furnishing": "Unknown",
                        "bachelor_verified": False,
                        "images": [], "deposit": 0,
                        "gated": False, "active": True,
                    })

    except Exception as e:
        print(f"  [99acres] Error: {e}")

    print(f"  [99acres] Found {len(listings)} in {area_name}")
    return listings[:10]


# ═══════════════════════════════════════════════════════════════════════════════
# MagicBricks
# ═══════════════════════════════════════════════════════════════════════════════

def search_magicbricks(area_name, bhk="3bhk"):
    listings = []
    bhk_slug = bhk.replace("bhk", "-bhk")
    url = f"https://www.magicbricks.com/{bhk_slug}-flats-for-rent-in-{area_name}-hyderabad-pppfs"
    print(f"  [MagicBricks] Fetching {bhk} in {area_name}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [MagicBricks] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        # Try JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and item.get("url"):
                            lid = f"mb_{hash(item['url']) % 10**10}"
                            listings.append({
                                "id": lid,
                                "url": item["url"],
                                "source": "MagicBricks",
                                "bhk": bhk,
                                "locality": area_name.title(),
                                "needs_verification": True,
                                "project": item.get("name", "Unknown"),
                                "rent": 0, "sqft": 0, "floor": 0,
                                "furnishing": "Unknown",
                                "bachelor_verified": False,
                                "images": [], "deposit": 0,
                                "gated": False, "active": True,
                            })
            except (json.JSONDecodeError, TypeError):
                pass

        # Extract links
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "propertyDetails" in href or re.search(r'/\d{10,}', href):
                id_match = re.search(r'(\d{10,})', href)
                if id_match:
                    full_url = urljoin("https://www.magicbricks.com", href)
                    lid = f"mb_{id_match.group(1)}"
                    if not any(l["id"] == lid for l in listings):
                        listings.append({
                            "id": lid,
                            "url": full_url,
                            "source": "MagicBricks",
                            "bhk": bhk,
                            "locality": area_name.title(),
                            "needs_verification": True,
                            "project": "Unknown",
                            "rent": 0, "sqft": 0, "floor": 0,
                            "furnishing": "Unknown",
                            "bachelor_verified": False,
                            "images": [], "deposit": 0,
                            "gated": False, "active": True,
                        })

    except Exception as e:
        print(f"  [MagicBricks] Error: {e}")

    print(f"  [MagicBricks] Found {len(listings)} in {area_name}")
    return listings[:10]


# ═══════════════════════════════════════════════════════════════════════════════
# Housing.com
# ═══════════════════════════════════════════════════════════════════════════════

def search_housing(area_name, bhk="3bhk"):
    listings = []
    bhk_slug = bhk.replace("bhk", "-bhk")
    url = f"https://housing.com/rent/{bhk_slug}-in-{area_name}-hyderabad"
    print(f"  [Housing.com] Fetching {bhk} in {area_name}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [Housing.com] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/rent/" in href and re.search(r'/(\d{8,})', href):
                id_match = re.search(r'/(\d{8,})', href)
                if id_match:
                    full_url = urljoin("https://housing.com", href)
                    lid = f"hc_{id_match.group(1)}"
                    if not any(l["id"] == lid for l in listings):
                        listings.append({
                            "id": lid,
                            "url": full_url,
                            "source": "Housing.com",
                            "bhk": bhk,
                            "locality": area_name.title(),
                            "needs_verification": True,
                            "project": "Unknown",
                            "rent": 0, "sqft": 0, "floor": 0,
                            "furnishing": "Unknown",
                            "bachelor_verified": False,
                            "images": [], "deposit": 0,
                            "gated": False, "active": True,
                        })

    except Exception as e:
        print(f"  [Housing.com] Error: {e}")

    print(f"  [Housing.com] Found {len(listings)} in {area_name}")
    return listings[:10]


# ═══════════════════════════════════════════════════════════════════════════════
# SquareYards
# ═══════════════════════════════════════════════════════════════════════════════

def search_squareyards(area_name, bhk="3bhk"):
    listings = []
    bhk_slug = bhk.replace("bhk", "-bhk")
    url = f"https://www.squareyards.com/rent/{bhk_slug}-for-rent-in-{area_name}-hyderabad"
    print(f"  [SquareYards] Fetching {bhk} in {area_name}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [SquareYards] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "rental-" in href and re.search(r'/(\d{6,})', href):
                id_match = re.search(r'/(\d{6,})', href)
                if id_match:
                    full_url = urljoin("https://www.squareyards.com", href)
                    lid = f"sy_{id_match.group(1)}"
                    if not any(l["id"] == lid for l in listings):
                        listings.append({
                            "id": lid,
                            "url": full_url,
                            "source": "SquareYards",
                            "bhk": bhk,
                            "locality": area_name.title(),
                            "needs_verification": True,
                            "project": "Unknown",
                            "rent": 0, "sqft": 0, "floor": 0,
                            "furnishing": "Unknown",
                            "bachelor_verified": False,
                            "images": [], "deposit": 0,
                            "gated": False, "active": True,
                        })

    except Exception as e:
        print(f"  [SquareYards] Error: {e}")

    print(f"  [SquareYards] Found {len(listings)} in {area_name}")
    return listings[:10]


# ═══════════════════════════════════════════════════════════════════════════════
# Generic detail-page verifier (for listings needing verification)
# ═══════════════════════════════════════════════════════════════════════════════

def verify_detail_page(listing):
    """
    Fetch detail page and extract data. Returns updated listing or None.
    Used for listings that only have a URL from HTML link extraction.
    """
    url = listing["url"]
    source = listing["source"]

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"    [verify {source}] HTTP {resp.status_code}")
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS:
            if kw in text:
                print(f"    [verify {source}] Rejected: '{kw}'")
                return None
        if source == "SquareYards":
            for kw in ["sold", "rented out", "currently unavailable"]:
                if kw in text:
                    print(f"    [verify {source}] Rejected: '{kw}'")
                    return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        # Extract rent
        rent_match = re.search(r'(?:rent|₹|rs\.?)\s*:?\s*([\d,]+)', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        # Budget check
        bhk = listing.get("bhk", "3bhk")
        max_rent = BUDGET.get(bhk, 65000)
        if rent > max_rent:
            return None
        if rent == 0:
            # Can't determine rent from page — skip (not permanently reject)
            return None

        # Sqft
        sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(sqft_match.group(1).replace(",", "")) if sqft_match else 0

        # Floor
        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of\s*\d+)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        # Bachelor check
        bachelor_ok = bool(re.search(r'bachelor|anyone|single\s*men|all\b', page_text, re.I))
        if not bachelor_ok and re.search(r'family\s*only|lease\s*type\s*:?\s*family', page_text, re.I):
            print(f"    [verify {source}] Rejected: family only")
            return None

        # Furnishing
        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text or "semi furnished" in text:
            furnishing = "Semi-Furnished"

        # Images
        images = []
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            images.append(og_img["content"])

        # Locality
        locality = listing.get("locality", "")
        for a in AREAS:
            if a in page_text.lower():
                locality = a.title()
                break

        # Project name
        title_tag = soup.find("title")
        project = title_tag.get_text(strip=True).split(" - ")[0].split("|")[0].strip() if title_tag else "Unknown"

        return {
            **listing,
            "rent": rent,
            "sqft": sqft,
            "floor": floor,
            "furnishing": furnishing,
            "bachelor_verified": bachelor_ok,
            "project": project,
            "locality": locality,
            "images": images[:5],
            "active": True,
            "needs_verification": False,
        }

    except Exception as e:
        print(f"    [verify {source}] Error: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# Main Pipeline
# ═══════════════════════════════════════════════════════════════════════════════

PLATFORMS = [
    ("NoBroker",    search_nobroker),
    ("99acres",     search_99acres),
    ("MagicBricks", search_magicbricks),
    ("Housing.com", search_housing),
    ("SquareYards", search_squareyards),
]


def run():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set.")
        sys.exit(1)

    now_str = datetime.now(IST).strftime("%d %b %Y %I:%M %p IST")
    print(f"\n{'='*60}")
    print(f"  Flat Hunter — {now_str}")
    print(f"{'='*60}\n")

    state = load_state()
    all_listings = []

    # ── Step 1: Search all platforms ──
    for platform_name, search_fn in PLATFORMS:
        print(f"\n[{platform_name}]")
        for area_name in AREAS:
            for bhk in ["3bhk", "2bhk"]:
                results = search_fn(area_name, bhk)
                all_listings.extend(results)
                time.sleep(1)

    print(f"\n{'─'*40}")
    print(f"Total raw listings: {len(all_listings)}")

    # ── Step 2: Deduplicate & remove processed ──
    seen_ids = set()
    unique = []
    for l in all_listings:
        lid = l["id"]
        if lid not in seen_ids and not is_already_processed(state, lid):
            seen_ids.add(lid)
            unique.append(l)
    print(f"After dedup & state filter: {len(unique)}")

    # ── Step 3: Separate complete vs needs-verification ──
    complete = [l for l in unique if not l.get("needs_verification")]
    needs_verify = [l for l in unique if l.get("needs_verification")]

    print(f"  Complete (from API/JSON): {len(complete)}")
    print(f"  Needs verification: {len(needs_verify)}")

    # ── Step 4: Verify incomplete listings (cap at 15 to stay within timeout) ──
    verified_from_pages = []
    for listing in needs_verify[:15]:
        print(f"  Verifying {listing['source']}: {listing['url'][:75]}...")
        result = verify_detail_page(listing)
        if result:
            verified_from_pages.append(result)
            print(f"    ✅ Verified — ₹{result['rent']}")
        else:
            # Don't permanently reject — verification might fail due to bot blocking
            # Only add to rejected if we got a clear signal (family only, etc.)
            print(f"    ⚠️ Could not verify (may be bot-blocked)")
        time.sleep(0.5)

    # Combine
    all_verified = complete + verified_from_pages
    print(f"\nTotal verified: {len(all_verified)}")

    # ── Step 5: Budget & basic filters ──
    qualified = []
    for listing in all_verified:
        rent = listing.get("rent", 0)
        bhk = listing.get("bhk", "3bhk")
        max_rent = BUDGET.get(bhk, 65000)

        if rent <= 0 or rent > max_rent:
            state["rejected"].append({"id": listing["id"], "reason": "budget"})
            continue

        listing["score"] = score_listing(listing)
        qualified.append(listing)

    # Sort by score
    qualified.sort(key=lambda x: x["score"], reverse=True)
    top_picks = qualified[:8]

    print(f"Qualified after scoring: {len(qualified)}")
    print(f"Sending top {len(top_picks)} to Telegram")

    # ── Step 6: Send to Telegram ──
    if top_picks:
        sources = sorted(set(p["source"] for p in top_picks))
        tg_send_message(
            f"🏠 <b>Flat Hunt Auto-Update</b>\n\n"
            f"📅 {now_str}\n"
            f"✅ <b>{len(top_picks)} new verified listings found</b>\n"
            f"📡 Sources: {', '.join(sources)}"
        )
        time.sleep(1)

        for i, listing in enumerate(top_picks, 1):
            caption = (
                f"#{i} <b>{listing['bhk'].upper()} in {listing.get('locality', 'Hyderabad')}"
                f" — ₹{listing['rent']:,}/mo — Score {listing['score']}/100</b>\n"
                f"📐 {listing.get('sqft', '?')} sq.ft | Floor {listing.get('floor', '?')}"
                f" | {listing.get('furnishing', '?')}\n"
                f"👤 Bachelor: {'✅ Yes' if listing.get('bachelor_verified') else '⚠️ Check'}\n"
                f"🏢 {listing.get('project', 'Unknown')}\n"
            )
            if listing.get("deposit"):
                caption += f"💰 Deposit: ₹{listing['deposit']:,}\n"
            caption += (
                f"📡 Source: {listing['source']}\n"
                f"🔗 {listing['url']}"
            )

            images = listing.get("images", [])
            tg_send_media_group(images, caption)

            state["sent"].append({
                "id": listing["id"],
                "project": listing.get("project", "Unknown"),
                "rent": listing["rent"],
                "bhk": listing["bhk"],
                "locality": listing.get("locality", ""),
                "source": listing["source"],
                "url": listing["url"],
                "sent_at": now_str,
            })
            time.sleep(1)

        tg_send_message(
            f"🏆 <b>Best pick:</b> {top_picks[0].get('project', '?')}, "
            f"{top_picks[0].get('locality', '')} — "
            f"₹{top_picks[0]['rent']:,}/mo (Score {top_picks[0]['score']})\n\n"
            f"⏰ Next scan in ~6 hours."
        )
    else:
        tg_send_message(
            f"🏠 <b>Flat Hunt Scan — {now_str}</b>\n\n"
            f"No new verified listings this round.\n"
            f"📡 Scanned: NoBroker, 99acres, MagicBricks, Housing.com, SquareYards\n"
            f"📍 Areas: Kondapur, Gachibowli, Kokapet\n"
            f"⏰ Will check again in ~6 hours."
        )

    # ── Step 7: Save state ──
    state["rejected"] = state["rejected"][-200:]
    save_state(state)
    print(f"\n✅ Done. {len(top_picks)} listings sent to Telegram.\n")


if __name__ == "__main__":
    run()
