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

# curl_cffi impersonates Chrome's TLS fingerprint to bypass Akamai bot detection
try:
    from curl_cffi import requests as curl_requests
    HAS_CURL_CFFI = True
except ImportError:
    HAS_CURL_CFFI = False
    print("[WARN] curl_cffi not available — Housing.com will likely be blocked")

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
MIN_IMAGES = 3  # minimum images required to share a listing
IST = timezone(timedelta(hours=5, minutes=30))

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

BROWSER_HEADERS = {
    **HEADERS,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
    "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"macOS"',
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


def _find_property_list(data, depth=0):
    """Recursively search a dict/list for arrays of property-like dicts."""
    if depth > 5:
        return None
    if isinstance(data, list) and len(data) > 0:
        if isinstance(data[0], dict) and any(
            k in data[0] for k in ["propertyId", "id", "rent", "price", "type",
                                    "listingId", "expectedRent", "listingPrice",
                                    "coverImage", "builtUpArea", "carpetArea"]
        ):
            return data
    if isinstance(data, dict):
        for key in ["cardData", "data", "results", "properties", "listings",
                     "searchResults", "propertyList", "list", "catalog",
                     "rentProperties", "rentalProperties", "hits", "items",
                     "propertyData", "cards", "entities"]:
            if key in data:
                result = _find_property_list(data[key], depth + 1)
                if result:
                    return result
        for key, val in data.items():
            if isinstance(val, (dict, list)):
                result = _find_property_list(val, depth + 1)
                if result:
                    return result
    return None


def _extract_all_images_from_html(soup_or_tag, source=""):
    """Extract all real property images from an HTML element.
    Handles lazy-loaded images (data-src, data-lazyimg, data-original, etc.)
    and background-image CSS.
    """
    images = []
    seen = set()

    # Known CDN domains per platform
    cdn_domains = {
        "NoBroker": ["assets.nobroker.in", "images.nobroker.in", "cdn.nobroker.in", "nobroker.in/nb-new"],
        "MagicBricks": ["img.staticmb.com", "mediacdn.99acres.com", "magicbricks.com"],
        "99acres": ["mediacdn.99acres.com", "99acres.com"],
        "Housing.com": ["housing.com", "housingcdn.com", "hc-img.housing.com",
                         "is1-2.housingcdn.com", "is1-3.housingcdn.com",
                         "is2-2.housingcdn.com", "is2-3.housingcdn.com",
                         "is3-2.housingcdn.com", "is3-3.housingcdn.com"],
        "SquareYards": ["img.squareyards.com", "squareyards.com"],
    }

    allowed = cdn_domains.get(source, [])
    skip_keywords = ["logo", "icon", "avatar", "placeholder", "noimage", "no-image",
                      "default", "blank", "spinner", "loading", "map", "watermark",
                      "verified", "badge", "tag", "star", "rating"]

    for img in soup_or_tag.find_all("img"):
        # Check all possible image source attributes
        for attr in ["src", "data-src", "data-lazyimg", "data-lazy-src",
                      "data-original", "data-lazy", "data-img", "data-image",
                      "data-hi-res-src", "data-hi-res"]:
            src = img.get(attr, "")
            if not src or not src.startswith("http"):
                continue
            if src in seen:
                continue
            # Skip non-property images
            if any(kw in src.lower() for kw in skip_keywords):
                continue
            # Must be an image file
            if not re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', src, re.I):
                continue
            # If we know CDN domains, prefer those; otherwise accept any
            if allowed:
                if any(d in src for d in allowed):
                    seen.add(src)
                    images.append(src)
                # Also accept images > 200px dimensions (likely property photos)
                elif not allowed:
                    seen.add(src)
                    images.append(src)
            else:
                seen.add(src)
                images.append(src)

    # Also check for background-image in style attributes
    for tag in soup_or_tag.find_all(style=True):
        style = tag.get("style", "")
        bg_match = re.search(r'background-image\s*:\s*url\(["\']?(https?://[^"\')\s]+)', style)
        if bg_match:
            src = bg_match.group(1)
            if src not in seen and not any(kw in src.lower() for kw in skip_keywords):
                if re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', src, re.I):
                    seen.add(src)
                    images.append(src)

    return images


def _enrich_images_from_url(listing, timeout=10):
    """Fetch a listing URL and extract images. Returns updated image list."""
    url = listing.get("url", "")
    if not url or not url.startswith("http"):
        return listing.get("images", [])

    existing = list(listing.get("images", []))
    source = listing.get("source", "")

    try:
        # Use curl_cffi for Housing.com (Akamai bot detection)
        if source == "Housing.com" and HAS_CURL_CFFI:
            resp = curl_requests.get(url, timeout=timeout, impersonate="chrome124",
                                      headers={"Accept-Language": "en-US,en;q=0.9"})
        else:
            resp = session.get(url, timeout=timeout, headers=BROWSER_HEADERS)
        if resp.status_code != 200:
            return existing

        soup = BeautifulSoup(resp.text, "html.parser")

        # 1. og:image meta tag
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            img_url = og_img["content"]
            if img_url.startswith("http") and img_url not in existing:
                existing.insert(0, img_url)

        # 2. All images from the page
        page_images = _extract_all_images_from_html(soup, source)
        for img in page_images:
            if img not in existing:
                existing.append(img)
            if len(existing) >= 6:
                break

        # 3. Source-specific: scan full HTML for CDN image URLs
        if source == "NoBroker" and len(existing) < 6:
            next_tag = soup.find("script", id="__NEXT_DATA__")
            if next_tag and next_tag.string:
                try:
                    nd = json.loads(next_tag.string)
                    nd_str = json.dumps(nd)
                    cdn_urls = re.findall(r'(https?://assets\.nobroker\.in/img/[^"\\]+)', nd_str)
                    for u in cdn_urls:
                        if u not in existing and re.search(r'\.(jpg|jpeg|png|webp)', u, re.I):
                            existing.append(u)
                        if len(existing) >= 6:
                            break
                except (json.JSONDecodeError, TypeError):
                    pass

        if source == "Housing.com" and len(existing) < 6:
            # Scan full page text for Housing.com CDN image URLs
            page_text = resp.text
            housing_cdn_urls = re.findall(
                r'(https?://(?:is\d+-\d+\.housingcdn\.com|hc-img\.housing\.com|'
                r'housing\.com/[^\s"\']+\.(?:jpg|jpeg|png|webp))[^\s"\']*)',
                page_text, re.I
            )
            for u in housing_cdn_urls:
                if u not in existing:
                    skip = ["logo", "icon", "avatar", "placeholder", "watermark", "map"]
                    if not any(kw in u.lower() for kw in skip):
                        existing.append(u)
                if len(existing) >= 6:
                    break

    except Exception as e:
        print(f"    [enrich] Error fetching {url[:60]}: {e}")

    return existing[:6]


# ═══════════════════════════════════════════════════════════════════════════════
# NoBroker — Primary source (API + HTML fallback)
# ═══════════════════════════════════════════════════════════════════════════════

def search_nobroker(area_name, bhk="3bhk"):
    """Try multiple approaches to get NoBroker listings."""
    # Approach 1: API
    listings = _nobroker_api(area_name, bhk)
    if listings:
        return listings

    # Approach 2: HTML with __NEXT_DATA__ parsing + card extraction
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

    api_endpoints = [
        {
            "url": "https://www.nobroker.in/api/v1/property/filter/region/rent/hyderabad",
            "params": {
                "pageNo": 1, "searchParam": search_param,
                "type": bhk_api, "budget": f"0,{max_rent}",
                "sharedAccomodation": 0, "radius": 2.0,
            },
        },
        {
            "url": f"https://www.nobroker.in/api/v3/multi/property/RENT/filter",
            "params": {
                "city": "hyderabad", "locality": area_name,
                "type": bhk_api, "budget": f",{max_rent}",
                "pageNo": 1, "sharedAccomodation": 0,
            },
        },
    ]

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

            if resp.status_code != 200 or "json" not in ct:
                continue

            data = resp.json()
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
                for prop in properties:
                    listing = _parse_nobroker_property(prop, area_name, bhk)
                    if listing:
                        listings.append(listing)
                if listings:
                    print(f"  [NoBroker API] Got {len(listings)} qualified in {area_name}")
                    return listings[:15]

        except requests.exceptions.JSONDecodeError:
            pass
        except Exception as e:
            print(f"  [NoBroker API] Error: {e}")

    return listings[:15]


def _nobroker_html(area_name, bhk):
    """Scrape NoBroker search page — try __NEXT_DATA__ first, then card extraction."""
    listings = []
    url = f"https://www.nobroker.in/{bhk}-flats-for-rent-in-{area_name}_hyderabad"

    try:
        resp = session.get(url, timeout=20)
        if resp.status_code != 200:
            return listings

        html_text = resp.text
        soup = BeautifulSoup(html_text, "html.parser")

        # ── Try __NEXT_DATA__ ──
        next_tag = soup.find("script", id="__NEXT_DATA__")
        if next_tag and next_tag.string:
            try:
                nd = json.loads(next_tag.string)
                page_props = nd.get("props", {}).get("pageProps", {})
                cards = _find_property_list(page_props)
                if cards:
                    for prop in cards:
                        listing = _parse_nobroker_property(prop, area_name, bhk)
                        if listing:
                            listings.append(listing)
                    if listings:
                        print(f"  [NoBroker HTML] Parsed {len(listings)} from __NEXT_DATA__")
                        return listings[:15]
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

        # ── Fallback: extract links + parse card context ──
        links = soup.find_all("a", href=re.compile(r"/property/.*?/detail"))

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
            card_data = _extract_card_data(link, bhk, area_name, pid)

            # Skip family-only listings
            if card_data.get("family_only"):
                continue

            has_data = card_data.get("rent", 0) > 0
            listings.append({
                "id": lid, "url": full_url, "source": "NoBroker",
                "bhk": bhk, "locality": area_name.title(),
                "needs_verification": not has_data,
                **card_data,
            })

        print(f"  [NoBroker HTML] Found {len(listings)} listings in {area_name}")

    except Exception as e:
        print(f"  [NoBroker HTML] Error: {e}")
        traceback.print_exc()

    return listings[:15]


def _extract_card_data(link_tag, bhk, area_name, property_id=""):
    """Extract listing data from the HTML card surrounding a NoBroker link."""
    data = {
        "project": "Unknown", "rent": 0, "sqft": 0, "floor": 0,
        "furnishing": "Unknown", "bachelor_verified": False,
        "images": [], "deposit": 0, "gated": False, "active": True,
    }

    # Walk up to find card container
    card = link_tag
    for _ in range(6):
        if card.parent and card.parent.name not in ["body", "html", "[document]"]:
            card = card.parent
        else:
            break

    card_text = card.get_text(" ", strip=True)

    # ── Extract rent ──
    for pattern in [
        r'[\u20b9]\s*([\d,]+)',
        r'(\d[\d,]+)\s*(?:\+[^R]*)?\s*(?:No Extra\s+)?(?:Maintenance\s+)?Rent',
        r'(\d[\d,]+)\s*(?:No Extra\s+)?Rent',
        r'(\d[\d,]+)\s*/\s*month',
    ]:
        m = re.search(pattern, card_text, re.I)
        if m:
            data["rent"] = int(m.group(1).replace(",", ""))
            break
    if data["rent"] == 0:
        numbers = re.findall(r'(\d[\d,]+)', card_text)
        for num_str in numbers:
            num = int(num_str.replace(",", ""))
            if 5000 <= num <= 100000:
                data["rent"] = num
                break

    # ── Extract deposit ──
    dep_match = re.search(r'([\d,]+)\s*Deposit', card_text, re.I)
    if dep_match:
        data["deposit"] = int(dep_match.group(1).replace(",", ""))

    # ── Extract sqft ──
    sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', card_text, re.I)
    if not sqft_match:
        sqft_match = re.search(r'([\d,]+)\s*(?:Builtup|Built[\s-]*up|Carpet|Super)', card_text, re.I)
    if sqft_match:
        data["sqft"] = int(sqft_match.group(1).replace(",", ""))

    # ── Extract floor (NoBroker: X/Y format) ──
    floor_match = re.search(r'(\d+)\s*/\s*(\d+)', card_text)
    if floor_match:
        data["floor"] = int(floor_match.group(1))
    if not floor_match or data["floor"] == 0:
        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)\s*(?:floor|of)', card_text, re.I)
        if floor_match:
            data["floor"] = int(floor_match.group(1))

    # ── Tenant preference ──
    tenant_text = card_text.lower()
    if re.search(r'all\s+preferred\s+tenants|bachelor\s+preferred|anyone', tenant_text):
        data["bachelor_verified"] = True
    elif re.search(r'family\s+preferred\s+tenants|family\s+only', tenant_text):
        data["bachelor_verified"] = False
        data["family_only"] = True

    # ── Furnishing ──
    if "fully furnished" in tenant_text or "fully-furnished" in tenant_text:
        data["furnishing"] = "Fully Furnished"
    elif "semi furnished" in tenant_text or "semi-furnished" in tenant_text:
        data["furnishing"] = "Semi-Furnished"
    elif "unfurnished" in tenant_text:
        data["furnishing"] = "Unfurnished"

    # ── Gated community ──
    if "posh society" in tenant_text or "gated" in tenant_text:
        data["gated"] = True

    # ── Extract images from card HTML ──
    card_images = _extract_all_images_from_html(card, "NoBroker")
    data["images"] = card_images[:5]

    # ── Construct CDN image URLs from property ID ──
    # NoBroker CDN pattern: https://assets.nobroker.in/img/{pid}/large/{pid}_{n}.jpg
    if property_id and len(data["images"]) < MIN_IMAGES:
        for n in range(5):
            cdn_url = f"https://assets.nobroker.in/img/{property_id}/large/{property_id}_{n}.jpg"
            if cdn_url not in data["images"]:
                data["images"].append(cdn_url)

    # ── Project name ──
    proj_match = re.search(
        r'(?:\d\s*BHK\s*(?:Apartment|Flat|Villa|House)\s*In\s+)(.+?)(?:\s+for\s+Rent)',
        card_text, re.I
    )
    if proj_match:
        data["project"] = proj_match.group(1).strip()
    else:
        href = link_tag.get("href", "")
        name_match = re.search(r'in-([a-z][\w-]+)-hyderabad', href)
        if name_match:
            raw = name_match.group(1).replace("-", " ").title()
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

    tenant = str(prop.get("tenantPreference", prop.get("leasetype", ""))).lower()
    bachelor_ok = any(kw in tenant for kw in ["bachelor", "anyone", "all", "single"])
    family_only = "family" in tenant and not bachelor_ok
    if family_only:
        return None

    floor = safe_int(prop.get("floor", prop.get("floorNo", 0)))
    images = extract_images_from_photos(prop.get("photos", prop.get("images", [])))

    photo_url = prop.get("photoUrl", prop.get("thumbnailImage", ""))
    if photo_url and photo_url.startswith("http") and photo_url not in images:
        images.insert(0, photo_url)

    # Construct CDN image URLs if we don't have enough
    if len(images) < MIN_IMAGES and prop_id:
        for n in range(5):
            cdn_url = f"https://assets.nobroker.in/img/{prop_id}/large/{prop_id}_{n}.jpg"
            if cdn_url not in images:
                images.append(cdn_url)

    project = prop.get("society", prop.get("title", prop.get("buildingName", "Unknown")))
    if isinstance(project, dict):
        project = project.get("name", "Unknown")
    if not project or project == "null":
        project = "Unknown"

    sqft = safe_int(prop.get("propertySize", prop.get("carpet_area",
                    prop.get("builtUpArea", prop.get("superBuiltupArea", 0)))))
    furnishing = str(prop.get("furnishing", prop.get("furnishingType", "Unfurnished")))
    deposit = safe_int(prop.get("deposit", 0))
    prop_type = str(prop.get("type", prop.get("propertyType", ""))).lower()
    gated = any(kw in prop_type for kw in ["apartment", "gated"])

    detail_url = (
        f"https://www.nobroker.in/property/"
        f"{bhk[0]}-bhk-apartment-for-rent-in-{area_name}"
        f"-hyderabad-for-rs-{rent}/{prop_id}/detail"
    )

    return {
        "id": f"nb_{str(prop_id)[:20]}",
        "url": detail_url,
        "rent": rent, "sqft": sqft, "floor": floor,
        "furnishing": furnishing, "bachelor_verified": bachelor_ok,
        "project": project, "locality": area_name.title(),
        "images": images[:6], "deposit": deposit,
        "source": "NoBroker", "bhk": bhk,
        "gated": gated, "active": True,
        "needs_verification": False,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 99acres — SSR with anti-bot headers
# ═══════════════════════════════════════════════════════════════════════════════

def search_99acres(area_name, bhk="3bhk"):
    listings = []
    bhk_num = bhk[0]
    url = f"https://www.99acres.com/{bhk_num}-bhk-flats-for-rent-in-{area_name}-hyderabad-ffid"
    print(f"  [99acres] Fetching {bhk} in {area_name}...")

    try:
        resp = session.get(url, timeout=20, headers={
            **BROWSER_HEADERS,
            "Referer": "https://www.99acres.com/",
            "Sec-Fetch-Site": "same-origin",
        })
        print(f"  [99acres] HTTP {resp.status_code}, size={len(resp.text)}")
        if resp.status_code != 200:
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        # Try __NEXT_DATA__ first
        next_tag = soup.find("script", id="__NEXT_DATA__")
        if next_tag and next_tag.string:
            try:
                nd = json.loads(next_tag.string)
                page_props = nd.get("props", {}).get("pageProps", {})
                cards = _find_property_list(page_props)
                if cards:
                    for prop in cards:
                        listing = _parse_generic_property(prop, area_name, bhk, "99acres",
                                                          "https://www.99acres.com")
                        if listing:
                            listings.append(listing)
                    if listings:
                        print(f"  [99acres] Parsed {len(listings)} from __NEXT_DATA__")
                        return listings[:15]
            except (json.JSONDecodeError, KeyError, TypeError):
                pass

        # Parse listing cards from HTML
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not re.search(r'/(\d{6,})', href):
                continue
            if "rent" not in href.lower() and "property" not in href.lower():
                continue
            id_match = re.search(r'/(\d{6,})', href)
            if not id_match:
                continue
            full_url = urljoin("https://www.99acres.com", href)
            lid = f"99a_{id_match.group(1)}"
            if any(l["id"] == lid for l in listings):
                continue

            card_data = _extract_generic_card(link, bhk, area_name, "99acres")
            listings.append({
                "id": lid, "url": full_url, "source": "99acres",
                "bhk": bhk, "locality": area_name.title(),
                "needs_verification": card_data.get("rent", 0) == 0,
                **card_data,
            })

    except Exception as e:
        print(f"  [99acres] Error: {e}")

    print(f"  [99acres] Found {len(listings)} in {area_name}")
    return listings[:15]


# ═══════════════════════════════════════════════════════════════════════════════
# MagicBricks — query-param URL works best, parse embedded JSON for images
# ═══════════════════════════════════════════════════════════════════════════════

def search_magicbricks(area_name, bhk="3bhk"):
    listings = []
    bhk_num = bhk[0]
    area_title = area_name.title()
    max_rent = BUDGET.get(bhk, 65000)

    # Query-param URL works best (returns 1.2MB page with data)
    urls_to_try = [
        (f"https://www.magicbricks.com/property-for-rent/residential-real-estate"
         f"?bedroom={bhk_num}&proptype=Multistorey-Apartment,Builder-Floor-Apartment,Penthouse,Studio-Apartment"
         f"&cityName=Hyderabad&Locality={area_title}"
         f"&BudgetMin=0&BudgetMax={max_rent}"),
        f"https://www.magicbricks.com/{bhk_num}-bhk-flats-for-rent-in-{area_title}-Hyderabad-pppfr",
        f"https://www.magicbricks.com/flats-for-rent-in-{area_title}-Hyderabad-pppfr",
    ]
    print(f"  [MagicBricks] Fetching {bhk} in {area_name}...")

    for url in urls_to_try:
        try:
            resp = session.get(url, timeout=20, headers={
                **BROWSER_HEADERS,
                "Referer": "https://www.magicbricks.com/",
                "Sec-Fetch-Site": "same-origin",
            })
            url_short = url.split("?")[0].split("/")[-1][:40]
            print(f"  [MagicBricks] {url_short} → HTTP {resp.status_code}, size={len(resp.text)}")

            if resp.status_code != 200 or len(resp.text) < 5000:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # ── Strategy 1: Parse embedded JSON data from script tags ──
            for script in soup.find_all("script"):
                text = script.string or ""
                if len(text) < 500:
                    continue

                # Look for window.__INITIAL_DATA__ or similar embedded JSON
                json_match = re.search(
                    r'(?:window\.__INITIAL_DATA__|window\.__INITIAL_STATE__|'
                    r'window\.mbData|window\.searchData|'
                    r'var\s+searchData|var\s+propertyData)\s*=\s*(\{.+?\});?\s*(?:</script>|$)',
                    text, re.S
                )
                if json_match:
                    try:
                        jdata = json.loads(json_match.group(1))
                        props = _find_property_list(jdata)
                        if props:
                            print(f"  [MagicBricks] Found {len(props)} props in embedded JSON")
                            for prop in props:
                                listing = _parse_magicbricks_property(prop, area_name, bhk)
                                if listing:
                                    listings.append(listing)
                            if listings:
                                break
                    except (json.JSONDecodeError, TypeError):
                        pass

                # Also look for JSON arrays containing property-like objects
                if '"propertyId"' in text or '"propId"' in text or '"id"' in text:
                    # Try to find a JSON array
                    array_match = re.search(r'(\[\s*\{[^;]{100,}?\}\s*\])', text, re.S)
                    if array_match:
                        try:
                            arr = json.loads(array_match.group(1))
                            if isinstance(arr, list) and arr and isinstance(arr[0], dict):
                                if any(k in arr[0] for k in ["propertyId", "propId", "id", "rent"]):
                                    print(f"  [MagicBricks] Found JSON array with {len(arr)} items")
                                    for prop in arr:
                                        listing = _parse_magicbricks_property(prop, area_name, bhk)
                                        if listing:
                                            listings.append(listing)
                                    if listings:
                                        break
                        except (json.JSONDecodeError, TypeError):
                            pass

            # ── Strategy 2: Parse HTML cards ──
            if not listings:
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    # MagicBricks property URLs contain long numeric IDs
                    if not re.search(r'(\d{8,})', href):
                        continue
                    # Must be a property link
                    if not any(kw in href.lower() for kw in ["property", "rent", "flat", "apartment", "bhk"]):
                        continue
                    id_match = re.search(r'(\d{8,})', href)
                    if not id_match:
                        continue
                    full_url = urljoin("https://www.magicbricks.com", href)
                    lid = f"mb_{id_match.group(1)}"
                    if any(l["id"] == lid for l in listings):
                        continue

                    card_data = _extract_generic_card(link, bhk, area_name, "MagicBricks")
                    listings.append({
                        "id": lid, "url": full_url, "source": "MagicBricks",
                        "bhk": bhk, "locality": area_name.title(),
                        "needs_verification": card_data.get("rent", 0) == 0,
                        **card_data,
                    })

            if listings:
                break

        except Exception as e:
            print(f"  [MagicBricks] Error: {e}")
            traceback.print_exc()

    print(f"  [MagicBricks] Found {len(listings)} in {area_name}")
    return listings[:15]


def _parse_magicbricks_property(prop, area_name, bhk):
    """Parse a MagicBricks property from JSON data."""
    if not isinstance(prop, dict):
        return None

    prop_id = str(prop.get("propertyId", prop.get("propId", prop.get("id", ""))))
    if not prop_id:
        return None

    rent = safe_int(prop.get("price", prop.get("rent", prop.get("expectedRent",
                    prop.get("rentAmount", 0)))))
    max_rent = BUDGET.get(bhk, 65000)
    if rent > max_rent or rent < 3000:
        return None

    # Images
    images = []
    for key in ["images", "photos", "photoGallery", "gallery", "multiImage",
                "imageList", "originalPhotos", "allImages"]:
        img_data = prop.get(key)
        if isinstance(img_data, list):
            for img in img_data[:6]:
                if isinstance(img, str) and img.startswith("http"):
                    images.append(img)
                elif isinstance(img, dict):
                    for img_key in ["largeImageUrl", "originalUrl", "url", "src",
                                     "largeImage", "mediumImageUrl", "thumbUrl"]:
                        u = img.get(img_key, "")
                        if u and u.startswith("http"):
                            images.append(u)
                            break
            if images:
                break

    # Single image fields
    for key in ["thumbnailUrl", "mainImage", "coverImage", "photoUrl",
                "largeImageUrl", "imageUrl", "originalImageUrl"]:
        u = prop.get(key, "")
        if isinstance(u, str) and u.startswith("http") and u not in images:
            images.insert(0, u)

    # MagicBricks CDN image construction
    # Pattern: https://img.staticmb.com/mbphoto/{id}/original/{id}_{n}_800.jpg
    if len(images) < MIN_IMAGES and prop_id:
        for n in range(1, 6):
            cdn_url = f"https://img.staticmb.com/mbphoto/{prop_id}/original/{prop_id}_{n}_800.jpg"
            if cdn_url not in images:
                images.append(cdn_url)

    prop_url = prop.get("url", prop.get("propertyUrl", prop.get("detailUrl", "")))
    if prop_url and not prop_url.startswith("http"):
        prop_url = urljoin("https://www.magicbricks.com", prop_url)

    project = str(prop.get("society", prop.get("projectName",
                  prop.get("buildingName", prop.get("title", "Unknown")))))

    sqft = safe_int(prop.get("area", prop.get("builtupArea",
            prop.get("carpetArea", prop.get("superArea", 0)))))
    floor = safe_int(prop.get("floor", prop.get("floorNo", 0)))
    furnishing = str(prop.get("furnishing", prop.get("furnishType", "Unknown")))

    return {
        "id": f"mb_{prop_id}",
        "url": prop_url or f"https://www.magicbricks.com/propertyDetails/{prop_id}",
        "rent": rent, "sqft": sqft, "floor": floor,
        "furnishing": furnishing, "bachelor_verified": False,
        "project": project, "locality": area_name.title(),
        "images": images[:6], "deposit": safe_int(prop.get("securityDeposit", 0)),
        "source": "MagicBricks", "bhk": bhk,
        "gated": False, "active": True,
        "needs_verification": False,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Housing.com — needs proper browser headers to avoid 406
# ═══════════════════════════════════════════════════════════════════════════════

def search_housing(area_name, bhk="3bhk"):
    """Search Housing.com using curl_cffi to bypass Akamai bot detection."""
    listings = []
    bhk_num = bhk[0]
    area_lower = area_name.lower()

    if not HAS_CURL_CFFI:
        print(f"  [Housing.com] Skipping — curl_cffi not available (needed to bypass bot detection)")
        return listings

    # Housing.com requires locality IDs in URLs (discovered via sitemap)
    # Format: /rent/{bhk}bhk-flats-for-rent-in-{area}-hyderabad-C{bhk_code}P{locality_id}
    # BHK codes: 2BHK=C4, 3BHK=C8
    HOUSING_LOCALITIES = {
        "kondapur":   "P5bp8fs9w5gm0jsim",
        "gachibowli": "Pg7khohd393v9det",
        "kokapet":    "P66lqyz1e9298u5ou",
    }
    BHK_CODES = {"2bhk": "C4", "3bhk": "C8"}

    loc_id = HOUSING_LOCALITIES.get(area_lower, "")
    bhk_code = BHK_CODES.get(bhk, "C8")

    if not loc_id:
        print(f"  [Housing.com] No locality ID for {area_name}")
        return listings

    # Construct correct URL with locality ID
    urls_to_try = [
        f"https://housing.com/rent/{bhk_num}bhk-flats-for-rent-in-{area_lower}-hyderabad-{bhk_code}{loc_id}",
        f"https://housing.com/rent/flats-for-rent-in-{area_lower}-hyderabad-{loc_id}",
    ]
    print(f"  [Housing.com] Fetching {bhk} in {area_name} (curl_cffi)...")

    for url in urls_to_try:
        try:
            # Use curl_cffi with Chrome impersonation to bypass Akamai
            resp = curl_requests.get(
                url,
                timeout=20,
                impersonate="chrome124",
                headers={
                    "Accept-Language": "en-US,en;q=0.9",
                },
            )
            url_short = url.split("/")[-1][:50]
            print(f"  [Housing.com] {url_short} → HTTP {resp.status_code}, size={len(resp.text)}")

            if resp.status_code != 200:
                continue
            if len(resp.text) < 5000:
                continue

            # ── PRIMARY: Parse window.__INITIAL_STATE__ = JSON.parse("...") ──
            # Housing.com uses a double-encoded JSON blob (string inside JSON.parse)
            # Format: window.__INITIAL_STATE__=JSON.parse("escaped_json_string");
            # Structure: state.searchResults.listings = [{id, key}...]
            #            state.searchResults.data = {id: {property_object}, ...}
            init_match = re.search(
                r'window\.__INITIAL_STATE__\s*=\s*JSON\.parse\("(.*?)"\)\s*;',
                resp.text, re.S
            )
            if init_match:
                try:
                    # Double-decode: first unescape the string, then parse JSON
                    raw_escaped = init_match.group(1)
                    unescaped = json.loads('"' + raw_escaped + '"')
                    state_data = json.loads(unescaped)

                    sr = state_data.get("searchResults", {})
                    sr_data = sr.get("data", {})
                    sr_listings = sr.get("listings", [])

                    if sr_data and sr_listings:
                        print(f"  [Housing.com] __INITIAL_STATE__ found {len(sr_listings)} listings, {len(sr_data)} data entries")
                        for item in sr_listings:
                            prop_id = str(item.get("id", ""))
                            prop = sr_data.get(prop_id)
                            if not prop or not isinstance(prop, dict):
                                continue
                            listing = _parse_housing_initial_state(prop, area_name, bhk)
                            if listing:
                                listings.append(listing)
                        if listings:
                            print(f"  [Housing.com] Parsed {len(listings)} from __INITIAL_STATE__")
                            return listings[:15]
                    else:
                        top_keys = list(state_data.keys())[:10]
                        print(f"  [Housing.com] __INITIAL_STATE__ no searchResults. Keys: {top_keys}")
                except (json.JSONDecodeError, TypeError, ValueError) as e:
                    print(f"  [Housing.com] __INITIAL_STATE__ parse error: {e}")
            else:
                # Try fallback: raw JSON assignment (older format)
                init_match2 = re.search(
                    r'window\.__INITIAL_STATE__\s*=\s*(\{.+?\})\s*;?\s*(?:</script>|window\.)',
                    resp.text, re.S
                )
                if init_match2:
                    try:
                        state_data = json.loads(init_match2.group(1))
                        sr = state_data.get("searchResults", {})
                        sr_data = sr.get("data", {})
                        if sr_data:
                            for prop_id, prop in sr_data.items():
                                listing = _parse_housing_initial_state(prop, area_name, bhk)
                                if listing:
                                    listings.append(listing)
                            if listings:
                                print(f"  [Housing.com] Parsed {len(listings)} from __INITIAL_STATE__ (raw)")
                                return listings[:15]
                    except (json.JSONDecodeError, TypeError):
                        pass

                print(f"  [Housing.com] No __INITIAL_STATE__ found, trying HTML fallback...")

            soup = BeautifulSoup(resp.text, "html.parser")

            # Fallback A: Parse Housing.com listing cards from HTML
            # Housing.com renders listing cards server-side with class patterns
            if not listings:
                listings = _parse_housing_html_cards(soup, area_name, bhk)
                if listings:
                    print(f"  [Housing.com] Parsed {len(listings)} from HTML cards")

            # Fallback B: parse links (last resort)
            if not listings:
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    id_match = re.search(r'/(\d{8,})', href)
                    if id_match and any(kw in href.lower() for kw in ["rent", "property", "flat", "apartment"]):
                        full_url = urljoin("https://housing.com", href)
                        lid = f"hc_{id_match.group(1)}"
                        if not any(l["id"] == lid for l in listings):
                            card_data = _extract_generic_card(link, bhk, area_name, "Housing.com")
                            listings.append({
                                "id": lid, "url": full_url, "source": "Housing.com",
                                "bhk": bhk, "locality": area_name.title(),
                                "needs_verification": card_data.get("rent", 0) == 0,
                                **card_data,
                            })

            if listings:
                break

        except Exception as e:
            print(f"  [Housing.com] Error: {e}")
            traceback.print_exc()

    print(f"  [Housing.com] Found {len(listings)} in {area_name}")
    return listings[:15]


def _parse_housing_initial_state(prop, area_name, bhk):
    """Parse a Housing.com property from the __INITIAL_STATE__ searchResults.data structure.
    This is the most reliable parser — extracts images, rent, floor, furnishing directly."""
    if not isinstance(prop, dict):
        return None

    prop_id = str(prop.get("listingId", prop.get("originalListingId", prop.get("id", ""))))
    if not prop_id:
        return None

    # Skip inactive
    if not prop.get("isActiveProperty", True):
        return None

    # Rent / Price
    rent = safe_int(prop.get("price", 0))
    if rent == 0:
        dp = prop.get("displayPrice", {})
        if isinstance(dp, dict):
            vals = dp.get("value", [])
            if isinstance(vals, list) and vals:
                rent = safe_int(vals[0])
    max_rent = BUDGET.get(bhk, 65000)
    if rent > max_rent or rent < 3000:
        return None

    # Images — Housing.com has rich image data
    images = []
    # 1. Cover image
    cover = prop.get("coverImage", {})
    if isinstance(cover, dict):
        cover_src = cover.get("src", "")
        if cover_src and cover_src.startswith("http"):
            images.append(cover_src)

    # 2. Gallery images from details.images
    details = prop.get("details", {})
    if isinstance(details, dict):
        img_groups = details.get("images", [])
        if isinstance(img_groups, list):
            for group in img_groups:
                if not isinstance(group, dict):
                    continue
                # Prefer "property" type over "locality"
                group_type = group.get("type", "")
                group_images = group.get("images", [])
                if isinstance(group_images, list):
                    for img_obj in group_images:
                        if isinstance(img_obj, dict):
                            src = img_obj.get("src", "")
                            if src and src.startswith("http") and src not in images:
                                images.append(src)
                        elif isinstance(img_obj, str) and img_obj.startswith("http"):
                            if img_obj not in images:
                                images.append(img_obj)
                        if len(images) >= 6:
                            break
                if len(images) >= 6:
                    break

    # URL
    prop_url = prop.get("url", "")
    if prop_url and not prop_url.startswith("http"):
        prop_url = "https://housing.com" + prop_url

    # Floor and furnishing from featureAndConfig array
    floor = 0
    furnishing = "Unknown"
    features = prop.get("featureAndConfig", [])
    if isinstance(features, list):
        for f in features:
            if not isinstance(f, dict):
                continue
            label = f.get("label", "")
            desc = f.get("description", "")
            if label == "Floor number" and desc:
                floor_match = re.match(r'(\d+)', desc)
                if floor_match:
                    floor = int(floor_match.group(1))
            elif label == "Furnishing" and desc:
                furnishing = desc  # "Unfurnished", "Semi Furnished", "Fully Furnished"

    # If floor not in features, try top-level
    if floor == 0:
        floor = safe_int(prop.get("floor", prop.get("floorNo", 0)))

    # Sqft
    sqft = 0
    built_up = prop.get("builtUpArea", {})
    if isinstance(built_up, dict):
        sqft = safe_int(built_up.get("value", 0))
    if sqft == 0:
        sqft = safe_int(prop.get("carpetArea", prop.get("superArea", 0)))

    # Project / society name
    project = str(prop.get("title", "Unknown"))
    address = prop.get("address", {})
    if isinstance(address, dict):
        addr_str = address.get("address", "")
        if addr_str and project == "Unknown":
            project = addr_str

    # Deposit
    deposit = 0
    dp = prop.get("displayPrice", {})
    if isinstance(dp, dict):
        deposit = safe_int(dp.get("deposit", 0))

    # Bachelor — check tenant preference if available
    bachelor = False
    tenant_pref = str(prop.get("tenantPreference", "")).lower()
    if any(kw in tenant_pref for kw in ["bachelor", "anyone", "all"]):
        bachelor = True

    return {
        "id": f"hc_{prop_id}",
        "url": prop_url or f"https://housing.com/rent/property/{prop_id}",
        "rent": rent,
        "sqft": sqft,
        "floor": floor,
        "furnishing": furnishing,
        "bachelor_verified": bachelor,
        "project": project,
        "locality": area_name.title(),
        "images": images[:6],
        "deposit": deposit,
        "source": "Housing.com",
        "bhk": bhk,
        "gated": False,
        "active": True,
        "needs_verification": False,
    }


def _parse_housing_html_cards(soup, area_name, bhk):
    """Parse Housing.com listing cards directly from the search page HTML.
    Housing.com renders property cards with images, prices, and links
    in the server-side HTML. This extracts them without needing __NEXT_DATA__."""
    listings = []
    max_rent = BUDGET.get(bhk, 65000)

    # Housing.com image CDN patterns
    housing_img_patterns = [
        "housingcdn.com", "housing.com", "hc-img", "is1-2.", "is1-3.",
        "is2-2.", "is2-3.", "is3-2.", "is3-3.",
    ]

    # Find all links to property detail pages
    property_links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Housing.com property URLs: /rent/..../N where N is 8+ digit ID
        # Also match /in/rent/... and /property/... patterns
        id_match = re.search(r'/(\d{8,})', href)
        if id_match and any(kw in href.lower() for kw in ["rent", "property", "flat", "apartment"]):
            property_links.append((link, id_match.group(1), href))

    # Walk up from each link to find the enclosing card, extract images + data
    seen_ids = set()
    for link, prop_id, href in property_links:
        lid = f"hc_{prop_id}"
        if lid in seen_ids:
            continue
        seen_ids.add(lid)

        full_url = urljoin("https://housing.com", href)

        # Walk up to find the card container (typically 3-6 levels up)
        card = link
        for _ in range(8):
            parent = card.parent
            if parent and parent.name not in ["body", "html", "[document]"]:
                card = parent
                # Stop if card seems large enough to be a listing card
                card_classes = " ".join(card.get("class", []))
                if any(kw in card_classes.lower() for kw in ["card", "listing", "property", "result"]):
                    break
            else:
                break

        card_text = card.get_text(" ", strip=True)

        # Extract images from card — check ALL img tags for Housing.com CDN URLs
        images = []
        for img in card.find_all("img"):
            for attr in ["src", "data-src", "data-lazyimg", "data-lazy-src",
                          "data-original", "data-lazy", "data-img", "data-image",
                          "data-hi-res-src", "data-hi-res", "content"]:
                src = img.get(attr, "")
                if not src or not src.startswith("http"):
                    continue
                if src in images:
                    continue
                # Accept Housing.com CDN images or any real property image
                if any(d in src for d in housing_img_patterns):
                    images.append(src)
                    break
                # Also accept images with common image extensions from any CDN
                if re.search(r'\.(jpg|jpeg|png|webp)(\?|$)', src, re.I):
                    skip = ["logo", "icon", "avatar", "placeholder", "noimage",
                            "default", "blank", "spinner", "loading", "watermark",
                            "verified", "badge", "tag", "star", "rating", "map"]
                    if not any(kw in src.lower() for kw in skip):
                        images.append(src)
                        break

        # Also check background-image CSS
        for tag in card.find_all(style=True):
            style = tag.get("style", "")
            bg_match = re.search(r'background-image\s*:\s*url\(["\']?(https?://[^"\')\s]+)', style)
            if bg_match:
                src = bg_match.group(1)
                if src not in images and any(d in src for d in housing_img_patterns):
                    images.append(src)

        # Extract rent from card text
        rent = 0
        for pattern in [
            r'[\u20b9₹]\s*([\d,]+)',
            r'(\d[\d,]+)\s*/\s*(?:month|mo)',
            r'(?:rent|price)\s*[:\s]*[\u20b9₹]?\s*([\d,]+)',
        ]:
            m = re.search(pattern, card_text, re.I)
            if m:
                rent = int(m.group(1).replace(",", ""))
                if 3000 <= rent <= max_rent:
                    break
                rent = 0

        # Extract sqft
        sqft = 0
        sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', card_text, re.I)
        if sqft_match:
            sqft = int(sqft_match.group(1).replace(",", ""))

        # Floor
        floor = 0
        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)\s*(?:floor|of)', card_text, re.I)
        if floor_match:
            floor = int(floor_match.group(1))

        # Furnishing
        ct_lower = card_text.lower()
        furnishing = "Unknown"
        if "fully furnished" in ct_lower:
            furnishing = "Fully Furnished"
        elif "semi" in ct_lower and "furnished" in ct_lower:
            furnishing = "Semi-Furnished"
        elif "unfurnished" in ct_lower:
            furnishing = "Unfurnished"

        # Project name — try extracting from structured elements first
        project = "Unknown"
        # Look for heading/title elements in the card
        for heading in card.find_all(["h2", "h3", "h4", "span", "a"]):
            heading_classes = " ".join(heading.get("class", []))
            if any(kw in heading_classes.lower() for kw in ["title", "name", "heading", "project"]):
                project = heading.get_text(strip=True)
                if project and len(project) > 3:
                    break
        if project == "Unknown":
            proj_match = re.search(
                r'(?:\d\s*BHK\s*(?:Apartment|Flat|House|Villa)?\s*(?:in|at)\s+)(.+?)(?:\s+for|\s+in\s+)',
                card_text, re.I
            )
            if proj_match:
                project = proj_match.group(1).strip()

        # Bachelor
        bachelor = bool(re.search(r'bachelor|anyone|single\s*men', ct_lower))

        listings.append({
            "id": lid,
            "url": full_url,
            "rent": rent,
            "sqft": sqft,
            "floor": floor,
            "furnishing": furnishing,
            "bachelor_verified": bachelor,
            "project": project,
            "locality": area_name.title(),
            "images": images[:6],
            "deposit": 0,
            "source": "Housing.com",
            "bhk": bhk,
            "gated": "gated" in ct_lower,
            "active": True,
            "needs_verification": rent == 0,
        })

    return listings[:15]


def _parse_housing_property(prop, area_name, bhk):
    """Parse a Housing.com property from JSON data."""
    if not isinstance(prop, dict):
        return None

    prop_id = str(prop.get("id", prop.get("propertyId", prop.get("listingId", ""))))
    if not prop_id:
        return None

    rent = safe_int(prop.get("price", prop.get("rent", prop.get("expectedRent", 0))))
    max_rent = BUDGET.get(bhk, 65000)
    if rent > max_rent or rent < 3000:
        return None

    # Images from Housing CDN
    images = []
    for key in ["images", "photos", "gallery", "coverImages", "imageList"]:
        img_data = prop.get(key)
        if isinstance(img_data, list):
            for img in img_data[:6]:
                if isinstance(img, str) and img.startswith("http"):
                    images.append(img)
                elif isinstance(img, dict):
                    for img_key in ["url", "src", "originalUrl", "hdUrl", "largeUrl"]:
                        u = img.get(img_key, "")
                        if u and u.startswith("http"):
                            images.append(u)
                            break
            if images:
                break

    # Single image fields
    for key in ["coverImage", "thumbnailUrl", "mainImage", "photoUrl"]:
        u = prop.get(key, "")
        if isinstance(u, str) and u.startswith("http") and u not in images:
            images.insert(0, u)

    prop_url = prop.get("url", prop.get("propertyUrl", prop.get("detailUrl", "")))
    if prop_url and not prop_url.startswith("http"):
        prop_url = urljoin("https://housing.com", prop_url)

    project = str(prop.get("society", prop.get("projectName",
                  prop.get("buildingName", prop.get("title", "Unknown")))))

    return {
        "id": f"hc_{prop_id}",
        "url": prop_url or f"https://housing.com/in/rent/property/{prop_id}",
        "rent": rent,
        "sqft": safe_int(prop.get("area", prop.get("builtupArea",
                prop.get("carpetArea", prop.get("superArea", 0))))),
        "floor": safe_int(prop.get("floor", prop.get("floorNo", 0))),
        "furnishing": str(prop.get("furnishing", prop.get("furnishType", "Unknown"))),
        "bachelor_verified": False,
        "project": project,
        "locality": area_name.title(),
        "images": images[:6],
        "deposit": safe_int(prop.get("securityDeposit", prop.get("deposit", 0))),
        "source": "Housing.com",
        "bhk": bhk,
        "gated": False,
        "active": True,
        "needs_verification": False,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# SquareYards — SSR HTML, lowest anti-bot
# ═══════════════════════════════════════════════════════════════════════════════

def search_squareyards(area_name, bhk="3bhk"):
    """Search SquareYards — parse <article class='listing-card'> containers with data-* attributes."""
    listings = []
    bhk_num = bhk[0]
    area_lower = area_name.lower()

    # The property-for-rent URL returns more results than bhk-specific
    urls_to_try = [
        f"https://www.squareyards.com/rent/{bhk_num}-bhk-for-rent-in-{area_lower}-hyderabad",
        f"https://www.squareyards.com/rent/property-for-rent-in-{area_lower}-hyderabad",
    ]
    print(f"  [SquareYards] Fetching {bhk} in {area_name}...")

    for url in urls_to_try:
        try:
            # IMPORTANT: Exclude Brotli (br) from Accept-Encoding.
            # Python requests can't decompress Brotli without the brotli package.
            # Without this, SquareYards returns br-compressed binary that gets
            # treated as garbled text → 0 parsed elements.
            sy_headers = {**BROWSER_HEADERS}
            sy_headers["Accept-Encoding"] = "gzip, deflate"
            sy_headers["Referer"] = "https://www.squareyards.com/"
            sy_headers["Sec-Fetch-Site"] = "same-origin"
            resp = session.get(url, timeout=20, headers=sy_headers)
            print(f"  [SquareYards] {url.split('/')[-1][:50]} → HTTP {resp.status_code}, size={len(resp.text)}")
            if resp.status_code != 200 or len(resp.text) < 5000:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Diagnostic: log what HTML structure we got
            all_divs_with_class = [
                (t.name, " ".join(t.get("class", [])))
                for t in soup.find_all(True, class_=True)
                if any(kw in " ".join(t.get("class", [])).lower()
                       for kw in ["listing", "card", "property", "result"])
            ][:10]
            if all_divs_with_class:
                print(f"  [SquareYards] Listing-like classes found: {all_divs_with_class[:5]}")
            else:
                # Log a sample of all classes to understand page structure
                all_classes = set()
                for t in soup.find_all(True, class_=True):
                    for c in t.get("class", []):
                        all_classes.add(c)
                print(f"  [SquareYards] No listing classes. Sample classes: {sorted(all_classes)[:15]}")
                # Check for JS-only rendering indicators
                scripts = soup.find_all("script", src=True)
                js_srcs = [s.get("src", "")[-40:] for s in scripts[:5]]
                print(f"  [SquareYards] JS bundles: {js_srcs}")

            # SquareYards uses <article class="listing-card" propertyid="..."> OR <div class="listing-card">
            cards = soup.find_all("article", class_=re.compile(r'listing[-_]?card'))
            if not cards:
                # Try div fallback (SquareYards may use div instead of article)
                cards = soup.find_all("div", class_=re.compile(r'listing[-_]?card'))
            if not cards:
                print(f"  [SquareYards] No listing-card found, trying data attributes...")
                # Try finding by favorite-btn data attributes
                fav_btns = soup.find_all(attrs={"data-propertyid": True})
                for btn in fav_btns:
                    # Walk up to find parent card
                    parent = btn
                    for _ in range(4):
                        if parent.parent and parent.parent.name not in ["body", "html", "[document]"]:
                            parent = parent.parent
                        else:
                            break
                    if parent not in cards:
                        cards.append(parent)

            if cards:
                print(f"  [SquareYards] Found {len(cards)} listing cards")

            for card in cards:
                # Get property ID from article tag or favorite-btn
                prop_id = card.get("propertyid", "")
                if not prop_id:
                    fav_btn = card.find(attrs={"data-propertyid": True})
                    if fav_btn:
                        prop_id = fav_btn.get("data-propertyid", "")
                if not prop_id:
                    continue

                lid = f"sy_{prop_id}"
                if any(l["id"] == lid for l in listings):
                    continue

                # Extract structured data from favorite-btn data attributes
                fav_btn = card.find(class_=re.compile(r'favorite|shortlist'))
                if not fav_btn:
                    fav_btn = card.find(attrs={"data-price": True})

                rent = 0
                sqft = 0
                project = "Unknown"
                locality = area_name.title()

                if fav_btn:
                    rent = safe_int(fav_btn.get("data-price", 0))
                    sqft_str = fav_btn.get("data-area", "")
                    sqft = safe_int(re.sub(r'[^\d]', '', sqft_str))
                    project = fav_btn.get("data-projectname", fav_btn.get("data-name", "Unknown"))
                    locality = fav_btn.get("data-sublocalityname", area_name.title())

                # Fallback: parse rent from text
                if rent == 0:
                    price_tag = card.find(class_=re.compile(r'listing[-_]?price'))
                    if price_tag:
                        price_text = price_tag.get_text(" ", strip=True)
                        m = re.search(r'([\d,]+)', price_text)
                        if m:
                            rent = int(m.group(1).replace(",", ""))

                # Budget check
                max_rent = BUDGET.get(bhk, 65000)
                if rent <= 0 or rent > max_rent:
                    continue

                # Floor
                floor = 0
                card_text = card.get_text(" ", strip=True)
                floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)\s*(?:floor|of)', card_text, re.I)
                if floor_match:
                    floor = int(floor_match.group(1))

                # Furnishing
                furnishing = "Unknown"
                ct_lower = card_text.lower()
                if "fully furnished" in ct_lower or "fully-furnished" in ct_lower:
                    furnishing = "Fully Furnished"
                elif "semi" in ct_lower and "furnished" in ct_lower:
                    furnishing = "Semi-Furnished"
                elif "unfurnished" in ct_lower:
                    furnishing = "Unfurnished"

                # Project name from .project-name span
                if project == "Unknown":
                    pn_tag = card.find(class_="project-name")
                    if pn_tag:
                        project = pn_tag.get_text(strip=True)

                # Detail page URL from .listing-body data-url
                detail_url = ""
                body_tag = card.find(class_=re.compile(r'listing[-_]?body'))
                if body_tag:
                    detail_url = body_tag.get("data-url", "")
                if not detail_url:
                    # Try bxslider item data-href
                    item_tag = card.find(class_="item")
                    if item_tag:
                        detail_url = item_tag.get("data-href", "")
                if detail_url and not detail_url.startswith("http"):
                    detail_url = urljoin("https://www.squareyards.com", detail_url)
                if not detail_url:
                    detail_url = f"https://www.squareyards.com/rental-{bhk_num}-bhk-apartment/{prop_id}"

                # Images from bxslider
                images = []
                for img in card.find_all("img", class_=re.compile(r'img[-_]?responsive')):
                    for attr in ["src", "data-src", "data-lazy"]:
                        src = img.get(attr, "")
                        if src and src.startswith("http") and "squareyards" in src:
                            if src not in images:
                                images.append(src)
                            break
                # Also check all images
                if not images:
                    images = _extract_all_images_from_html(card, "SquareYards")

                listings.append({
                    "id": lid,
                    "url": detail_url,
                    "rent": rent,
                    "sqft": sqft,
                    "floor": floor,
                    "furnishing": furnishing,
                    "bachelor_verified": False,
                    "project": project,
                    "locality": locality,
                    "images": images[:6],
                    "deposit": 0,
                    "source": "SquareYards",
                    "bhk": bhk,
                    "gated": False,
                    "active": True,
                    "needs_verification": False,
                })

            if listings:
                break

        except Exception as e:
            print(f"  [SquareYards] Error: {e}")
            traceback.print_exc()

    print(f"  [SquareYards] Found {len(listings)} in {area_name}")
    return listings[:15]


# ═══════════════════════════════════════════════════════════════════════════════
# Generic helpers for multi-platform parsing
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_generic_property(prop, area_name, bhk, source, base_url):
    """Parse a property dict from JSON (__NEXT_DATA__, initialState, etc.)."""
    if not isinstance(prop, dict):
        return None

    prop_id = str(prop.get("id", prop.get("propertyId", prop.get("listingId", ""))))
    if not prop_id:
        return None

    rent = safe_int(prop.get("rent", prop.get("price", prop.get("expectedPrice", 0))))
    max_rent = BUDGET.get(bhk, 65000)
    if rent > max_rent or rent < 3000:
        return None

    prop_url = prop.get("url", prop.get("propertyUrl", prop.get("detailUrl", "")))
    if prop_url and not prop_url.startswith("http"):
        prop_url = urljoin(base_url, prop_url)

    images = []
    for key in ["images", "photos", "imageList", "galleryImages"]:
        img_list = prop.get(key, [])
        if isinstance(img_list, list):
            for img in img_list[:6]:
                if isinstance(img, str) and img.startswith("http"):
                    images.append(img)
                elif isinstance(img, dict):
                    u = img.get("url", img.get("src", img.get("originalUrl", "")))
                    if u and u.startswith("http"):
                        images.append(u)
            if images:
                break
    for key in ["thumbnailUrl", "mainImage", "coverImage", "photoUrl"]:
        u = prop.get(key, "")
        if isinstance(u, str) and u.startswith("http") and u not in images:
            images.insert(0, u)

    project = str(prop.get("society", prop.get("projectName",
                  prop.get("buildingName", prop.get("title", "Unknown")))))

    return {
        "id": f"{source[:3].lower()}_{prop_id}",
        "url": prop_url or "",
        "rent": rent,
        "sqft": safe_int(prop.get("area", prop.get("builtupArea",
                prop.get("carpet_area", prop.get("superArea", 0))))),
        "floor": safe_int(prop.get("floor", prop.get("floorNo", 0))),
        "furnishing": str(prop.get("furnishing", prop.get("furnishingType", "Unknown"))),
        "bachelor_verified": False,
        "project": project,
        "locality": area_name.title(),
        "images": images[:5],
        "deposit": safe_int(prop.get("securityDeposit", prop.get("deposit", 0))),
        "source": source,
        "bhk": bhk,
        "gated": False,
        "active": True,
        "needs_verification": False,
    }


def _extract_generic_card(link_tag, bhk, area_name, source):
    """Extract listing data from an HTML card surrounding a link."""
    data = {
        "project": "Unknown", "rent": 0, "sqft": 0, "floor": 0,
        "furnishing": "Unknown", "bachelor_verified": False,
        "images": [], "deposit": 0, "gated": False, "active": True,
    }

    card = link_tag
    for _ in range(6):
        if card.parent and card.parent.name not in ["body", "html", "[document]"]:
            card = card.parent
        else:
            break

    card_text = card.get_text(" ", strip=True)

    # Rent
    for pattern in [
        r'[\u20b9]\s*([\d,]+)',
        r'(\d[\d,]+)\s*(?:\+[^R]*)?\s*(?:No Extra\s+)?(?:Maintenance\s+)?Rent',
        r'(\d[\d,]+)\s*/\s*(?:month|mo)',
        r'(?:rent|price)\s*[:\s]*[\u20b9]?\s*([\d,]+)',
    ]:
        m = re.search(pattern, card_text, re.I)
        if m:
            data["rent"] = int(m.group(1).replace(",", ""))
            break
    if data["rent"] == 0:
        numbers = re.findall(r'(\d[\d,]+)', card_text)
        for num_str in numbers:
            num = int(num_str.replace(",", ""))
            if 5000 <= num <= 100000:
                data["rent"] = num
                break

    # Deposit
    dep_match = re.search(r'([\d,]+)\s*Deposit', card_text, re.I)
    if dep_match:
        data["deposit"] = int(dep_match.group(1).replace(",", ""))

    # Sqft
    for pattern in [r'([\d,]+)\s*sq\.?\s*ft', r'([\d,]+)\s*(?:Builtup|Carpet|Super)']:
        m = re.search(pattern, card_text, re.I)
        if m:
            data["sqft"] = int(m.group(1).replace(",", ""))
            break

    # Floor
    floor_match = re.search(r'(\d+)\s*/\s*(\d+)', card_text)
    if floor_match:
        data["floor"] = int(floor_match.group(1))
    if data["floor"] == 0:
        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)\s*(?:floor|of)', card_text, re.I)
        if floor_match:
            data["floor"] = int(floor_match.group(1))

    # Tenant
    ct_lower = card_text.lower()
    if re.search(r'all\s+preferred|bachelor|anyone', ct_lower):
        data["bachelor_verified"] = True
    elif re.search(r'family\s+preferred|family\s+only', ct_lower):
        data["family_only"] = True

    # Furnishing
    if "fully furnished" in ct_lower:
        data["furnishing"] = "Fully Furnished"
    elif "semi" in ct_lower and "furnished" in ct_lower:
        data["furnishing"] = "Semi-Furnished"
    elif "unfurnished" in ct_lower:
        data["furnishing"] = "Unfurnished"

    # Gated
    if "posh society" in ct_lower or "gated" in ct_lower:
        data["gated"] = True

    # Project name
    proj_match = re.search(
        r'(?:\d\s*BHK\s*(?:Apartment|Flat|Villa|House|Independent)\s*(?:In|in|at)\s+)(.+?)(?:\s+for\s+Rent|\s+in\s+)',
        card_text, re.I
    )
    if proj_match:
        data["project"] = proj_match.group(1).strip()

    # Images — use the comprehensive extractor
    card_images = _extract_all_images_from_html(card, source)
    data["images"] = card_images[:5]

    return data


# ═══════════════════════════════════════════════════════════════════════════════
# Generic detail-page verifier
# ═══════════════════════════════════════════════════════════════════════════════

def verify_detail_page(listing):
    """Fetch detail page and extract data. Returns updated listing or None."""
    url = listing["url"]
    source = listing["source"]

    try:
        resp = session.get(url, timeout=15, headers=BROWSER_HEADERS)
        if resp.status_code != 200:
            print(f"    [verify {source}] HTTP {resp.status_code}")
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS:
            if kw in text:
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        rent_match = re.search(r'(?:rent|₹|rs\.?)\s*:?\s*([\d,]+)', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        bhk = listing.get("bhk", "3bhk")
        max_rent = BUDGET.get(bhk, 65000)
        if rent > max_rent:
            return None
        if rent == 0:
            return None

        sqft_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(sqft_match.group(1).replace(",", "")) if sqft_match else 0

        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of\s*\d+)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        bachelor_ok = bool(re.search(r'bachelor|anyone|single\s*men', page_text, re.I))
        if not bachelor_ok and re.search(r'family\s*only|lease\s*type\s*:?\s*family', page_text, re.I):
            return None

        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text or "semi furnished" in text:
            furnishing = "Semi-Furnished"

        images = _extract_all_images_from_html(soup, source)

        # og:image
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            img_url = og_img["content"]
            if img_url.startswith("http") and img_url not in images:
                images.insert(0, img_url)

        locality = listing.get("locality", "")
        for a in AREAS:
            if a in page_text.lower():
                locality = a.title()
                break

        title_tag = soup.find("title")
        project = title_tag.get_text(strip=True).split(" - ")[0].split("|")[0].strip() if title_tag else "Unknown"

        return {
            **listing,
            "rent": rent if rent > 0 else listing.get("rent", 0),
            "sqft": sqft if sqft > 0 else listing.get("sqft", 0),
            "floor": floor if floor > 0 else listing.get("floor", 0),
            "furnishing": furnishing,
            "bachelor_verified": bachelor_ok,
            "project": project, "locality": locality,
            "images": images[:6] if images else listing.get("images", []),
            "active": True, "needs_verification": False,
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

    print(f"  Complete (from API/JSON/cards): {len(complete)}")
    print(f"  Needs verification: {len(needs_verify)}")

    # ── Step 4: Verify incomplete listings (cap at 15) ──
    verified_from_pages = []
    for listing in needs_verify[:15]:
        print(f"  Verifying {listing['source']}: {listing['url'][:75]}...")
        result = verify_detail_page(listing)
        if result:
            verified_from_pages.append(result)
            print(f"    ✅ Verified — ₹{result['rent']}")
        else:
            print(f"    ⚠️ Could not verify")
        time.sleep(0.5)

    all_verified = complete + verified_from_pages
    print(f"\nTotal verified: {len(all_verified)}")

    # ── Step 5: IMAGE ENRICHMENT — fetch images for listings with <3 ──
    low_image_listings = [l for l in all_verified if len(l.get("images", [])) < MIN_IMAGES]
    print(f"\nImage enrichment: {len(low_image_listings)} listings need more images")

    # Sort by source diversity: round-robin so every platform gets enrichment slots
    # Group by source, then interleave
    by_source = {}
    for l in low_image_listings:
        src = l["source"]
        by_source.setdefault(src, []).append(l)
    source_dist = {s: len(v) for s, v in by_source.items()}
    print(f"  Enrichment by source: {source_dist}")

    # Round-robin interleave: take 1 from each source in turn
    interleaved = []
    source_iters = {s: iter(v) for s, v in by_source.items()}
    while source_iters:
        empty_sources = []
        for src in list(source_iters.keys()):
            try:
                interleaved.append(next(source_iters[src]))
            except StopIteration:
                empty_sources.append(src)
        for s in empty_sources:
            del source_iters[s]

    ENRICH_CAP = 80  # increased from 40 to ensure all sources get slots
    enriched_count = 0
    for listing in interleaved[:ENRICH_CAP]:
        old_count = len(listing.get("images", []))
        listing["images"] = _enrich_images_from_url(listing)
        new_count = len(listing.get("images", []))
        if new_count > old_count:
            enriched_count += 1
            print(f"  [enrich] {listing['source']} {listing['id']}: {old_count} → {new_count} images")
        time.sleep(0.3)

    print(f"  Enriched {enriched_count} listings with additional images")

    # ── Step 6: Budget & image filters ──
    qualified = []
    no_images_count = 0
    for listing in all_verified:
        rent = listing.get("rent", 0)
        bhk = listing.get("bhk", "3bhk")
        max_rent = BUDGET.get(bhk, 65000)

        if rent <= 0 or rent > max_rent:
            state["rejected"].append({"id": listing["id"], "reason": "budget"})
            continue

        img_count = len(listing.get("images", []))
        if img_count < MIN_IMAGES:
            no_images_count += 1
            state["rejected"].append({"id": listing["id"], "reason": f"only {img_count} images (need {MIN_IMAGES})"})
            continue

        listing["score"] = score_listing(listing)
        qualified.append(listing)

    qualified.sort(key=lambda x: x["score"], reverse=True)

    # Select top picks with source diversity: ensure each source gets representation
    top_picks = []
    picks_by_source = {}
    MAX_PER_SOURCE = 4  # max listings from any single source in top picks
    for listing in qualified:
        src = listing["source"]
        if picks_by_source.get(src, 0) < MAX_PER_SOURCE:
            top_picks.append(listing)
            picks_by_source[src] = picks_by_source.get(src, 0) + 1
        if len(top_picks) >= 8:
            break
    # If we don't have 8 yet, fill with remaining by score
    if len(top_picks) < 8:
        for listing in qualified:
            if listing not in top_picks:
                top_picks.append(listing)
            if len(top_picks) >= 8:
                break

    print(f"Qualified after scoring: {len(qualified)} (skipped {no_images_count} with <{MIN_IMAGES} images)")
    print(f"Sending top {len(top_picks)} to Telegram")

    # Log source breakdown
    source_counts = {}
    for l in qualified:
        src = l["source"]
        source_counts[src] = source_counts.get(src, 0) + 1
    print(f"Source breakdown: {source_counts}")

    # ── Step 7: Send to Telegram ──
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

    # ── Step 8: Save state ──
    state["rejected"] = state["rejected"][-200:]
    save_state(state)
    print(f"\n✅ Done. {len(top_picks)} listings sent to Telegram.\n")


if __name__ == "__main__":
    run()
