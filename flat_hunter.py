#!/usr/bin/env python3
"""
Hyderabad Flat Hunter — Automated rental listing scanner.
Searches NoBroker, MagicBricks, 99acres, Housing.com, SquareYards.
Verifies listings are live & bachelor-friendly, then sends to Telegram.
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote, urljoin

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

STATE_FILE = Path(__file__).parent / "state.json"

AREAS = ["kondapur", "gachibowli", "kokapet"]
BUDGET = {"2bhk": 40000, "3bhk": 65000}
MIN_FLOOR = 5  # prefer floors >= 5
IST = timezone(timedelta(hours=5, minutes=30))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
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
    all_ids = {item.get("id") for item in state.get("sent", [])}
    all_ids |= {item.get("id") for item in state.get("rejected", [])}
    return listing_id in all_ids


# ─── Telegram ─────────────────────────────────────────────────────────────────

def tg_send_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    resp = session.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "parse_mode": "HTML",
        "text": text,
    })
    return resp.json().get("ok", False)


def tg_send_photo(photo_url, caption):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendPhoto"
    resp = session.post(url, json={
        "chat_id": TELEGRAM_CHAT_ID,
        "photo": photo_url,
        "parse_mode": "HTML",
        "caption": caption[:1024],
    })
    return resp.json().get("ok", False)


def tg_send_media_group(images, caption):
    if len(images) < 2:
        if images:
            return tg_send_photo(images[0], caption)
        return tg_send_message(caption)

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMediaGroup"
    media = [{"type": "photo", "media": images[0], "caption": caption[:1024], "parse_mode": "HTML"}]
    for img in images[1:5]:
        media.append({"type": "photo", "media": img})

    resp = session.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "media": media})
    result = resp.json()
    if not result.get("ok"):
        # Fallback to single photo
        return tg_send_photo(images[0], caption)
    return True


# ─── Scoring ──────────────────────────────────────────────────────────────────

def score_listing(listing):
    score = 50  # base

    # Location bonus
    locality = listing.get("locality", "").lower()
    if "kondapur" in locality:
        score += 15
    elif "gachibowli" in locality:
        score += 12
    elif "kokapet" in locality:
        score += 8

    # Budget fit
    rent = listing.get("rent", 0)
    bhk = listing.get("bhk", "3bhk")
    max_budget = BUDGET.get(bhk, 65000)
    if rent <= max_budget * 0.75:
        score += 10
    elif rent <= max_budget * 0.9:
        score += 5

    # Floor bonus
    floor = listing.get("floor", 0)
    if floor >= 15:
        score += 12
    elif floor >= 10:
        score += 8
    elif floor >= 5:
        score += 4

    # Bachelor confirmed
    if listing.get("bachelor_verified"):
        score += 10

    # Gated community
    if listing.get("gated"):
        score += 5

    # Furnished bonus
    furnishing = listing.get("furnishing", "").lower()
    if "fully" in furnishing:
        score += 5
    elif "semi" in furnishing:
        score += 2

    # Photos bonus
    if len(listing.get("images", [])) >= 3:
        score += 3

    return min(score, 100)


# ─── NoBroker Scraper ─────────────────────────────────────────────────────────

def search_nobroker(area, bhk="3bhk"):
    listings = []
    url = f"https://www.nobroker.in/{bhk}-flats-for-rent-in-{area}_hyderabad"
    print(f"  [NoBroker] Fetching {bhk} in {area}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [NoBroker] HTTP {resp.status_code} for {area}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract listing links — NoBroker uses /property/... paths
        links = soup.find_all("a", href=re.compile(r"/property/\d+-bhk-.*?/detail"))
        for link in links:
            href = link.get("href", "")
            if "/detail" in href:
                full_url = urljoin("https://www.nobroker.in", href)
                listing_id = re.search(r"/([a-f0-9]{30,})/detail", href)
                if listing_id:
                    listings.append({
                        "url": full_url,
                        "id": f"nb_{listing_id.group(1)[:20]}",
                        "source": "NoBroker",
                    })

        # Also try extracting from script tags (JSON data)
        for script in soup.find_all("script"):
            text = script.string or ""
            # Look for property IDs in JSON
            ids = re.findall(r'"propertyId"\s*:\s*"([a-f0-9]{20,})"', text)
            for pid in ids:
                detail_url = f"https://www.nobroker.in/property/{bhk}-apartment-for-rent-in-{area}-hyderabad/{pid}/detail"
                lid = f"nb_{pid[:20]}"
                if not any(l["id"] == lid for l in listings):
                    listings.append({
                        "url": detail_url,
                        "id": lid,
                        "source": "NoBroker",
                    })

    except Exception as e:
        print(f"  [NoBroker] Error: {e}")

    print(f"  [NoBroker] Found {len(listings)} raw listings in {area}")
    return listings[:15]  # cap to avoid too many verifications


def verify_nobroker(listing_url):
    """Fetch NoBroker detail page and extract verified listing data."""
    try:
        resp = session.get(listing_url, timeout=15)
        if resp.status_code != 200:
            return None

        text = resp.text.lower()
        # Check if listing is inactive
        for kw in REJECT_KEYWORDS:
            if kw in text:
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        # Extract rent
        rent_match = re.search(r'(?:rent|₹|rs\.?)\s*:?\s*([\d,]+)', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        # Extract area
        area_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(area_match.group(1).replace(",", "")) if area_match else 0

        # Extract floor
        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of\s*\d+)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        # Check bachelor friendliness
        bachelor_ok = False
        if re.search(r'bachelor|anyone|all|single\s*men', page_text, re.I):
            if not re.search(r'no\s*bachelor|not\s*for\s*bachelor', page_text, re.I):
                bachelor_ok = True

        # Check if family only
        family_only = bool(re.search(r'lease\s*type\s*:?\s*family(?!\s*[,/&])', page_text, re.I))
        if family_only and not bachelor_ok:
            return None

        # Extract project name
        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        project_match = re.search(r'in\s+(.+?)(?:,|\s+for\s+)', title, re.I)
        project = project_match.group(1).strip() if project_match else "Unknown Project"

        # Extract furnishing
        furnishing = "Unfurnished"
        if "fully furnished" in page_text.lower():
            furnishing = "Fully Furnished"
        elif "semi-furnished" in page_text.lower() or "semi furnished" in page_text.lower():
            furnishing = "Semi-Furnished"

        # Extract images
        images = []
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if "assets.nobroker.in" in src and ("large" in src or "original" in src):
                if src not in images:
                    images.append(src)
        # Also check meta og:image
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            img_url = og_img["content"]
            if img_url not in images:
                images.insert(0, img_url)

        # Extract deposit
        deposit_match = re.search(r'deposit\s*:?\s*₹?\s*([\d,]+)', page_text, re.I)
        deposit = int(deposit_match.group(1).replace(",", "")) if deposit_match else 0

        # Extract locality
        locality = ""
        for area in ["kondapur", "gachibowli", "kokapet"]:
            if area in page_text.lower():
                locality = area.title()
                break

        return {
            "rent": rent,
            "sqft": sqft,
            "floor": floor,
            "furnishing": furnishing,
            "bachelor_verified": bachelor_ok,
            "project": project,
            "locality": locality,
            "images": images[:5],
            "deposit": deposit,
            "url": listing_url,
            "source": "NoBroker",
            "active": True,
        }
    except Exception as e:
        print(f"  [NoBroker] Verify error: {e}")
        return None


# ─── 99acres Scraper ──────────────────────────────────────────────────────────

def search_99acres(area, bhk="3-bhk"):
    listings = []
    url = f"https://www.99acres.com/{bhk}-flats-for-rent-in-{area}-hyderabad-ffid"
    print(f"  [99acres] Fetching {bhk} in {area}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [99acres] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        # 99acres listing links pattern
        for link in soup.find_all("a", href=True):
            href = link["href"]
            # Individual listing URLs contain numeric IDs
            match = re.search(r'/(\d{8,})', href)
            if match and "rent" in href.lower():
                full_url = urljoin("https://www.99acres.com", href)
                lid = f"99a_{match.group(1)}"
                if not any(l["id"] == lid for l in listings):
                    listings.append({
                        "url": full_url,
                        "id": lid,
                        "source": "99acres",
                    })

    except Exception as e:
        print(f"  [99acres] Error: {e}")

    print(f"  [99acres] Found {len(listings)} raw listings in {area}")
    return listings[:10]


def verify_99acres(listing_url):
    """Fetch 99acres detail page and verify listing."""
    try:
        resp = session.get(listing_url, timeout=15)
        if resp.status_code != 200:
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS:
            if kw in text:
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        rent_match = re.search(r'(?:rent|₹)\s*:?\s*([\d,]+)\s*(?:/\s*month|per\s*month)?', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        area_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(area_match.group(1).replace(",", "")) if area_match else 0

        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        bachelor_ok = bool(re.search(r'bachelor|anyone|single\s*men', page_text, re.I))
        family_only = bool(re.search(r'family\s*only|families\s*only', page_text, re.I))
        if family_only and not bachelor_ok:
            return None

        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text or "semi furnished" in text:
            furnishing = "Semi-Furnished"

        images = []
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if ("imagecdn.99acres" in src or "99acres" in src) and (".jpg" in src or ".png" in src or ".webp" in src):
                if src not in images and "logo" not in src.lower():
                    images.append(src)

        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            img_url = og_img["content"]
            if img_url not in images:
                images.insert(0, img_url)

        locality = ""
        for area_name in ["kondapur", "gachibowli", "kokapet"]:
            if area_name in page_text.lower():
                locality = area_name.title()
                break

        title_tag = soup.find("title")
        title = title_tag.get_text(strip=True) if title_tag else ""
        project_match = re.search(r'in\s+(.+?)(?:,|\s+for|\s+rent)', title, re.I)
        project = project_match.group(1).strip() if project_match else "Unknown"

        return {
            "rent": rent,
            "sqft": sqft,
            "floor": floor,
            "furnishing": furnishing,
            "bachelor_verified": bachelor_ok,
            "project": project,
            "locality": locality,
            "images": images[:5],
            "deposit": 0,
            "url": listing_url,
            "source": "99acres",
            "active": True,
        }
    except Exception as e:
        print(f"  [99acres] Verify error: {e}")
        return None


# ─── MagicBricks Scraper ─────────────────────────────────────────────────────

def search_magicbricks(area, bhk="3-bhk"):
    listings = []
    url = f"https://www.magicbricks.com/{bhk}-flats-for-rent-in-{area}-hyderabad-pppfs"
    print(f"  [MagicBricks] Fetching {bhk} in {area}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [MagicBricks] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "propertyDetails" in href or re.search(r'/\d{10,}', href):
                full_url = urljoin("https://www.magicbricks.com", href)
                id_match = re.search(r'(\d{10,})', href)
                if id_match:
                    lid = f"mb_{id_match.group(1)}"
                    if not any(l["id"] == lid for l in listings):
                        listings.append({
                            "url": full_url,
                            "id": lid,
                            "source": "MagicBricks",
                        })

        # Also check JSON-LD or data attributes
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if isinstance(data, list):
                    for item in data:
                        if item.get("url"):
                            listings.append({
                                "url": item["url"],
                                "id": f"mb_{hash(item['url']) % 10**10}",
                                "source": "MagicBricks",
                            })
            except (json.JSONDecodeError, TypeError):
                pass

    except Exception as e:
        print(f"  [MagicBricks] Error: {e}")

    print(f"  [MagicBricks] Found {len(listings)} raw listings in {area}")
    return listings[:10]


def verify_magicbricks(listing_url):
    """Verify MagicBricks listing."""
    try:
        resp = session.get(listing_url, timeout=15)
        if resp.status_code != 200:
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS:
            if kw in text:
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        rent_match = re.search(r'(?:rent|₹)\s*:?\s*([\d,]+)', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        area_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(area_match.group(1).replace(",", "")) if area_match else 0

        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        bachelor_ok = bool(re.search(r'bachelor|anyone|single|all', page_text, re.I))
        family_only = bool(re.search(r'family\s*only', page_text, re.I))
        if family_only and not bachelor_ok:
            return None

        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text:
            furnishing = "Semi-Furnished"

        images = []
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if "staticmb.com" in src and (".jpg" in src or ".png" in src or ".webp" in src):
                if src not in images:
                    images.append(src)
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            images.insert(0, og_img["content"])

        locality = ""
        for area_name in ["kondapur", "gachibowli", "kokapet"]:
            if area_name in page_text.lower():
                locality = area_name.title()
                break

        title_tag = soup.find("title")
        project = title_tag.get_text(strip=True).split(" - ")[0] if title_tag else "Unknown"

        return {
            "rent": rent, "sqft": sqft, "floor": floor, "furnishing": furnishing,
            "bachelor_verified": bachelor_ok, "project": project, "locality": locality,
            "images": images[:5], "deposit": 0, "url": listing_url,
            "source": "MagicBricks", "active": True,
        }
    except Exception as e:
        print(f"  [MagicBricks] Verify error: {e}")
        return None


# ─── Housing.com Scraper ─────────────────────────────────────────────────────

def search_housing(area, bhk="3-bhk"):
    listings = []
    url = f"https://housing.com/rent/{bhk}-in-{area}-hyderabad"
    print(f"  [Housing.com] Fetching {bhk} in {area}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [Housing.com] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/rent/" in href and re.search(r'/(\d{8,})', href):
                full_url = urljoin("https://housing.com", href)
                id_match = re.search(r'/(\d{8,})', href)
                if id_match:
                    lid = f"hc_{id_match.group(1)}"
                    if not any(l["id"] == lid for l in listings):
                        listings.append({
                            "url": full_url,
                            "id": lid,
                            "source": "Housing.com",
                        })

    except Exception as e:
        print(f"  [Housing.com] Error: {e}")

    print(f"  [Housing.com] Found {len(listings)} raw listings in {area}")
    return listings[:10]


def verify_housing(listing_url):
    """Verify Housing.com listing."""
    try:
        resp = session.get(listing_url, timeout=15)
        if resp.status_code != 200:
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS:
            if kw in text:
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        rent_match = re.search(r'(?:rent|₹)\s*:?\s*([\d,]+)', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        area_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(area_match.group(1).replace(",", "")) if area_match else 0

        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        bachelor_ok = bool(re.search(r'bachelor|anyone|single|all', page_text, re.I))
        family_only = bool(re.search(r'family\s*only', page_text, re.I))
        if family_only and not bachelor_ok:
            return None

        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text:
            furnishing = "Semi-Furnished"

        images = []
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if "im.housing.com" in src or "housing.com" in src:
                if (".jpg" in src or ".png" in src or ".webp" in src) and "logo" not in src:
                    if src not in images:
                        images.append(src)
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            images.insert(0, og_img["content"])

        locality = ""
        for area_name in ["kondapur", "gachibowli", "kokapet"]:
            if area_name in page_text.lower():
                locality = area_name.title()
                break

        title_tag = soup.find("title")
        project = title_tag.get_text(strip=True).split("|")[0].strip() if title_tag else "Unknown"

        return {
            "rent": rent, "sqft": sqft, "floor": floor, "furnishing": furnishing,
            "bachelor_verified": bachelor_ok, "project": project, "locality": locality,
            "images": images[:5], "deposit": 0, "url": listing_url,
            "source": "Housing.com", "active": True,
        }
    except Exception as e:
        print(f"  [Housing.com] Verify error: {e}")
        return None


# ─── SquareYards Scraper ──────────────────────────────────────────────────────

def search_squareyards(area, bhk="3-bhk"):
    listings = []
    url = f"https://www.squareyards.com/rent/{bhk}-for-rent-in-{area}-hyderabad"
    print(f"  [SquareYards] Fetching {bhk} in {area}...")

    try:
        resp = session.get(url, timeout=15)
        if resp.status_code != 200:
            print(f"  [SquareYards] HTTP {resp.status_code}")
            return listings

        soup = BeautifulSoup(resp.text, "html.parser")

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "rental-" in href and re.search(r'/(\d{6,})', href):
                full_url = urljoin("https://www.squareyards.com", href)
                id_match = re.search(r'/(\d{6,})', href)
                if id_match:
                    lid = f"sy_{id_match.group(1)}"
                    if not any(l["id"] == lid for l in listings):
                        listings.append({
                            "url": full_url,
                            "id": lid,
                            "source": "SquareYards",
                        })

    except Exception as e:
        print(f"  [SquareYards] Error: {e}")

    print(f"  [SquareYards] Found {len(listings)} raw listings in {area}")
    return listings[:10]


def verify_squareyards(listing_url):
    """Verify SquareYards listing — be extra careful, many are stale."""
    try:
        resp = session.get(listing_url, timeout=15)
        if resp.status_code != 200:
            return None

        text = resp.text.lower()
        for kw in REJECT_KEYWORDS + ["sold", "rented out", "currently unavailable"]:
            if kw in text:
                return None

        soup = BeautifulSoup(resp.text, "html.parser")
        page_text = soup.get_text(" ", strip=True)

        rent_match = re.search(r'(?:rent|₹)\s*:?\s*([\d,]+)\s*(?:/\s*month)?', page_text, re.I)
        rent = int(rent_match.group(1).replace(",", "")) if rent_match else 0

        area_match = re.search(r'([\d,]+)\s*sq\.?\s*ft', page_text, re.I)
        sqft = int(area_match.group(1).replace(",", "")) if area_match else 0

        floor_match = re.search(r'(\d+)\s*(?:th|st|nd|rd)?\s*(?:floor|of)', page_text, re.I)
        floor = int(floor_match.group(1)) if floor_match else 0

        bachelor_ok = bool(re.search(r'bachelor|anyone|single\s*men', page_text, re.I))
        family_only = bool(re.search(r'family\s*only|families\s*only', page_text, re.I))
        if family_only and not bachelor_ok:
            return None

        furnishing = "Unfurnished"
        if "fully furnished" in text:
            furnishing = "Fully Furnished"
        elif "semi-furnished" in text or "semi furnished" in text:
            furnishing = "Semi-Furnished"

        images = []
        for img in soup.find_all("img"):
            src = img.get("src", "") or img.get("data-src", "")
            if "img.squareyards.com" in src and (".jpg" in src or ".png" in src or ".webp" in src):
                clean_src = src.split("?")[0]
                if clean_src not in images:
                    images.append(clean_src)
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            images.insert(0, og_img["content"])

        locality = ""
        for area_name in ["kondapur", "gachibowli", "kokapet"]:
            if area_name in page_text.lower():
                locality = area_name.title()
                break

        title_tag = soup.find("title")
        project = title_tag.get_text(strip=True).split(" in ")[0].strip() if title_tag else "Unknown"

        return {
            "rent": rent, "sqft": sqft, "floor": floor, "furnishing": furnishing,
            "bachelor_verified": bachelor_ok, "project": project, "locality": locality,
            "images": images[:5], "deposit": 0, "url": listing_url,
            "source": "SquareYards", "active": True,
        }
    except Exception as e:
        print(f"  [SquareYards] Verify error: {e}")
        return None


# ─── Main Pipeline ────────────────────────────────────────────────────────────

PLATFORM_SEARCH = {
    "NoBroker": (search_nobroker, verify_nobroker, {"3bhk": "3bhk", "2bhk": "2bhk"}),
    "99acres": (search_99acres, verify_99acres, {"3bhk": "3-bhk", "2bhk": "2-bhk"}),
    "MagicBricks": (search_magicbricks, verify_magicbricks, {"3bhk": "3-bhk", "2bhk": "2-bhk"}),
    "Housing.com": (search_housing, verify_housing, {"3bhk": "3-bhk", "2bhk": "2-bhk"}),
    "SquareYards": (search_squareyards, verify_squareyards, {"3bhk": "3-bhk", "2bhk": "2-bhk"}),
}


def run():
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set as environment variables.")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Flat Hunter — {datetime.now(IST).strftime('%d %b %Y %I:%M %p IST')}")
    print(f"{'='*60}\n")

    state = load_state()
    raw_listings = []

    # Step 1: Search all platforms
    for platform_name, (search_fn, verify_fn, bhk_map) in PLATFORM_SEARCH.items():
        print(f"\n[{platform_name}]")
        for area in AREAS:
            # 3BHK search
            results = search_fn(area, bhk_map["3bhk"])
            for r in results:
                r["bhk"] = "3bhk"
            raw_listings.extend(results)
            time.sleep(1)  # rate limit

            # 2BHK search
            results = search_fn(area, bhk_map["2bhk"])
            for r in results:
                r["bhk"] = "2bhk"
            raw_listings.extend(results)
            time.sleep(1)

    print(f"\n--- Total raw listings found: {len(raw_listings)} ---\n")

    # Step 2: Remove already processed
    new_listings = [l for l in raw_listings if not is_already_processed(state, l["id"])]
    print(f"--- After removing already processed: {len(new_listings)} ---\n")

    # Step 3: Verify each listing
    verified = []
    verify_fns = {
        "NoBroker": verify_nobroker,
        "99acres": verify_99acres,
        "MagicBricks": verify_magicbricks,
        "Housing.com": verify_housing,
        "SquareYards": verify_squareyards,
    }

    for listing in new_listings[:30]:  # cap at 30 verifications per run
        print(f"  Verifying {listing['source']}: {listing['url'][:80]}...")
        verify_fn = verify_fns.get(listing["source"])
        if not verify_fn:
            continue

        result = verify_fn(listing["url"])
        if result:
            result["id"] = listing["id"]
            result["bhk"] = listing.get("bhk", "3bhk")

            # Budget check
            max_rent = BUDGET.get(result["bhk"], 65000)
            if result["rent"] > max_rent or result["rent"] == 0:
                state["rejected"].append({"id": listing["id"], "reason": "over budget or no rent"})
                continue

            result["score"] = score_listing(result)
            if result["score"] >= 70:
                verified.append(result)
                print(f"    ✅ VERIFIED — Score {result['score']}, ₹{result['rent']}, {result['source']}")
            else:
                state["rejected"].append({"id": listing["id"], "reason": f"low score ({result['score']})"})
        else:
            state["rejected"].append({"id": listing["id"], "reason": "failed verification"})
            print(f"    ❌ Failed verification")

        time.sleep(0.5)

    # Step 4: Sort by score
    verified.sort(key=lambda x: x["score"], reverse=True)
    top_picks = verified[:8]  # send max 8

    print(f"\n--- Verified & qualified: {len(verified)} (sending top {len(top_picks)}) ---\n")

    # Step 5: Send to Telegram
    now = datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST")

    if top_picks:
        sources = list(set(p["source"] for p in top_picks))
        tg_send_message(
            f"🏠 <b>Flat Hunt Auto-Update</b>\n\n"
            f"📅 {now}\n"
            f"✅ <b>{len(top_picks)} new VERIFIED listings found</b>\n"
            f"📡 Sources: {', '.join(sources)}"
        )
        time.sleep(1)

        for i, listing in enumerate(top_picks, 1):
            caption = (
                f"#{i} <b>{listing['bhk'].upper()} in {listing['locality'] or 'Hyderabad'}"
                f" — ₹{listing['rent']:,} — Score {listing['score']}/100</b>\n"
                f"📐 {listing['sqft']} sq.ft. | Floor {listing['floor']} | {listing['furnishing']}\n"
                f"👤 Bachelor: {'✅ Verified' if listing['bachelor_verified'] else '⚠️ Verify'}\n"
                f"🏢 {listing['project']}\n"
                f"📡 Source: {listing['source']}\n"
                f"🔗 {listing['url']}"
            )
            if listing.get("deposit"):
                caption = caption.replace(
                    f"📡 Source:",
                    f"💰 Deposit: ₹{listing['deposit']:,}\n📡 Source:"
                )

            images = listing.get("images", [])
            if images:
                tg_send_media_group(images, caption)
            else:
                tg_send_message(caption)

            # Track as sent
            state["sent"].append({
                "id": listing["id"],
                "project": listing["project"],
                "rent": listing["rent"],
                "bhk": listing["bhk"],
                "locality": listing["locality"],
                "source": listing["source"],
                "url": listing["url"],
                "sent_at": now,
            })
            time.sleep(1)

        tg_send_message(
            f"🏆 <b>Best pick this scan:</b> #{1} — {top_picks[0]['project']}, "
            f"{top_picks[0]['locality']} at ₹{top_picks[0]['rent']:,}/month "
            f"(Score {top_picks[0]['score']})\n\n"
            f"⏰ Next scan in 6 hours."
        )
    else:
        tg_send_message(
            f"🏠 <b>Flat Hunt Auto-Scan — {now}</b>\n\n"
            f"No new verified bachelor-friendly listings found.\n"
            f"📡 Scanned: NoBroker, MagicBricks, 99acres, Housing.com, SquareYards\n"
            f"📍 Areas: Kondapur, Gachibowli, Kokapet\n"
            f"⏰ Will check again in 6 hours."
        )

    # Step 6: Save state
    # Keep only last 200 rejected to avoid file bloat
    state["rejected"] = state["rejected"][-200:]
    save_state(state)
    print(f"\n✅ Run complete. State saved. {len(top_picks)} listings sent to Telegram.")


if __name__ == "__main__":
    run()
