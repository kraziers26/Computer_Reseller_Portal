"""
services/query_parser.py

Natural language query parser for the Deal Blaster.
Tuned to iGamer Corp's real product vocabulary from order data.

Handles descriptions like:
  "ASUS gaming laptop AMD Ryzen 9 32GB RTX 5070 under $1200"
  "MacBook Air M5 16GB 512GB"
  "HP gaming desktop Intel i7 RTX 5060 16GB"
  "Dell all in one 27 inch Intel Core Ultra 7 16GB"
  "Lenovo laptop Intel Core i7 16GB 1TB under 900"
"""

import re


# ── Known values tuned to real order data ────────────────────────────────────

BRANDS = [
    'Apple', 'ASUS', 'Asus', 'Acer', 'Dell', 'HP', 'Lenovo',
    'MSI', 'Microsoft', 'Samsung', 'LG', 'Razer', 'Gigabyte', 'Alienware',
]

# Maps user-friendly category phrases → BB API category slugs
CATEGORY_PATTERNS = [
    # Gaming laptop — check before generic laptop
    (r'gaming\s+laptop|laptop\s+gaming|rog|tuf\s+|nitro\s+|predator|omen\s+laptop|strix', 'gaming'),
    # MacBook
    (r'macbook|mac\s*book|apple\s+laptop|m[1-5]\s+chip|a1[5-9]\s+pro', 'macbook'),
    # All-in-one
    (r'all[\s\-]*in[\s\-]*one|aio|omnidesk|omni\s+desktop|imac', 'aio'),
    # Gaming desktop — check before generic desktop
    (r'gaming\s+desktop|desktop\s+gaming|omen\s+desktop|alienware|aurora|cordex', 'desktop'),
    # Regular desktop
    (r'\bdesktop\b|\btower\b|\bpc\b(?!\s*gaming)', 'desktop'),
    # Monitor
    (r'\bmonitor\b|\bdisplay\b|\bscreen\b(?!\s*size)', 'monitor'),
    # Generic laptop last
    (r'\blaptop\b|\bnotebook\b|\bvivobook\b|\bzenbook\b|\bzenbook\b|\bideapad\b|\bvivobook\b|\bsurface\b|\bspectre\b|\benvy\b', 'laptop'),
]

# CPU keyword → BB API search term
CPU_PATTERNS = [
    # Apple chips
    (r'apple\s+m5|m5\s+chip',              'Apple M5'),
    (r'apple\s+m4|m4\s+chip',              'Apple M4'),
    (r'apple\s+m3|m3\s+chip',              'Apple M3'),
    (r'apple\s+m2|m2\s+chip',              'Apple M2'),
    (r'apple\s+m1|m1\s+chip',              'Apple M1'),
    (r'apple\s+[am]\d|a1[5-9]\s+pro',      'Apple M'),
    # Intel Ultra
    (r'intel\s+(?:core\s+)?ultra\s+9',     'Intel Core Ultra 9'),
    (r'intel\s+(?:core\s+)?ultra\s+7',     'Intel Core Ultra 7'),
    (r'intel\s+(?:core\s+)?ultra\s+5',     'Intel Core Ultra 5'),
    (r'intel\s+ultra',                      'Intel Core Ultra'),
    # Intel Core numbered
    (r'intel\s+(?:core\s+)?i9|core\s+i9',  'Intel i9'),
    (r'intel\s+(?:core\s+)?i7|core\s+i7',  'Intel i7'),
    (r'intel\s+(?:core\s+)?i5|core\s+i5',  'Intel i5'),
    (r'intel\s+core\s+9',                   'Intel Core 9'),
    (r'intel\s+core\s+7',                   'Intel Core 7'),
    (r'intel\s+core\s+5',                   'Intel Core 5'),
    (r'\bintel\b',                           'Intel'),
    # AMD
    (r'amd\s+ryzen\s+9|ryzen\s+9',         'AMD Ryzen 9'),
    (r'amd\s+ryzen\s+7|ryzen\s+7',         'AMD Ryzen 7'),
    (r'amd\s+ryzen\s+5|ryzen\s+5',         'AMD Ryzen 5'),
    (r'\bamd\b|\bryzen\b',                  'AMD'),
    # Snapdragon
    (r'snapdragon\s+x\s+elite',            'Snapdragon X Elite'),
    (r'snapdragon\s+x\s+plus',             'Snapdragon X Plus'),
    (r'snapdragon',                         'Snapdragon'),
]

# GPU keyword → search hint (used in keyword search, not BB API filter)
GPU_PATTERNS = [
    (r'rtx\s*5090',  'RTX 5090'),
    (r'rtx\s*5080',  'RTX 5080'),
    (r'rtx\s*5070\s*ti', 'RTX 5070 Ti'),
    (r'rtx\s*5070',  'RTX 5070'),
    (r'rtx\s*5060\s*ti', 'RTX 5060 Ti'),
    (r'rtx\s*5060',  'RTX 5060'),
    (r'rtx\s*4090',  'RTX 4090'),
    (r'rtx\s*4080',  'RTX 4080'),
    (r'rtx\s*4070\s*ti', 'RTX 4070 Ti'),
    (r'rtx\s*4070',  'RTX 4070'),
    (r'rtx\s*4060\s*ti', 'RTX 4060 Ti'),
    (r'rtx\s*4060',  'RTX 4060'),
    (r'rtx\s*3080',  'RTX 3080'),
    (r'rtx\s*3070',  'RTX 3070'),
    (r'rtx\s*3060',  'RTX 3060'),
    (r'gtx\s*1650',  'GTX 1650'),
    (r'gtx\s*1660',  'GTX 1660'),
    (r'\brtx\b',     'RTX'),
    (r'\bgtx\b',     'GTX'),
    (r'nvidia',      'NVIDIA'),
]

# RAM normalizer — maps various expressions to standard values
RAM_PATTERNS = [
    (r'64\s*gb(?:\s+ram)?',   '64GB'),
    (r'32\s*gb(?:\s+ram)?',   '32GB'),
    (r'18\s*gb(?:\s+ram)?',   '18GB'),
    (r'16\s*gb(?:\s+ram)?',   '16GB'),
    (r'8\s*gb(?:\s+ram)?',    '8GB'),
    # "32 gigs", "32 gig"
    (r'32\s*gigs?',           '32GB'),
    (r'16\s*gigs?',           '16GB'),
    (r'8\s*gigs?',            '8GB'),
    # "at least 16GB", "minimum 16GB"
    (r'(?:at\s+least|min(?:imum)?|more\s+than)\s+32',  '32GB'),
    (r'(?:at\s+least|min(?:imum)?|more\s+than)\s+16',  '16GB'),
    (r'(?:at\s+least|min(?:imum)?|more\s+than)\s+8',   '8GB'),
]

# Price extraction patterns
PRICE_PATTERNS = [
    # "under $900", "below $1200", "less than $1500", "max $800"
    r'(?:under|below|less\s+than|max(?:imum)?|no\s+more\s+than|up\s+to)\s*\$?\s*(\d[\d,]*)',
    # "$900 or less", "$1000 max"
    r'\$\s*(\d[\d,]*)\s*(?:or\s+less|max)',
    # "budget $900", "budget of $900"
    r'budget\s+(?:of\s+)?\$?\s*(\d[\d,]*)',
    # "around $900" — treat as max with 10% buffer
    r'around\s+\$?\s*(\d[\d,]*)',
    # bare "under 900" without $
    r'under\s+(\d{3,4})\b',
]


# ── Main parser ───────────────────────────────────────────────────────────────

def parse_query(query: str) -> dict:
    """
    Parse a natural language product description into BB API filter params.

    Returns a filters dict compatible with run_scan():
    {
        "brands":     ["ASUS"],
        "categories": ["gaming"],
        "price_max":  1200.0,
        "cpu":        ["AMD Ryzen 9"],
        "ram":        ["32GB"],
        "min_score":  7,
        "_parsed": {
            "gpu": "RTX 5070",
            "original_query": "...",
            "confidence": "high"
        }
    }
    """
    if not query or not query.strip():
        return {}

    q     = query.strip()
    q_low = q.lower()
    filters = {}
    meta    = {"original_query": q}

    # ── Brand ─────────────────────────────────────────────────────────────────
    found_brands = []
    for brand in BRANDS:
        if brand.lower() in q_low:
            # Normalize ASUS/Asus → ASUS
            canonical = 'ASUS' if brand.lower() == 'asus' else brand
            if canonical not in found_brands:
                found_brands.append(canonical)
    if found_brands:
        filters['brands'] = found_brands

    # ── Category ──────────────────────────────────────────────────────────────
    for pattern, slug in CATEGORY_PATTERNS:
        if re.search(pattern, q_low):
            filters['categories'] = [slug]
            meta['category_match'] = pattern
            break

    # ── CPU ───────────────────────────────────────────────────────────────────
    found_cpus = []
    for pattern, label in CPU_PATTERNS:
        if re.search(pattern, q_low):
            found_cpus.append(label)
            break  # take the most specific match
    if found_cpus:
        filters['cpu'] = found_cpus

    # ── GPU (stored in meta for keyword search enhancement) ───────────────────
    for pattern, label in GPU_PATTERNS:
        if re.search(pattern, q_low):
            meta['gpu'] = label
            break

    # ── RAM ───────────────────────────────────────────────────────────────────
    for pattern, value in RAM_PATTERNS:
        if re.search(pattern, q_low):
            filters['ram'] = [value]
            break

    # ── Price ─────────────────────────────────────────────────────────────────
    for pattern in PRICE_PATTERNS:
        m = re.search(pattern, q_low)
        if m:
            raw = m.group(1).replace(',', '')
            price = float(raw)
            # "around" → add 15% buffer
            if 'around' in pattern:
                price = price * 1.15
            filters['price_max'] = price
            break

    # ── Default min score ─────────────────────────────────────────────────────
    # Lower threshold for targeted searches so we get more results
    filters['min_score'] = 5

    # ── Confidence assessment ─────────────────────────────────────────────────
    signals = sum([
        'brands'     in filters,
        'categories' in filters,
        'cpu'        in filters,
        'ram'        in filters,
        'price_max'  in filters,
    ])
    meta['confidence'] = 'high' if signals >= 3 else 'medium' if signals >= 2 else 'low'
    meta['signals_found'] = signals
    filters['_parsed'] = meta

    return filters


def describe_filters(filters: dict) -> str:
    """
    Returns a human-readable summary of what was parsed.
    Used to show the user what the search understood.
    """
    meta  = filters.get('_parsed', {})
    parts = []

    if filters.get('brands'):
        parts.append(f"Brand: {', '.join(filters['brands'])}")
    if filters.get('categories'):
        cat_map = {
            'gaming': 'Gaming Laptops', 'macbook': 'MacBooks',
            'laptop': 'Laptops', 'desktop': 'Desktops',
            'monitor': 'Monitors', 'aio': 'All-in-One PCs'
        }
        cats = [cat_map.get(c, c) for c in filters['categories']]
        parts.append(f"Category: {', '.join(cats)}")
    if filters.get('cpu'):
        parts.append(f"Processor: {', '.join(filters['cpu'])}")
    if meta.get('gpu'):
        parts.append(f"GPU: {meta['gpu']}")
    if filters.get('ram'):
        parts.append(f"RAM: {', '.join(filters['ram'])}")
    if filters.get('price_max'):
        parts.append(f"Max price: ${filters['price_max']:,.0f}")

    if not parts:
        return "Searching all deals (no specific filters detected)"

    return "Searching for: " + " · ".join(parts)


# ── Quick self-test ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    tests = [
        "ASUS gaming laptop AMD Ryzen 9 32GB RTX 5070 under $1200",
        "MacBook Air M5 16GB 512GB",
        "HP gaming desktop Intel i7 RTX 5060 16GB under $900",
        "Dell all in one 27 inch Intel Core Ultra 7 16GB",
        "Acer Nitro gaming laptop Intel Core 9 32GB RTX 5070",
        "Intel Core i9 laptop 16GB 1TB under 1500",
        "Lenovo laptop 16GB Snapdragon",
        "MacBook Pro M3 32GB",
        "gaming laptop with RTX 4060 at least 16GB RAM under $800",
        "ASUS ROG 32GB Ryzen 9 RTX 5070Ti",
    ]
    for t in tests:
        f = parse_query(t)
        print(f"\nQ: {t}")
        print(f"  → {describe_filters(f)}")
        print(f"  → filters: brands={f.get('brands')} cat={f.get('categories')} cpu={f.get('cpu')} ram={f.get('ram')} price_max={f.get('price_max')} gpu={f.get('_parsed',{}).get('gpu')} confidence={f.get('_parsed',{}).get('confidence')}")
