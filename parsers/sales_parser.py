"""
Sales Tracker PDF parser — parses Sunrise International Group Corp invoices
issued to Tier 2 resale clients.

Uses pdfplumber's table-mode extraction (not raw text-stream order) because
table mode correctly preserves visual column order, while item codes are
pulled from the description text rather than the Item Code column since
long codes get truncated there (e.g. "DECT1250-7274B...").

Two table layouts have been observed in the wild:
  1. One table row per line item (the original format).
  2. All line items on a page collapsed into a SINGLE table row, with each
     column (qty/code/description/price/amount) holding every value for
     that page joined by newlines. Both are handled below.
"""
import re
import pdfplumber

NON_ITEM_MARKERS = ('out-of-state', 'out-o', 'procesado', 'total', 'no commercial value')

SPEC_PATTERNS = {
    'ram_gb':    re.compile(r'(\d+)\s*GB\s*(?:GB\s*)?RAM', re.I),
    'storage_gb': re.compile(r'(\d+)\s*(TB|GB)\s*(?:Solid State|SSD|Hard Drive|SHard)', re.I),
    'gpu':       re.compile(r'(NVIDIA[®™]?\s*(?:GeForce\s*)?RTX[™]?\s*\d{4}\s*\w*|GeForce\s*(?:GTX|MX)\s*\w+|RTX\s*\d{4}\s*\w*)', re.I),
    'cpu':       re.compile(r'(Intel\s*Core[®™]?\s*(?:Ultra\s*)?\w*\s*[\w-]+|AMD\s*Ryzen[™]?\s*(?:AI\s*)?\d?\s*[\w-]+|Snapdragon[®]?\s*\w*)', re.I),
    'screen':    re.compile(r'(\d{2}(?:\.\d)?)["\u201d]', re.I),
    'os':        re.compile(r'(Windows\s*\d+|Windws\s*\d+)', re.I),
}

# (cid:NN) codes are PDF-internal glyph indices from subsetted fonts and
# aren't a stable universal mapping — these are reverse-engineered from
# specific invoices by cross-referencing against the same text rendered
# cleanly elsewhere. Unknown cid codes fall back to being stripped rather
# than left visible as literal "(cid:NN)" junk in the parsed output.
KNOWN_CID_MAP = {
    '102': 'f',   # "Out-o(cid:102)-state" -> "Out-of-state" (ff/fi ligature)
    '36':  '$',
    '37':  '%',
    '89':  'Y',   # confirmed via "82(cid:89)20001US" matching "82Y20001US" elsewhere
}


def _clean_cid(text):
    if not text or '(cid:' not in text:
        return text
    # Two consecutive (cid:39) (single-quote glyph) is used as a double-quote
    # / inch-mark substitute in some invoices, e.g. 14(cid:39)(cid:39) QHD.
    text = re.sub(r'(\(cid:39\)){2}', '"', text)

    def repl(m):
        return KNOWN_CID_MAP.get(m.group(1), '')
    return re.sub(r'\(cid:(\d+)\)', repl, text)


def _clean_num(text):
    """Extract a decimal number from text, tolerant of (cid:XX) glyph artifacts."""
    if not text:
        return None
    match = re.search(r'[\d,]+\.\d{2}', text)
    if not match:
        return None
    return float(match.group(0).replace(',', ''))


def _is_item_row(row):
    qty_cell = (row[0] or '').strip()
    if not qty_cell.isdigit():
        return False
    desc = ' '.join(c for c in row if c).lower()
    return not any(marker in desc for marker in NON_ITEM_MARKERS)


def _is_multi_item_row(cells):
    """Detects the collapsed layout: first non-empty cell is several
    newline-separated digit-only lines (one per item on the page)."""
    first = next((c for c in cells if c), '')
    lines = [l.strip() for l in first.split('\n') if l.strip()]
    return len(lines) > 1 and all(l.isdigit() for l in lines)


def _extract_item_code(description):
    # The code is never the first word (that's the brand: "Dell", "Asus"...)
    # and short marketing tokens like "A16" or "G18" can also contain a
    # digit, so brand alone isn't a safe filter. Instead: scan tokens left
    # to right and take the first one that (a) mixes letters and digits,
    # and (b) is at least 6 chars — long enough to rule out marketing
    # tokens ("A16", "G18") while matching real codes ("DECT1250-7274BLK-PUS",
    # "83N30010US", "17-cn5085cl"). Known limitation: a small number of
    # invoices split the code across two tokens with an embedded space
    # (e.g. "AM242TP 1M-839US") — the preview/confirm step catches these.
    flat = description.replace('\n', ' ')
    for token in flat.split():
        token = token.strip(',":')
        if len(token) < 6:
            continue
        if re.search(r'[A-Za-z]', token) and re.search(r'\d', token):
            return token
    return None


def _classify_product_type(description):
    d = description.lower()
    if 'all in one' in d or 'aio' in d:
        return 'aio'
    if 'desktop' in d or 'dektop' in d:
        return 'desktop'
    if 'laptop' in d:
        return 'laptop'
    # Some laptop lines omit the word entirely (e.g. "LDC15255-A117BLK-PUS,
    # AMD Ryzen 7 7730U... 15.6" FHD... Touch Screen"). Desktops never carry
    # a screen-size spec and AIOs always say "all in one" explicitly, so a
    # bare screen-size mention with no other category word is safely a laptop.
    if re.search(r'["\u201d]\s?\d{2}(?:\.\d)?|\d{2}(?:\.\d)?["\u201d]', description):
        return 'laptop'
    return 'other'


def _parse_specs(description):
    flat = description.replace('\n', ' ')
    specs = {}
    for field, pattern in SPEC_PATTERNS.items():
        m = pattern.search(flat)
        if not m:
            continue
        if field == 'ram_gb':
            specs['ram_gb'] = int(m.group(1))
        elif field == 'storage_gb':
            val, unit = int(m.group(1)), m.group(2).upper()
            specs['storage_gb'] = val * 1024 if unit == 'TB' else val
        elif field == 'screen':
            specs['screen'] = m.group(1) + '"'
        else:
            specs[field] = m.group(1).strip()
    return specs


def _build_line_item(code, desc, qty, price_each, amount):
    desc_clean = _clean_cid(desc).replace('\n', ' ').strip()
    code_clean = _clean_cid(code).strip() if code else _extract_item_code(desc_clean)
    is_customs = 'no commercial value' in desc_clean.lower() or (amount == 0)
    return {
        'item_code': code_clean,
        'description': desc_clean,
        'product_type': _classify_product_type(desc_clean),
        'quantity': qty,
        'price_each': price_each,
        'amount': amount,
        'is_customs_only': is_customs,
        **_parse_specs(desc_clean),
    }


def _parse_multi_item_row(cells):
    """Splits a collapsed row (all items on a page merged into one row)
    back into individual line items. Quantity count is the source of truth
    for how many real items exist — trailing tax-rate/tax-amount entries
    that get merged into the price/amount columns are simply beyond that
    count and ignored. Descriptions are one continuous blob; every real
    item's spec list ends in "Windows 11" (optionally "... Home"), which
    is used as the split marker."""
    non_empty = [c for c in cells if c]
    if len(non_empty) < 4:
        return []

    qty_col, code_col, desc_col = non_empty[0], non_empty[1], non_empty[2]
    price_col, amount_col = non_empty[-2], non_empty[-1]

    qtys = [l.strip() for l in qty_col.split('\n') if l.strip()]
    n = len(qtys)
    if n < 1:
        return []

    codes = [l.strip() for l in code_col.split('\n') if l.strip()]
    prices = [l.strip() for l in price_col.split('\n') if l.strip()]
    amounts = [l.strip() for l in amount_col.split('\n') if l.strip()]

    # Split the description blob on its OS-line terminator, keeping the
    # terminator attached to the chunk it closes out.
    parts = re.split(r'(Windows\s*11(?:\s+Home)?)', desc_col)
    descs = []
    i = 0
    while i < len(parts) - 1 and len(descs) < n:
        descs.append((parts[i] + parts[i + 1]).strip())
        i += 2

    items = []
    for idx in range(n):
        if not qtys[idx].isdigit():
            continue
        qty = int(qtys[idx])
        code = codes[idx] if idx < len(codes) else None
        desc = descs[idx] if idx < len(descs) else ''
        price_each = _clean_num(prices[idx]) if idx < len(prices) else None
        amount = _clean_num(amounts[idx]) if idx < len(amounts) else None
        items.append(_build_line_item(code, desc, qty, price_each, amount))
    return items


def _parse_client_block(text):
    """Bill To cell is one multi-line string: name, address line(s), and a
    phone number — either explicitly labeled ("Tel: ...") or, in some
    invoices, a bare line that looks like a phone number with no label."""
    lines = [l.strip() for l in text.split('\n') if l.strip()]
    if not lines:
        return None, None, None
    name = lines[0]
    phone = next((l.split('Tel:')[-1].strip() for l in lines if 'Tel:' in l), None)
    remaining = [l for l in lines[1:] if 'Tel:' not in l]
    if not phone:
        # Bare phone line with no "Tel:" label, e.g. "+30 698 97 08 848".
        phone_like = next((l for l in remaining if re.match(r'^\+?[\d][\d\s().-]{6,}$', l)), None)
        if phone_like:
            phone = phone_like
            remaining = [l for l in remaining if l != phone_like]
    address = ', '.join(remaining) if remaining else None
    return name, address, phone


def _extract_client_from_text(first_page_text):
    """Fallback for invoices where the Bill To/Ship To box isn't a detected
    table (borderless layout) — falls back to plain text extraction. Bill To
    and Ship To are always identical in these invoices and print side by side
    on the same line, duplicating every line's word count evenly, e.g.
    "MG MANAGER COMPUTERS SA MG Manager Computers SA" — so splitting each
    line's tokens exactly in half recovers the single true value."""
    m = re.search(r'Bill To:?\s*Ship To:?\s*\n(.*?)\n(?:P\.O\. Number|Quantity)',
                   first_page_text, re.DOTALL)
    if not m:
        return None, None, None
    lines = [l for l in m.group(1).split('\n') if l.strip()]
    halved = []
    for line in lines:
        tokens = line.split()
        if tokens and len(tokens) % 2 == 0:
            halved.append(' '.join(tokens[:len(tokens) // 2]))
        else:
            halved.append(line.strip())
    return _parse_client_block('\n'.join(halved))


def parse_sales_invoice(pdf_path):
    """Returns a dict: {invoice_number, invoice_date, client_name, client_address,
    client_phone, po_number, terms, rep, total, line_items: [...]}."""
    header = {}
    line_items = []
    expect_client_block = False

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                for row in table:
                    cells = [c.strip() if c else '' for c in row]
                    joined = ' '.join(c for c in cells if c)

                    if not header.get('invoice_number'):
                        if len(cells) >= 2 and re.match(r'\d{1,2}/\d{1,2}/\d{4}', cells[0]) and cells[1].isdigit():
                            header['invoice_date'] = cells[0]
                            header['invoice_number'] = cells[1]

                    stripped = joined.strip(': ')
                    if stripped == 'Bill To':
                        expect_client_block = True
                        continue
                    if expect_client_block:
                        expect_client_block = False
                        if not header.get('client_name'):
                            name, address, phone = _parse_client_block(joined)
                            header['client_name'] = name
                            header['client_address'] = address
                            header['client_phone'] = phone
                        continue

                    if _is_multi_item_row(cells):
                        line_items.extend(_parse_multi_item_row(cells))
                        continue

                    if _is_item_row(cells):
                        qty = int(cells[0])
                        desc = next((c for c in cells[2:-2] if c and len(c) > 15), '')
                        if not desc:
                            desc = next((c for c in cells if c and len(c) > 15), '')
                        code = _extract_item_code(desc)
                        nums = [c for c in cells if _clean_num(c) is not None]
                        price_each = _clean_num(nums[0]) if len(nums) >= 2 else None
                        amount = _clean_num(nums[-1]) if nums else None
                        line_items.append(_build_line_item(code, desc, qty, price_each, amount))

    header['line_items'] = line_items
    header['computed_total'] = round(sum(li['amount'] or 0 for li in line_items), 2)

    if not header.get('client_name'):
        with pdfplumber.open(pdf_path) as pdf:
            text = pdf.pages[0].extract_text() or ''
        name, address, phone = _extract_client_from_text(text)
        header['client_name'] = name
        header['client_address'] = address
        header['client_phone'] = phone

    return header


if __name__ == '__main__':
    import sys, json
    result = parse_sales_invoice(sys.argv[1])
    result_summary = {k: v for k, v in result.items() if k != 'line_items'}
    print(json.dumps(result_summary, indent=2))
    print(f"\n{len(result['line_items'])} line items, computed total: {result['computed_total']}")
    for li in result['line_items']:
        print(f"  {li['quantity']:>3}x {li['item_code']:<22} {li['product_type']:<8} "
              f"${li['amount']:>10,.2f}  customs={li['is_customs_only']}  "
              f"cpu={li.get('cpu','-')}  ram={li.get('ram_gb','-')}  gpu={li.get('gpu','-')}")
