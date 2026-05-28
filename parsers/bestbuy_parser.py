import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import pdfplumber


@dataclass
class LineItem:
    item_description: str
    sku_model_color: str
    quantity: int
    unit_price: float
    line_total: float
    serial: Optional[str] = None


@dataclass
class BestBuyInvoice:
    retailer: str = "Best Buy"
    order_number: Optional[str] = None
    purchase_date: Optional[str] = None
    purchase_year_month: Optional[str] = None
    card_last4: Optional[str] = None
    fulfillment_method: Optional[str] = None
    price_total: Optional[float] = None
    items: list = field(default_factory=list)
    parse_errors: list = field(default_factory=list)
    needs_review: bool = False


# ── Noise / stop patterns ──────────────────────────────────────────────────────

_NOISE_PATS = [
    "Make Pickup Changes", "Ship it Instead", "Add Alternate Pickup",
    "Cancel &", "AppleCare", "Protection for your",
    "What's Included", "Terms & Conditions",
    "Add 4 Year", "Add 3 Year", "Digital Item", "Order Received",
    "Ready to Redeem", "Ready for Pickup", "Included free",
    "Pickup Person", "Store Pickup", "Extend Pickup",
    "Store hours", "In-Store Hours",
    "Best Buy Sweetwater", "Best Buy Support", "Browse our", "Get help",
    "Resend Email", "Redeem Now", "We've emailed", "\u2019ve emailed",
    "Return Options", "Returnable until", "Price Match", "Check Price",
    "Picked up on", "We'll hold it", "\u2019ll hold it", "We can ship",
    "SWEETWATER", "qualif",
    "Can't make it", "Can\u2019t make it",
    # "Get Product Support" and "Start a return" handled via INLINE_STRIP + SKIP_EXACT below
    "There\u2019s still time", "There's still time",
    "To purchase a protection", "Accidental Geek Squad",
    "See Details & Stores", "You can add a plan",
    "Print Receipt", "Print Gift Receipt",
    "See all orders", "Order Details",
    "Copy",               # redemption code copy button
    "Redeem Now",
    "GCR", "WBBW",        # partial redemption code prefixes
]
NOISE = re.compile(
    "|".join(re.escape(p) for p in _NOISE_PATS)
    + r"|^\$[\d,]+\.\d{2}(\s+\$[\d,]+\.\d{2})*$"   # pure price lines
    + r"|^\d Year$"                                    # "1 Year", "4 Year"
    + r"|\(\d{1,3},\d{3} reviews\)"                   # "(11,939 reviews)"
    + r"|^\d+ Year$",                                  # "4 Year"
    re.IGNORECASE
)

# Lines that are noise when standalone but valid content when appended to a product line.
# Handled by stripping them inline, then skipping if the result is one of these exactly.
SKIP_EXACT = {
    "get product support", "start a return", "check price match",
    "redeem now", "copy", "show item",
}

# Hard-stop patterns — immediately exit backward scan
STOP = re.compile(
    r"^Store Pickup (One|Two|Three|Four)|^Digital Item|"
    r"^Picked up (Today|on \w+ \d)|Order Summary|Order Total|Product Total|"
    r"^\d+ a\.m\.|In-Store Hours|Store hours|"
    r"^Payment Method|^Visa \*|^Mastercard \*|^Amex \*|"
    r"^Pickup Person|^SWEETWATER|^Adriana|"   # name/personal data lines
    r"^\d{4} W |^\d{4}$",                     # address lines, 4-digit years
    re.IGNORECASE
)

# Lines that are always structural noise regardless of position
STRUCTURAL_NOISE = re.compile(
    r"^5/\d{2}/\d{2},|"           # timestamp header "5/27/26, 8:56 PM..."
    r"bestbuy\.com/profile|"       # URL lines
    r"^\d+/\d+$|"                  # page number "1/4"
    r"^Best Buy Order Details",
    re.IGNORECASE
)


# ── PDF extraction ─────────────────────────────────────────────────────────────

def extract_text(pdf_path: str) -> str:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if not page_text:
                continue
            lines = page_text.splitlines()
            cleaned = []
            for ln in lines:
                if STRUCTURAL_NOISE.search(ln):
                    continue
                cleaned.append(ln)
            pages.append('\n'.join(cleaned))
    return '\n'.join(pages)


def is_bestbuy_invoice(text: str) -> bool:
    return bool(re.search(r'BBY\d{2}-\d+|Best Buy Order Details', text, re.IGNORECASE))


# ── Header parsing ─────────────────────────────────────────────────────────────

def parse_order_header(text: str, invoice: BestBuyInvoice):
    # Order number — "Order Number:BBY01-807181668342" (no space after colon in this layout)
    m = re.search(r'Order Number:\s*(BBY\d{2}-\d+)', text)
    if m:
        invoice.order_number = m.group(1).strip()
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Purchase date — "Purchase Date:May 15, 2026" (no space after colon)
    m = re.search(r'Purchase Date:\s*([A-Za-z]+ \d{1,2},\s*\d{4})', text)
    if m:
        dt = datetime.strptime(m.group(1).strip(), "%b %d, %Y")
        invoice.purchase_date = dt.strftime("%Y-%m-%d")
        invoice.purchase_year_month = dt.strftime("%Y-%m")
    else:
        invoice.parse_errors.append("purchase_date not found")
        invoice.needs_review = True

    # Card last 4
    m = re.search(r'\*{3,4}(\d{4})', text)
    if m:
        invoice.card_last4 = m.group(1)
    else:
        payment_collapsed = any('PAYMENT DETAILS' in e for e in invoice.parse_errors)
        if not payment_collapsed:
            invoice.parse_errors.append("card_last4 not found")
        invoice.needs_review = True

    # Fulfillment
    if re.search(r'Store Pickup', text, re.IGNORECASE):
        invoice.fulfillment_method = "Store Pick Up"
    elif re.search(r'Shipping|Delivered|Ships to', text, re.IGNORECASE):
        invoice.fulfillment_method = "Delivery"
    else:
        invoice.fulfillment_method = "Delivery"


# ── Order summary ──────────────────────────────────────────────────────────────

def parse_order_summary(text: str, invoice: BestBuyInvoice):
    # "Order Total $3,199.98" — preferred (expanded payment details)
    m = re.search(r'Order Total\s+\$([0-9,]+\.\d{2})', text)
    if not m:
        # Fallback: "Total:$3,199.98" — always in header line
        m = re.search(r'Total:\$([0-9,]+\.\d{2})', text)
    if m:
        invoice.price_total = float(m.group(1).replace(',', ''))
    else:
        invoice.parse_errors.append("price_total not found")
        invoice.needs_review = True


# ── Line item parsing ──────────────────────────────────────────────────────────

def _clean_model(raw: str) -> Optional[str]:
    """Strip 'Model:' prefix and return first token, or None if invalid."""
    if not raw:
        return None
    # Remove label prefix if present
    raw = re.sub(r'^Model:\s*', '', raw, flags=re.IGNORECASE).strip()
    # Take first whitespace-delimited token
    token = raw.split()[0] if raw else None
    if not token or token.lower() in ('model:', 'model', 'digital', 'item', ''):
        return None
    return token


def parse_line_items(text: str, invoice: BestBuyInvoice):
    """
    Anchor strategy: lines matching "Serial:... Item Total:$X" OR
    "Model:... Item Total:$X".

    This PDF layout puts Serial on the anchor line and Model on the next line.
    Example (from raw extraction):
      line N:   Serial:TANRSG004356406  Item Total: $1,599.99
      line N+1: Model:G614PR-G16.R95070TI  Product Price: $1,599.99  Returnable until...
      line N+2: SKU:6613956
      line N+3: Sales Tax, Fees & Surcharges: $0.00
      line N+4: Quantity:1

    Description sits ABOVE the Serial line. Walk backwards, but stop at:
      - Any STOP pattern
      - Any line that looks like address/personal data
      - The line that contains "Picked up" or "Store Pickup"
    Collect at most the product name lines (typically 2–3 lines for long names).
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Anchor: Serial or Model line that also contains Item Total
    anchor_indices = [
        i for i, ln in enumerate(lines)
        if re.search(r'(Model:|Serial:).*Item Total:', ln)
    ]

    if not anchor_indices:
        invoice.parse_errors.append("No item anchors found")
        invoice.needs_review = True
        return

    raw_items = []

    for anchor_idx in anchor_indices:
        anchor_line = lines[anchor_idx]

        # ── Item total ──
        m = re.search(r'Item Total:\s*\$([0-9,]+\.\d{2})', anchor_line)
        item_total = float(m.group(1).replace(',', '')) if m else 0.0

        # ── Serial number ──
        serial = None
        m_ser = re.search(r'Serial:\s*(\S+)', anchor_line)
        if m_ser:
            serial = m_ser.group(1).strip()

        # ── Model — may be on anchor line (when anchor is Model:) or next line ──
        model = None
        m_mod = re.search(r'Model:\s*(\S+)', anchor_line)
        if m_mod:
            model = _clean_model(m_mod.group(1))
        else:
            # Anchor is Serial line — look at next line for Model:
            for fwd in range(1, 4):
                nidx = anchor_idx + fwd
                if nidx >= len(lines):
                    break
                nln = lines[nidx]
                m_mod2 = re.search(r'Model:\s*(\S+)', nln)
                if m_mod2:
                    model = _clean_model(m_mod2.group(1))
                    break

        # ── SKU, unit price, quantity — scan forward ──
        sku        = None
        unit_price = 0.0
        quantity   = 1
        for offset in range(0, 10):
            idx = anchor_idx + offset
            if idx >= len(lines):
                break
            ln = lines[idx]
            if unit_price == 0.0:
                m_price = re.search(r'Product Price:\s*\$([0-9,]+\.\d{2})', ln)
                if m_price:
                    unit_price = float(m_price.group(1).replace(',', ''))
            if sku is None:
                m_sku = re.search(r'SKU:\s*(\S+)', ln)
                if m_sku:
                    sku = m_sku.group(1)
            m_qty = re.search(r'(?:^|(?<=\s))Quantity:\s*(\d+)', ln)
            if m_qty:
                quantity = int(m_qty.group(1))
                break  # Quantity is last field — stop scanning

        # Skip free digital bundles
        if unit_price == 0.0 and item_total == 0.0:
            continue

        # ── Description — walk BACKWARDS from anchor ──
        # Only collect lines that look like product name text.
        # Stop immediately at anything structural.
        # Find the previous anchor index — don't scan past it
        prev_anchor = anchor_indices[anchor_indices.index(anchor_idx) - 1]             if anchor_idx != anchor_indices[0] else -1

        desc_lines = []
        scan_idx = anchor_idx - 1
        while scan_idx >= 0 and len(desc_lines) < 5:
            # Never scan past the previous item's anchor line
            if scan_idx <= prev_anchor:
                break

            ln = lines[scan_idx]

            # Hard structural stops
            if STOP.search(ln):
                break
            if re.search(
                r'Order Total|Order Summary|Payment Method|Credit -\$|'
                r'Store hours|In-Store Hours|\d+ a\.m\.|'
                r'^\d{4} [A-Z]|^\w+ FL \d{5}',   # address patterns
                ln, re.IGNORECASE
            ):
                break
            # Stop at Geek Squad pricing table (multiple $ amounts on one line)
            if re.search(r'\$\d+\.\d{2}\s+\$\d+\.\d{2}', ln):
                break

            # Skip noise lines silently
            if NOISE.search(ln):
                scan_idx -= 1
                continue

            # Strip inline UI junk appended to the right of product name lines
            clean = re.sub(
                r'\s+(Get Product Support|Start a return|Make Pickup Changes|'
                r'Ship it Instead|Cancel &.*|Return Options|'
                r'Returnable until.*|Check Price Match).*$',
                '', ln, flags=re.IGNORECASE
            ).strip()

            # Skip if clean result is a known standalone-noise string
            if not clean or clean.lower() in SKIP_EXACT:
                scan_idx -= 1
                continue

            # Reject lines that are purely an email address, a name, or an address
            if re.match(r'^[\w.+-]+@[\w.-]+\.\w+$', clean):   # email
                scan_idx -= 1
                continue
            if re.match(r'^\d{4} [A-Z]', clean):              # street address
                break
            if re.match(r'^[A-Z][a-z]+ [A-Z][a-z]+$', clean): # "First Last" name
                break

            desc_lines.insert(0, clean)
            scan_idx -= 1

        item_description = ' '.join(desc_lines).strip()

        # Build sku_model string
        sku_model = f"SKU: {sku}" if sku else "SKU: unknown"
        if model:
            sku_model += f" | Model: {model}"
        if serial:
            sku_model += f" | Serial: {serial}"

        raw_items.append(LineItem(
            item_description=item_description,
            sku_model_color=sku_model,
            quantity=quantity,
            unit_price=unit_price,
            line_total=item_total,
            serial=serial,
        ))

    # ── Deduplication ──────────────────────────────────────────────────────────
    # BB renders each PHYSICAL unit as a separate block (Qty:1 each) with its
    # own serial number. Two units of the same SKU = two blocks.
    # Strategy: if serials differ → these are separate units, keep separate.
    # If no serial or same serial → true duplicate rendering, merge.
    deduped = []
    for item in raw_items:
        # Find existing entry with same SKU
        sku_key = re.search(r'SKU:\s*(\S+)', item.sku_model_color)
        item_sku = sku_key.group(1) if sku_key else None

        merged = False
        for existing in deduped:
            ex_sku_key = re.search(r'SKU:\s*(\S+)', existing.sku_model_color)
            ex_sku = ex_sku_key.group(1) if ex_sku_key else None

            if item_sku and item_sku == ex_sku:
                # Same SKU — check serials
                if item.serial and existing.serial and item.serial != existing.serial:
                    # Different serial numbers = different physical units, keep separate
                    break
                else:
                    # Same serial or no serial = rendering duplicate, merge
                    existing.quantity   += item.quantity
                    existing.line_total  = round(existing.line_total + item.line_total, 2)
                    if item.unit_price > existing.unit_price:
                        existing.unit_price = item.unit_price
                    merged = True
                    break

        if not merged:
            deduped.append(item)

    invoice.items = deduped

    if not invoice.items:
        invoice.parse_errors.append("No paid items found after filtering")
        invoice.needs_review = True


# ── Validation ─────────────────────────────────────────────────────────────────

def validate(invoice: BestBuyInvoice):
    if not invoice.items or invoice.price_total is None:
        return
    sum_items = round(sum(i.line_total for i in invoice.items), 2)
    if abs(sum_items - invoice.price_total) > 0.10:
        invoice.parse_errors.append(
            f"Item totals (${sum_items}) don't match Order Total (${invoice.price_total})")
        invoice.needs_review = True


# ── Payment Details expansion check ───────────────────────────────────────────

def check_payment_details_expanded(text: str) -> bool:
    expanded_indicators = [
        r'\*{3,4}\d{4}',
        r'Product Total:\s*\$',
        r'Order Total\s+\$',
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in expanded_indicators)


# ── Public API ─────────────────────────────────────────────────────────────────

def parse(pdf_path: str) -> Optional[BestBuyInvoice]:
    text = extract_text(pdf_path)
    if not is_bestbuy_invoice(text):
        return None
    invoice = BestBuyInvoice()

    if not check_payment_details_expanded(text):
        invoice.needs_review = True
        invoice.parse_errors.append(
            "PAYMENT DETAILS NOT EXPANDED — resubmit with Payment Details "
            "section expanded before printing"
        )

    parse_order_header(text, invoice)
    parse_order_summary(text, invoice)
    parse_line_items(text, invoice)
    validate(invoice)
    return invoice


def to_db_rows(invoice: BestBuyInvoice, user_id: int, company_id: int,
               invoice_file_path: str) -> dict:
    transaction = {
        "retailer":            invoice.retailer,
        "order_number":        invoice.order_number,
        "purchase_date":       invoice.purchase_date,
        "purchase_year_month": invoice.purchase_year_month,
        "user_id":             user_id,
        "company_id":          company_id,
        "card_last4":          invoice.card_last4,
        "price_total":         invoice.price_total,
        "costco_taxes_paid":   None,
        "fulfillment_method":  invoice.fulfillment_method,
        "invoice_file_path":   invoice_file_path,
        "review_status":       "Pending" if invoice.needs_review else "Auto-approved",
        "is_duplicate":        False,
    }
    items = [
        {
            "item_description": it.item_description,
            "sku_model_color":  it.sku_model_color,
            "quantity":         it.quantity,
            "unit_price":       it.unit_price,
            "line_total":       it.line_total,
        }
        for it in invoice.items
    ]
    return {"transaction": transaction, "items": items}


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, json
    pdf = sys.argv[1] if len(sys.argv) > 1 else None
    if not pdf:
        print("Usage: python bestbuy_parser.py <path_to_pdf>"); sys.exit(1)

    invoice = parse(pdf)
    if not invoice:
        print("Not a Best Buy invoice."); sys.exit(1)

    print(f"\n{'='*55}\nBEST BUY INVOICE PARSED\n{'='*55}")
    print(f"Order #:       {invoice.order_number}")
    print(f"Date:          {invoice.purchase_date}")
    print(f"Card last 4:   {invoice.card_last4}")
    print(f"Fulfillment:   {invoice.fulfillment_method}")
    print(f"Order Total:   ${invoice.price_total:,.2f}")
    print(f"Needs review:  {invoice.needs_review}")
    if invoice.parse_errors:
        print(f"Errors:        {invoice.parse_errors}")
    print(f"\nLine Items ({len(invoice.items)}):")
    for i, item in enumerate(invoice.items, 1):
        print(f"  {i}. {item.item_description[:80]}")
        print(f"     {item.sku_model_color}")
        print(f"     Qty: {item.quantity}  |  Unit: ${item.unit_price:,.2f}  |  Total: ${item.line_total:,.2f}")
    print(f"\nDB rows:")
    print(json.dumps(to_db_rows(invoice, 999, 999, "test.pdf"), indent=2, default=str))
