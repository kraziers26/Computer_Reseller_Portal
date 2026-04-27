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



# UI noise patterns — lines to ignore when building descriptions
_NOISE_PATS = [
    "Make Pickup Changes", "Ship it Instead", "Add Alternate Pickup",
    "Cancel &", "AppleCare", "Protection for your",
    "What's Included", "Terms & Conditions", "review",
    "Add 4 Year", "Add 3 Year", "Digital Item", "Order Received",
    "Ready to Redeem", "Ready for Pickup", "Included free",
    "Pickup Person", "Store Pickup", "Extend Pickup",
    "Store hours", "In-Store Hours",
    "Best Buy Sweetwater", "Best Buy Support", "Browse our", "Get help",
    "Resend Email", "Redeem Now", "We've emailed", "We\u2019ve emailed",
    "Return Options", "Returnable until", "Price Match", "Check Price",
    "Picked up on", "We'll hold it", "We\u2019ll hold it", "We can ship",
    "SWEETWATER", "qualif",
    "Can't make it", "Can\u2019t make it",
]
NOISE = re.compile("|".join(re.escape(p) for p in _NOISE_PATS)
                   + r"|^\$[\d,]+\.\d{2}(\s+\$[\d,]+\.\d{2})*$"
                   + r"|^\d Year$",
                   re.IGNORECASE)

# Hard-stop patterns — stop backward scan immediately
STOP = re.compile(
    r"^Store Pickup (One|Two|Three|Four)|^Digital Item|"
    r"^Picked up on \w+ \d|Order Summary|Order Total|Product Total|"
    r"^\d+ a\.m\.|In-Store Hours|Store hours",
    re.IGNORECASE
)

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
                if re.match(r'^Best Buy Order Details\s+\d+/\d+/\d+', ln): continue
                if re.search(r'bestbuy\.com/profile/ss/orders', ln): continue
                cleaned.append(ln)
            pages.append('\n'.join(cleaned))
    return '\n'.join(pages)


def is_bestbuy_invoice(text: str) -> bool:
    return bool(re.search(r'BBY\d{2}-\d+|Best Buy Order Details', text, re.IGNORECASE))


def parse_order_header(text: str, invoice: BestBuyInvoice):
    # Order number BBY01-XXXXXXXXXX
    m = re.search(r'Order Number:\s*(BBY\d{2}-\d+)', text)
    if m:
        invoice.order_number = m.group(1).strip()
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Purchase date "Purchase Date:Mar 18, 2026"
    m = re.search(r'Purchase Date:\s*([A-Za-z]+ \d{1,2},\s*\d{4})', text)
    if m:
        dt = datetime.strptime(m.group(1).strip(), "%b %d, %Y")
        invoice.purchase_date = dt.strftime("%Y-%m-%d")
        invoice.purchase_year_month = dt.strftime("%Y-%m")
    else:
        invoice.parse_errors.append("purchase_date not found")
        invoice.needs_review = True

    # Card last 4 "Visa ****8299" — only present when Payment Details expanded
    m = re.search(r'\*{3,4}(\d{4})', text)
    if m:
        invoice.card_last4 = m.group(1)
    else:
        # Suppress redundant error if already flagged for collapsed payment details
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


def parse_order_summary(text: str, invoice: BestBuyInvoice):
    # Try Order Total first (only present when Payment Details expanded)
    m = re.search(r'Order Total\s+\$([0-9,]+\.\d{2})', text)
    if not m:
        # Fallback: "Total:$X" always present in page header even when collapsed
        m = re.search(r'Total:\$([0-9,]+\.\d{2})', text)
    if m:
        invoice.price_total = float(m.group(1).replace(',', ''))
    else:
        invoice.parse_errors.append("price_total not found")
        invoice.needs_review = True


def parse_line_items(text: str, invoice: BestBuyInvoice):
    """
    Best Buy item block (from line index inspection):
      Line N:   <description part 1>
      Line N+1: <description part 2>  Make Pickup Changes     ← UI noise appended
      Line N+2: Model:<model>  Item Total: $<total>           ← anchor line
      Line N+3: Ship it Instead                               ← noise
      Line N+4: SKU:<sku>  Product Price: $<unit_price>
      Line N+5: Quantity:<qty>
      Line N+6: Sales Tax, Fees & Surcharges: $0.00 ...

    Anchor: lines matching "Model:.*Item Total:"
    Description: walk backwards from Model line, collect non-noise lines, strip appended noise.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Find all Model+ItemTotal anchor lines
    # Format 1: "Model:MHFF4LL/A Item Total: $599.00"  (model on same line)
    # Format 2: "Serial:XXXXX Item Total: $1,199.99"   (serial precedes model)
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

        # Item total from anchor line
        m = re.search(r'Item Total:\s*\$([0-9,]+\.\d{2})', anchor_line)
        item_total = float(m.group(1).replace(',', '')) if m else 0.0

        # Model from anchor line — may be inline "Model:MHFF4LL/A" or
        # split across two lines "Model:\n16-AP0053DX / BP1Q1UA#ABA"
        m = re.search(r'Model:\s*(\S[^\n]*)', anchor_line)
        if m and m.group(1).strip():
            model = m.group(1).strip().split()[0]  # first token only
        else:
            # Check next line for model value
            next_line = lines[anchor_idx + 1] if anchor_idx + 1 < len(lines) else ''
            if next_line and not re.search(
                    r'SKU:|Quantity:|Item Total:|Serial:|Sales Tax', next_line):
                model = next_line.strip().split()[0]
            else:
                model = None
        # Clean up model if it accidentally captured the label itself
        if model and model.lower() in ('model:', 'model'):
            model = None

        # SKU, unit price and quantity — scan forward from anchor
        # Product Price: may appear on the Model line OR the SKU line depending on layout
        sku = None
        unit_price = 0.0
        quantity = 1
        # First check if Product Price: is on the anchor line itself or lines just after
        for offset in range(0, 8):
            idx = anchor_idx + offset
            if idx >= len(lines):
                break
            ln = lines[idx]
            # Pick up unit price wherever Product Price: appears
            if unit_price == 0.0:
                m_price = re.search(r'Product Price:\s*\$([0-9,]+\.\d{2})', ln)
                if m_price:
                    unit_price = float(m_price.group(1).replace(',', ''))
            # Pick up SKU
            if sku is None:
                m_sku = re.search(r'SKU:\s*(\S+)', ln)
                if m_sku:
                    sku = m_sku.group(1)
            # Pick up quantity
            m_qty = re.match(r'^Quantity:\s*(\d+)', ln)
            if m_qty:
                quantity = int(m_qty.group(1))
                break  # quantity is always last, stop here

        # Skip $0.00 free digital bundles
        if unit_price == 0.0 and item_total == 0.0:
            continue

        # Description — walk backwards from anchor, collect non-noise lines
        desc_lines = []
        scan_idx = anchor_idx - 1
        while scan_idx >= 0 and len(desc_lines) < 6:
            ln = lines[scan_idx]
            # Hard stop — immediately exit on section boundaries
            if STOP.search(ln):
                break
            if re.search(r'Order Total|Order Summary|Payment Method|Credit -\$|'
                         r'Store hours|In-Store Hours|\d+ a\.m\.|^\d{4}$', ln, re.IGNORECASE):
                break
            if not NOISE.search(ln):
                # Strip appended UI noise from end of line
                clean = re.sub(
                    r'\s+(Make Pickup Changes|Ship it Instead|Cancel &.*|'
                    r"I\'m at the store|Return Options|Returnable until.*)$",
                    '', ln, flags=re.IGNORECASE).strip()
                # Skip lines that are purely price amounts (AppleCare prices etc.)
                if clean and not re.match(r'^\$[\d,]+\.\d{2}(\s+\$[\d,]+\.\d{2})*$', clean):
                    desc_lines.insert(0, clean)
            scan_idx -= 1

        item_description = ' '.join(desc_lines).strip()

        sku_model = f"SKU: {sku}" if sku else "SKU: unknown"
        if model and model not in ('', 'DIGITAL', 'ITEM'):
            sku_model += f" | Model: {model}"

        raw_items.append(LineItem(
            item_description=item_description,
            sku_model_color=sku_model,
            quantity=quantity,
            unit_price=unit_price,
            line_total=item_total,
        ))

    # Deduplicate — Best Buy renders each unit as a separate block (Qty:1 each).
    # Key by SKU only so same item with unit_price=0 (layout variant) merges correctly.
    seen = {}
    for item in raw_items:
        # Use just the SKU number as dedup key
        sku_key = re.search(r'SKU:\s*(\S+)', item.sku_model_color)
        key = sku_key.group(1) if sku_key else item.sku_model_color
        if key in seen:
            seen[key].quantity += item.quantity
            seen[key].line_total = round(seen[key].line_total + item.line_total, 2)
            # Keep the higher unit_price (one variant may come through as $0)
            if item.unit_price > seen[key].unit_price:
                seen[key].unit_price = item.unit_price
        else:
            seen[key] = item

    invoice.items = list(seen.values())

    if not invoice.items:
        invoice.parse_errors.append("No paid items found after filtering")
        invoice.needs_review = True


def validate(invoice: BestBuyInvoice):
    if not invoice.items or invoice.price_total is None:
        return
    sum_items = round(sum(i.line_total for i in invoice.items), 2)
    if abs(sum_items - invoice.price_total) > 0.10:
        invoice.parse_errors.append(
            f"Item totals (${sum_items}) don't match Order Total (${invoice.price_total})")
        invoice.needs_review = True




# ── Payment Details expansion check ─────────────────────────────────────────

def check_payment_details_expanded(text: str) -> bool:
    """
    Returns True if Payment Details section was expanded when printing.
    Key indicator: the Order Summary block only appears when expanded.
    "Product Total:" appears in the Order Summary — NOT in item rows.
    Card masked number (****XXXX) also confirms expansion.
    NOTE: "Sales Tax, Fees & Surcharges" appears on every item row so
    is NOT a reliable indicator of the Order Summary block.
    """
    expanded_indicators = [
        r'\*{3,4}\d{4}',          # masked card number e.g. ****8299
        r'Product Total:\s*\$',   # Order Summary block (only when expanded)
        r'Order Total\s+\$',      # Order Total label in summary table
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in expanded_indicators)

def parse(pdf_path: str) -> Optional[BestBuyInvoice]:
    text = extract_text(pdf_path)
    if not is_bestbuy_invoice(text):
        return None
    invoice = BestBuyInvoice()

    # Check if Payment Details was expanded — if not, flag immediately
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
    items = [{"item_description": it.item_description, "sku_model_color": it.sku_model_color,
              "quantity": it.quantity, "unit_price": it.unit_price, "line_total": it.line_total}
             for it in invoice.items]
    return {"transaction": transaction, "items": items}


if __name__ == "__main__":
    import sys, json
    pdf = sys.argv[1] if len(sys.argv) > 1 else None
    if not pdf:
        print("Usage: python bestbuy_parser.py <path_to_pdf>"); sys.exit(1)

    invoice = parse(pdf)
    if not invoice:
        print("Not a Best Buy invoice."); sys.exit(1)

    print(f"\n{'='*50}\nBEST BUY INVOICE PARSED\n{'='*50}")
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
        print(f"  {i}. {item.item_description[:70]}")
        print(f"     {item.sku_model_color}  |  Qty: {item.quantity}  |  "
              f"Unit: ${item.unit_price:,.2f}  |  Total: ${item.line_total:,.2f}")
    print(f"\nDB rows:")
    print(json.dumps(to_db_rows(invoice, 999, 999, "test.pdf"), indent=2, default=str))
