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
class WalmartInvoice:
    retailer: str = "Walmart"
    order_number: Optional[str] = None
    purchase_date: Optional[str] = None
    purchase_year_month: Optional[str] = None
    card_last4: Optional[str] = None
    fulfillment_method: str = "Delivery"  # Walmart online = always delivery
    price_total: Optional[float] = None
    savings: Optional[float] = None       # NEW — captures Savings line if present
    items: list = field(default_factory=list)
    parse_errors: list = field(default_factory=list)
    needs_review: bool = False


# Month abbreviation map
MONTHS = {
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
    'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}


def extract_text(pdf_path: str) -> str:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                # Remove unicode icon characters (e.g. \uf140 arrow icon)
                t = re.sub(r'[\uf000-\uf8ff]', '', t)
                pages.append(t)
    return '\n'.join(pages)


def is_walmart_invoice(text: str) -> bool:
    return bool(re.search(
        r'walmart|Order#\s*\d{7}-\d+|Charge history',
        text, re.IGNORECASE))


def check_card_visible(text: str) -> bool:
    """Returns True if card info was rendered in the PDF."""
    return bool(re.search(r'Ending in\s*\d{4}', text, re.IGNORECASE))


def parse_order_header(text: str, invoice: WalmartInvoice):
    # Order number — "Order# 2000140-54822457"
    m = re.search(r'Order#\s*([\d-]+)', text)
    if m:
        invoice.order_number = m.group(1).strip()
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Date — "May 13, 2026 order" or "Nov 19, 2025 order"
    m = re.search(r'([A-Za-z]{3,9})\s+(\d{1,2}),\s*(\d{4})\s+order', text, re.IGNORECASE)
    if m:
        month = MONTHS.get(m.group(1).lower()[:3])
        day, year = int(m.group(2)), int(m.group(3))
        if month:
            invoice.purchase_date = f"{year}-{month:02d}-{day:02d}"
            invoice.purchase_year_month = f"{year}-{month:02d}"
        else:
            invoice.parse_errors.append("purchase_date month not recognized")
            invoice.needs_review = True
    else:
        invoice.parse_errors.append("purchase_date not found")
        invoice.needs_review = True

    # Card last 4 — "Ending in 0529" — only present when payment section rendered
    m = re.search(r'Ending in\s*(\d{4})', text, re.IGNORECASE)
    if m:
        invoice.card_last4 = m.group(1)
    # No card visible is normal for this invoice format — not flagged as error


def parse_totals(text: str, invoice: WalmartInvoice):
    # "Total $2298.00" — bolded final total after savings/tax
    m = re.search(r'\bTotal\s+\$([0-9,]+\.\d{2})', text)
    if m:
        invoice.price_total = float(m.group(1).replace(',', ''))
    else:
        invoice.parse_errors.append("price_total not found")
        invoice.needs_review = True

    # "Savings -$300.00" — optional discount line
    m = re.search(r'\bSavings\s+-?\$([0-9,]+\.\d{2})', text, re.IGNORECASE)
    if m:
        invoice.savings = float(m.group(1).replace(',', ''))


def parse_line_items(text: str, invoice: WalmartInvoice):
    """
    Walmart item line structure (single line per item):
      <description> Qty <N> $<line_total>

    The line_total on each item is the PRE-discount subtotal for that item.
    The Savings line is a single order-level discount applied at checkout.
    We prorate the savings across items proportionally so each item's
    stored unit_price reflects the actual amount paid.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Find the LAST "Order#" line as start boundary (it appears twice — header and footer)
    order_indices = [i for i, ln in enumerate(lines) if re.match(r'Order#', ln)]
    order_idx = order_indices[0] if order_indices else 0

    # Find "Subtotal" as end boundary
    subtotal_idx = next((i for i, ln in enumerate(lines)
                         if re.match(r'Subtotal', ln, re.IGNORECASE)), len(lines))

    item_lines = lines[order_idx + 1:subtotal_idx]

    # Skip non-item lines between order header and items (Buyer, name, address)
    # Item line pattern: ends with "Qty N $X.XX"
    item_pat = re.compile(r'^(.+?)\s+Qty\s+(\d+)\s+\$([0-9,]+\.\d{2})$', re.IGNORECASE)

    raw_items = []
    for ln in item_lines:
        m = item_pat.match(ln)
        if not m:
            continue
        raw_items.append({
            'desc':       m.group(1).strip(),
            'quantity':   int(m.group(2)),
            'line_total': float(m.group(3).replace(',', '')),
        })

    if not raw_items:
        invoice.parse_errors.append("No line items found")
        invoice.needs_review = True
        return

    # Prorate savings across items if present
    # e.g. $300 savings on $2598 subtotal → each item discounted proportionally
    subtotal = sum(it['line_total'] for it in raw_items)
    savings  = invoice.savings or 0.0

    for it in raw_items:
        if savings > 0 and subtotal > 0:
            item_discount  = round(savings * (it['line_total'] / subtotal), 2)
            adj_line_total = round(it['line_total'] - item_discount, 2)
        else:
            adj_line_total = it['line_total']

        unit_price = round(adj_line_total / it['quantity'], 2) if it['quantity'] > 0 else adj_line_total

        invoice.items.append(LineItem(
            item_description=it['desc'],
            sku_model_color='',   # Walmart doesn't show SKU on invoice
            quantity=it['quantity'],
            unit_price=unit_price,
            line_total=adj_line_total,
        ))


def validate(invoice: WalmartInvoice):
    if not invoice.items or invoice.price_total is None:
        return
    sum_items = round(sum(i.line_total for i in invoice.items), 2)
    if abs(sum_items - invoice.price_total) > 0.10:
        invoice.parse_errors.append(
            f"Item totals (${sum_items}) don't match Total (${invoice.price_total})")
        invoice.needs_review = True


def parse(pdf_path: str) -> Optional[WalmartInvoice]:
    text = extract_text(pdf_path)
    if not is_walmart_invoice(text):
        return None
    invoice = WalmartInvoice()
    parse_order_header(text, invoice)
    parse_totals(text, invoice)
    parse_line_items(text, invoice)
    validate(invoice)
    return invoice


def to_db_rows(invoice: WalmartInvoice, user_id: int, company_id: int,
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
        print("Usage: python walmart_parser.py <path>"); sys.exit(1)

    invoice = parse(pdf)
    if not invoice:
        print("Not a Walmart invoice."); sys.exit(1)

    print(f"\n{'='*50}\nWALMART INVOICE PARSED\n{'='*50}")
    print(f"Order #:      {invoice.order_number}")
    print(f"Date:         {invoice.purchase_date}")
    print(f"Card last 4:  {invoice.card_last4}")
    print(f"Savings:      ${invoice.savings:,.2f}" if invoice.savings else "Savings:      None")
    print(f"Fulfillment:  {invoice.fulfillment_method}")
    print(f"Order Total:  ${invoice.price_total:,.2f}" if invoice.price_total else "Order Total:  None")
    print(f"Needs review: {invoice.needs_review}")
    if invoice.parse_errors:
        print(f"Errors:       {invoice.parse_errors}")
    print(f"\nLine Items ({len(invoice.items)}):")
    for i, item in enumerate(invoice.items, 1):
        print(f"  {i}. {item.item_description[:70]}")
        print(f"     Qty: {item.quantity}  |  Unit: ${item.unit_price:,.2f}  |  Total: ${item.line_total:,.2f}")
    print(f"\nDB rows:")
    print(json.dumps(to_db_rows(invoice, 999, 999, "test.pdf"), indent=2, default=str))
