import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import pdfplumber


# Products that Apple always serializes — used to decide whether a missing
# serial should flag the invoice for review. Accessories (cases, cables,
# adapters, AppleCare, etc.) legitimately have no serial and are exempt.
SERIALIZED_KEYWORDS = (
    "IPHONE", "IPAD", "MACBOOK", "IMAC", "MAC MINI", "MAC STUDIO", "MAC PRO",
    "APPLE WATCH", "WATCH", "AIRPODS", "APPLE TV", "STUDIO DISPLAY",
    "PRO DISPLAY", "VISION PRO", "HOMEPOD",
)


def is_serialized(description: str) -> bool:
    d = (description or "").upper()
    return any(k in d for k in SERIALIZED_KEYWORDS)


@dataclass
class LineItem:
    item_description: str
    sku_model_color: str
    quantity: int
    unit_price: float
    line_total: float
    serial_number: Optional[str] = None
    imei: Optional[str] = None
    tax_amount: float = 0.0
    landed_cost: Optional[float] = None


@dataclass
class AppleInvoice:
    retailer: str = "Apple"
    order_number: Optional[str] = None
    purchase_date: Optional[str] = None
    purchase_year_month: Optional[str] = None
    card_last4: Optional[str] = None
    fulfillment_method: str = "Store Pick Up"  # Apple Store = always in-store
    price_total: Optional[float] = None
    sales_tax: Optional[float] = None
    invoice_format: Optional[str] = None  # 'web_invoice' | 'retail_receipt'
    items: list = field(default_factory=list)
    parse_errors: list = field(default_factory=list)
    needs_review: bool = False


def extract_text(pdf_path: str) -> str:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if not t:
                continue
            lines = t.splitlines()
            cleaned = []
            for ln in lines:
                # Strip Apple URL + timestamp footer lines
                if re.search(r'secure\d*\.store\.apple\.com', ln): continue
                if re.match(r'P[aá]g(e|ina)\s+\d+\s+(of|de)\s+\d+', ln, re.IGNORECASE): continue
                cleaned.append(ln)
            pages.append('\n'.join(cleaned))
    return '\n'.join(pages)


def is_apple_invoice(text: str) -> bool:
    return bool(re.search(
        r'Apple Store|Invoice Receipt|store\.apple\.com|apple\.com/(retail|support)'
        r'|Order Number:\s*W\d+|Part Number:',
        text, re.IGNORECASE))


def detect_format(text: str) -> str:
    """
    Two Apple layouts:
      - 'retail_receipt': in-store email/print receipt. Item blocks with
        'Part Number:', 'Serial Number:', 'IMEI:', 'Return Date:'; order id
        like *R##########*; totals use 'Sub-Total'/'Tax'/'Total' with a space
        after the dollar sign.
      - 'web_invoice': the online order invoice (formats A/B). 'Order Number: W…',
        'Serial No.: (…)', 'Sales Tax', no space after the dollar sign.
    """
    if re.search(r'Part Number:', text, re.IGNORECASE) and (
        re.search(r'Return Date:', text, re.IGNORECASE)
        or re.search(r'\*?R\d{9,12}\*?', text)
    ):
        return 'retail_receipt'
    return 'web_invoice'


# --------------------------------------------------------------------------
# WEB INVOICE (formats A / B) — online order invoice
# --------------------------------------------------------------------------

def parse_order_header(text: str, invoice: AppleInvoice):
    # Order number — "Order Number: Order Date:\nW1508947786 January 15, 2026"
    m = re.search(r'Order Number:.*?\n(W\d+)', text, re.DOTALL)
    if not m:
        m = re.search(r'\b(W\d{9,10})\b', text)
    if m:
        invoice.order_number = m.group(1).strip()
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Order date — same line as order number, after the W number
    m = re.search(r'W\d{9,10}\s+([A-Za-z]+ \d{1,2},\s*\d{4})', text)
    if not m:
        m = re.search(r'Order Date:.*?([A-Za-z]+ \d{1,2},\s*\d{4})', text, re.DOTALL)
    if m:
        try:
            dt = datetime.strptime(m.group(1).strip(), "%B %d, %Y")
            invoice.purchase_date = dt.strftime("%Y-%m-%d")
            invoice.purchase_year_month = dt.strftime("%Y-%m")
        except ValueError:
            invoice.parse_errors.append("purchase_date parse error")
            invoice.needs_review = True
    else:
        invoice.parse_errors.append("purchase_date not found")
        invoice.needs_review = True

    # Card last 4 — "charged to Visa XXXXXXXXXXXX1231"
    m = re.search(r'charged to.*?X+(\d{4})', text, re.IGNORECASE)
    if not m:
        m = re.search(r'Visa\s+X+(\d{4})', text, re.IGNORECASE)
    if m:
        invoice.card_last4 = m.group(1)
    else:
        invoice.parse_errors.append("card_last4 not found")
        invoice.needs_review = True


def parse_totals(text: str, invoice: AppleInvoice):
    # "For a total of $1,282.93" — reliable on both A/B
    m = re.search(r'For a total of\s+\$([0-9,]+\.\d{2})', text)
    if m:
        invoice.price_total = float(m.group(1).replace(',', ''))
    else:
        m = re.search(r'\bTotal\s+\$([0-9,]+\.\d{2})', text)
        if m:
            invoice.price_total = float(m.group(1).replace(',', ''))
        else:
            invoice.parse_errors.append("price_total not found")
            invoice.needs_review = True

    m = re.search(r'Sales Tax\s+\$([0-9,]+\.\d{2})', text)
    invoice.sales_tax = float(m.group(1).replace(',', '')) if m else None


def _split_product_number(raw_desc: str):
    prod_num_m = re.search(r'(?:-USA)?\s*([A-Z0-9]{6,12}(?:/[A-Z])?)\s*$', raw_desc)
    if prod_num_m:
        product_number = prod_num_m.group(1)
        item_description = raw_desc[:prod_num_m.start()].strip()
    else:
        product_number = ''
        item_description = raw_desc
    item_description = re.sub(r'\s*-USA\s*$', '', item_description).strip()
    return item_description, product_number


def parse_line_items(text: str, invoice: AppleInvoice):
    """
    Web-invoice item structure (pdfplumber merges table columns):

      IPHONE 17 PRO MAX SILVER 256GB-USA MFXG4LL/A $1,199.00 1 1 $1,199.00
      Serial No.: (HJ6LQ7P005)

    Each unit gets its own row so every serial is individually stored. A line
    with qty > 1 is followed by one 'Serial No.:' line per unit and is expanded
    into that many qty-1 rows.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    details_idx = next((i for i, ln in enumerate(lines)
                        if re.match(r'Order Details', ln, re.IGNORECASE)), 0)
    end_idx = next((i for i, ln in enumerate(lines)
                    if re.search(r'Items will be invoiced', ln, re.IGNORECASE)), len(lines))
    item_lines = lines[details_idx:end_idx]

    item_pat = re.compile(
        r'^(.+?)\s+\$([0-9,]+\.\d{2})\s+(\d+)\s+(\d+)(?:\s+\$[0-9,]+\.\d{2})?$'
    )
    serial_pat = re.compile(r'Serial No\.?:\s*\(?([A-Za-z0-9]+)\)?', re.IGNORECASE)

    i = 0
    while i < len(item_lines):
        m = item_pat.match(item_lines[i])
        if not m:
            i += 1
            continue

        raw_desc = m.group(1).strip()
        unit_price = float(m.group(2).replace(',', ''))
        qty_ordered = int(m.group(3))
        qty_fulfilled = int(m.group(4))
        quantity = qty_fulfilled if qty_fulfilled > 0 else qty_ordered

        item_description, product_number = _split_product_number(raw_desc)

        # Collect the serial line(s) that follow this item, one per unit.
        serials = []
        j = i + 1
        while j < len(item_lines):
            sm = serial_pat.search(item_lines[j])
            if sm:
                serials.append(sm.group(1).upper())
                j += 1
            else:
                break

        if serials:
            for s in serials:
                invoice.items.append(LineItem(
                    item_description=item_description,
                    sku_model_color=product_number,
                    quantity=1,
                    unit_price=unit_price,
                    line_total=unit_price,
                    serial_number=s,
                ))
            if len(serials) != quantity:
                invoice.parse_errors.append(
                    f"serial count {len(serials)} != qty {quantity} for {item_description}")
                invoice.needs_review = True
            i = j
        else:
            # No serial lines (non-serialized accessory, or a serial Apple
            # didn't print). Keep the original quantity as a single row.
            invoice.items.append(LineItem(
                item_description=item_description,
                sku_model_color=product_number,
                quantity=quantity,
                unit_price=unit_price,
                line_total=round(unit_price * quantity, 2),
                serial_number=None,
            ))
            i += 1

    if not invoice.items:
        invoice.parse_errors.append("No line items found")
        invoice.needs_review = True


# --------------------------------------------------------------------------
# RETAIL RECEIPT — in-store email/print receipt
# --------------------------------------------------------------------------

def parse_retail_receipt(text: str, invoice: AppleInvoice):
    lines = [ln.strip() for ln in text.splitlines()]

    # Order date from the header timestamp: "September 18, 2026 05:21 PM"
    for ln in lines:
        m = re.search(r'([A-Za-z]{3,9}\s+\d{1,2},\s*\d{4})\s+\d{1,2}:\d{2}\s*(?:AM|PM)', ln)
        if m:
            try:
                dt = datetime.strptime(m.group(1), "%B %d, %Y")
                invoice.purchase_date = dt.strftime("%Y-%m-%d")
                invoice.purchase_year_month = dt.strftime("%Y-%m")
            except ValueError:
                pass
            break
    if not invoice.purchase_date:
        invoice.parse_errors.append("purchase_date not found")
        invoice.needs_review = True

    # Order number: "*R3121025575*"
    m = re.search(r'\*?(R\d{9,12})\*?', text)
    if m:
        invoice.order_number = m.group(1)
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Card last 4: "•••• 4360" or "Card Number: •••• 4360"
    m = re.search(r'(?:[•\*]\s*){2,}\s*(\d{4})', text)
    if not m:
        m = re.search(r'Card Number:.*?(\d{4})', text, re.IGNORECASE)
    if m:
        invoice.card_last4 = m.group(1)
    else:
        invoice.parse_errors.append("card_last4 not found")
        invoice.needs_review = True

    # Totals — note the space after '$' and the 'Sub-Total'/'Tax'/'Total' labels
    for ln in lines:
        mt = re.match(r'Tax\s+\$\s*([\d,]+\.\d{2})', ln, re.IGNORECASE)
        if mt:
            invoice.sales_tax = float(mt.group(1).replace(',', ''))
        mtot = re.match(r'Total\s+\$\s*([\d,]+\.\d{2})', ln, re.IGNORECASE)
        if mtot and not ln.lower().startswith('sub'):
            invoice.price_total = float(mtot.group(1).replace(',', ''))
    if invoice.price_total is None:
        invoice.parse_errors.append("price_total not found")
        invoice.needs_review = True

    # Item region: after the header timestamp, up to the legal boilerplate /
    # totals / payment block.
    start = 0
    for i, ln in enumerate(lines):
        if re.search(r'\d{1,2}:\d{2}\s*(?:AM|PM)', ln):
            start = i + 1
            break
    end = len(lines)
    for i in range(start, len(lines)):
        if re.match(r'(Use of |Sub-?Total|Payment Method|Total\s+\$)', lines[i], re.IGNORECASE):
            end = i
            break

    price_line = re.compile(r'^(.*\S)\s+\$\s*([\d,]+\.\d{2})$')

    i = start
    while i < end:
        pm = price_line.match(lines[i])
        if not pm:
            i += 1
            continue
        desc = pm.group(1).strip()
        unit_price = float(pm.group(2).replace(',', ''))
        part = serial = imei = None
        j = i + 1
        while j < end:
            l2 = lines[j]
            if re.match(r'Part Number:', l2, re.IGNORECASE):
                part = l2.split(':', 1)[1].strip()
            elif re.match(r'Serial Number:', l2, re.IGNORECASE):
                serial = l2.split(':', 1)[1].strip().upper()
            elif re.match(r'IMEI:', l2, re.IGNORECASE):
                imei = l2.split(':', 1)[1].strip()
            elif re.match(r'Return Date:', l2, re.IGNORECASE):
                pass
            elif re.match(r'For Support', l2, re.IGNORECASE):
                pass
            elif price_line.match(l2):
                break  # next item block
            else:
                break  # unexpected line — stop this block
            j += 1

        invoice.items.append(LineItem(
            item_description=desc,
            sku_model_color=part or '',
            quantity=1,
            unit_price=unit_price,
            line_total=unit_price,
            serial_number=serial,
            imei=imei,
        ))
        i = j if j > i else i + 1

    if not invoice.items:
        invoice.parse_errors.append("No line items found")
        invoice.needs_review = True


# --------------------------------------------------------------------------

def allocate_tax_amounts(weights, total_tax):
    """
    Split total_tax across the given pre-tax line weights, proportionally, to
    the cent, dropping any rounding remainder on the largest-weight line so the
    result sums back to total_tax exactly. Returns a list of per-line tax
    dollars aligned with `weights`.

    Shared by the parser (parse time) and the upload route (re-allocation when
    a line is edited on the confirm screen), so both use identical math.
    """
    n = len(weights)
    if n == 0:
        return []
    if not total_tax or total_tax <= 0:
        return [0.0] * n

    subtotal = round(sum(weights), 2)
    tax_cents = int(round(total_tax * 100))

    if subtotal <= 0:
        base, rem = divmod(tax_cents, n)
        cents = [base + (1 if i < rem else 0) for i in range(n)]
    else:
        cents = [int(round(tax_cents * (w / subtotal))) for w in weights]
        diff = tax_cents - sum(cents)
        if diff != 0:
            largest = max(range(n), key=lambda i: weights[i])
            cents[largest] += diff

    return [round(c / 100, 2) for c in cents]


def allocate_tax(invoice: AppleInvoice):
    """
    Allocate the receipt's sales tax across the invoice's lines and set
    tax_amount + landed_cost (tax-inclusive cost basis) on each. unit_price and
    line_total stay pre-tax and untouched.
    """
    for it in invoice.items:
        it.line_total = round(it.unit_price * it.quantity, 2)
    weights = [it.line_total for it in invoice.items]
    taxes = allocate_tax_amounts(weights, invoice.sales_tax or 0.0)
    for it, t in zip(invoice.items, taxes):
        it.tax_amount = t
        it.landed_cost = round(it.line_total + t, 2)


def validate(invoice: AppleInvoice):
    if invoice.items and invoice.price_total is not None:
        sum_items = round(sum(i.line_total for i in invoice.items), 2)
        if sum_items == 0:
            invoice.parse_errors.append("All line totals zero — check parser")
            invoice.needs_review = True

    # Serialized products must carry a serial; accessories are exempt.
    for it in invoice.items:
        if is_serialized(it.item_description) and not it.serial_number:
            invoice.parse_errors.append(
                f"missing serial for serialized item: {it.item_description}")
            invoice.needs_review = True


def parse(pdf_path: str) -> Optional[AppleInvoice]:
    text = extract_text(pdf_path)
    if not is_apple_invoice(text):
        return None
    invoice = AppleInvoice()
    invoice.invoice_format = detect_format(text)
    if invoice.invoice_format == 'retail_receipt':
        parse_retail_receipt(text, invoice)
    else:
        parse_order_header(text, invoice)
        parse_totals(text, invoice)
        parse_line_items(text, invoice)
    allocate_tax(invoice)
    validate(invoice)
    return invoice


def to_db_rows(invoice: AppleInvoice, user_id: int, company_id: int,
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
              "quantity": it.quantity, "unit_price": it.unit_price, "line_total": it.line_total,
              "serial_number": it.serial_number, "imei": it.imei,
              "tax_amount": it.tax_amount, "landed_cost": it.landed_cost}
             for it in invoice.items]
    return {"transaction": transaction, "items": items}


if __name__ == "__main__":
    import sys, json
    pdf = sys.argv[1] if len(sys.argv) > 1 else None
    if not pdf:
        print("Usage: python apple_parser.py <path>"); sys.exit(1)

    invoice = parse(pdf)
    if not invoice:
        print("Not an Apple invoice."); sys.exit(1)

    print(f"\n{'='*50}\nAPPLE INVOICE PARSED\n{'='*50}")
    print(f"Format:       {invoice.invoice_format}")
    print(f"Order #:      {invoice.order_number}")
    print(f"Date:         {invoice.purchase_date}")
    print(f"Card last 4:  {invoice.card_last4}")
    print(f"Fulfillment:  {invoice.fulfillment_method}")
    print(f"Order Total:  ${invoice.price_total:,.2f}" if invoice.price_total else "Order Total:  None")
    print(f"Sales Tax:    ${invoice.sales_tax:,.2f}" if invoice.sales_tax else "Sales Tax:    (not shown)")
    print(f"Needs review: {invoice.needs_review}")
    if invoice.parse_errors:
        print(f"Errors:       {invoice.parse_errors}")
    print(f"\nLine Items ({len(invoice.items)}):")
    for i, item in enumerate(invoice.items, 1):
        print(f"  {i}. {item.item_description}")
        print(f"     SKU: {item.sku_model_color}  |  Qty: {item.quantity}  |  "
              f"Unit: ${item.unit_price:,.2f}  |  Total: ${item.line_total:,.2f}")
        print(f"     Serial: {item.serial_number or '—'}  |  IMEI: {item.imei or '—'}")
        print(f"     Tax: ${item.tax_amount:,.2f}  |  Landed (incl tax): ${item.landed_cost:,.2f}")
    print(f"\nDB rows:")
    print(json.dumps(to_db_rows(invoice, 999, 999, "test.pdf"), indent=2, default=str))
