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
class AppleInvoice:
    retailer: str = "Apple"
    order_number: Optional[str] = None
    purchase_date: Optional[str] = None
    purchase_year_month: Optional[str] = None
    card_last4: Optional[str] = None
    fulfillment_method: str = "Store Pick Up"  # Apple Store = always in-store
    price_total: Optional[float] = None
    sales_tax: Optional[float] = None
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
        r'Apple Store|Invoice Receipt|store\.apple\.com|Order Number:\s*W\d+',
        text, re.IGNORECASE))


def parse_order_header(text: str, invoice: AppleInvoice):
    # Order number — "Order Number: Order Date:\nW1508947786 January 15, 2026"
    # pdfplumber puts labels on one line and values on the next
    m = re.search(r'Order Number:.*?\n(W\d+)', text, re.DOTALL)
    if not m:
        m = re.search(r'\b(W\d{9,10})\b', text)
    if m:
        invoice.order_number = m.group(1).strip()
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Order date — on same line as order number after the W number
    # "W1508947786 January 15, 2026"
    m = re.search(r'W\d{9,10}\s+([A-Za-z]+ \d{1,2},\s*\d{4})', text)
    if not m:
        # Fallback: any "Month DD, YYYY" near top
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
    # "For a total of $1,282.93" — reliable on both formats
    m = re.search(r'For a total of\s+\$([0-9,]+\.\d{2})', text)
    if m:
        invoice.price_total = float(m.group(1).replace(',', ''))
    else:
        # Fallback: "Total $1,282.93" (Format B)
        m = re.search(r'\bTotal\s+\$([0-9,]+\.\d{2})', text)
        if m:
            invoice.price_total = float(m.group(1).replace(',', ''))
        else:
            invoice.parse_errors.append("price_total not found")
            invoice.needs_review = True

    # Sales tax (Format B has it, Format A doesn't show value)
    m = re.search(r'Sales Tax\s+\$([0-9,]+\.\d{2})', text)
    invoice.sales_tax = float(m.group(1).replace(',', '')) if m else None


def parse_line_items(text: str, invoice: AppleInvoice):
    """
    Apple item line structure (pdfplumber merges table columns):

    Format A (no Extended Price):
      IPHONE 17 PRO MAX SILVER 256GB-USAMFXG4LL/A $1,199.00 1 1
      Serial No.: (D2VK4FTFM3)

    Format B (with Extended Price):
      IPHONE 17 PRO MAX SILVER 256GB-USA MFXG4LL/A $1,199.00 1 1 $1,199.00
      Serial No.: (M17YJH7FJK)

    Pattern: one long line with product name + product number + price + qty_ordered + qty_fulfilled [+ extended]
    Anchor: line ending with digits pattern matching table columns
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Find "Order Details" header as start boundary
    details_idx = next((i for i, ln in enumerate(lines)
                        if re.match(r'Order Details', ln, re.IGNORECASE)), 0)

    # Find "Items will be invoiced" as end boundary
    end_idx = next((i for i, ln in enumerate(lines)
                    if re.search(r'Items will be invoiced', ln, re.IGNORECASE)), len(lines))

    item_lines = lines[details_idx:end_idx]

    # Item line pattern: ends with "qty qty" or "qty qty $price"
    # and contains a $ price somewhere
    item_pat = re.compile(
        r'^(.+?)\s+\$([0-9,]+\.\d{2})\s+(\d+)\s+(\d+)(?:\s+\$[0-9,]+\.\d{2})?$'
    )

    for ln in item_lines:
        m = item_pat.match(ln)
        if not m:
            continue

        raw_desc = m.group(1).strip()
        unit_price = float(m.group(2).replace(',', ''))
        qty_ordered = int(m.group(3))
        qty_fulfilled = int(m.group(4))

        # Use qty_fulfilled as actual quantity (what was actually delivered)
        quantity = qty_fulfilled if qty_fulfilled > 0 else qty_ordered

        # Split product name from product number
        # Format A: "IPHONE 17 PRO MAX SILVER 256GB-USAMFXG4LL/A" (no space before prod num)
        # Format B: "IPHONE 17 PRO MAX SILVER 256GB-USA MFXG4LL/A" (space before prod num)
        # Product number pattern: 6-12 uppercase alphanum chars, often ends in LL/A
        prod_num_m = re.search(r'(?:-USA)?\s*([A-Z0-9]{6,12}(?:/[A-Z])?)\s*$', raw_desc)
        if prod_num_m:
            product_number = prod_num_m.group(1)
            item_description = raw_desc[:prod_num_m.start()].strip()
        else:
            product_number = ''
            item_description = raw_desc

        # Clean trailing -USA country suffix from description if still present
        item_description = re.sub(r'\s*-USA\s*$', '', item_description).strip()

        line_total = round(unit_price * quantity, 2)

        invoice.items.append(LineItem(
            item_description=item_description,
            sku_model_color=product_number,
            quantity=quantity,
            unit_price=unit_price,
            line_total=line_total,
        ))

    if not invoice.items:
        invoice.parse_errors.append("No line items found")
        invoice.needs_review = True


def validate(invoice: AppleInvoice):
    if not invoice.items or invoice.price_total is None:
        return
    sum_items = round(sum(i.line_total for i in invoice.items), 2)
    # Apple total includes tax so sum_items (pre-tax) will be less than price_total
    # Just flag if item totals are zero
    if sum_items == 0:
        invoice.parse_errors.append("All line totals zero — check parser")
        invoice.needs_review = True


def parse(pdf_path: str) -> Optional[AppleInvoice]:
    text = extract_text(pdf_path)
    if not is_apple_invoice(text):
        return None
    invoice = AppleInvoice()
    parse_order_header(text, invoice)
    parse_totals(text, invoice)
    parse_line_items(text, invoice)
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
              "quantity": it.quantity, "unit_price": it.unit_price, "line_total": it.line_total}
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
    print(f"\nDB rows:")
    print(json.dumps(to_db_rows(invoice, 999, 999, "test.pdf"), indent=2, default=str))
