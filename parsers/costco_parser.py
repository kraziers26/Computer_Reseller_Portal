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
class CostcoInvoice:
    retailer: str = "Costco"
    order_number: Optional[str] = None
    purchase_date: Optional[str] = None
    purchase_year_month: Optional[str] = None
    card_last4: Optional[str] = None
    fulfillment_method: Optional[str] = None
    price_total: Optional[float] = None
    costco_taxes_paid: Optional[float] = None
    items: list = field(default_factory=list)
    parse_errors: list = field(default_factory=list)
    needs_review: bool = False


def extract_text(pdf_path: str) -> str:
    """Extract full text from all pages, stripping Costco page headers/footers."""
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if not page_text:
                continue
            lines = page_text.splitlines()
            cleaned = []
            for ln in lines:
                if re.match(r'^\d+/\d+/\d+,\s+\d+:\d+\s+[AP]M\s+Orders', ln): continue
                if re.search(r'costco\.com/OrderDetailPrintView', ln): continue
                if re.match(r'^\s*\d+/\d+\s*$', ln.strip()): continue
                cleaned.append(ln)
            pages.append('\n'.join(cleaned))
    return '\n'.join(pages)


def is_costco_invoice(text: str) -> bool:
    indicators = [
        r'costco\.com/OrderDetailPrintView',
        r'COSTCO\s+WHOLESALE',
        r'Membership Number',
        r'Order Number.*Payment Method',
    ]
    return any(re.search(p, text, re.IGNORECASE) for p in indicators)


def parse_order_header(text: str, invoice: CostcoInvoice):
    # Order number — long digit sequence on line after header labels
    m = re.search(r'(?:Order Number.*?\n)(\d{7,})', text, re.DOTALL)
    if not m:
        m = re.search(r'\b(\d{9,10})\b', text[:500])
    if m:
        invoice.order_number = m.group(1).strip()
    else:
        invoice.parse_errors.append("order_number not found")
        invoice.needs_review = True

    # Order date — label and value separated by address column text
    m = re.search(r'Order Date\s*\n[^\n]*\n\s*(\d{2}/\d{2}/\d{4})', text)
    if not m:
        m = re.search(r'\b(\d{2}/\d{2}/\d{4})\b', text)
    if m:
        dt = datetime.strptime(m.group(1), "%m/%d/%Y")
        invoice.purchase_date = dt.strftime("%Y-%m-%d")
        invoice.purchase_year_month = dt.strftime("%Y-%m")
    else:
        invoice.parse_errors.append("purchase_date not found")
        invoice.needs_review = True

    # Card last 4
    m = re.search(r'ending in (\d{4})', text, re.IGNORECASE)
    if m:
        invoice.card_last4 = m.group(1)
    else:
        invoice.parse_errors.append("card_last4 not found")
        invoice.needs_review = True


def parse_order_summary(text: str, invoice: CostcoInvoice):
    m = re.search(r'Order Total\s*\$([0-9,]+\.\d{2})', text)
    if m:
        invoice.price_total = float(m.group(1).replace(',', ''))
    else:
        invoice.parse_errors.append("price_total not found")
        invoice.needs_review = True

    m = re.search(r'\bTax\s+\$([0-9,]+\.\d{2})', text)
    invoice.costco_taxes_paid = float(m.group(1).replace(',', '')) if m else 0.0


def parse_line_items(text: str, invoice: CostcoInvoice):
    """
    After stripping page headers, each item block looks like:
      <description line>  <qty> <status> $<line_total>   ← pdfplumber flattens columns
      [continuation description lines]
      Item #<sku>
      $<unit_price>
      Discount $<amount>   ← optional
    """
    summary_start = re.search(r'Order Summary', text)
    items_section = text[:summary_start.start()] if summary_start else text

    header_m = re.search(r'Item\s+Quantity\s+Status\s+Total Price', items_section)
    items_text = items_section[header_m.end():].strip() if header_m else items_section

    lines = [ln.strip() for ln in items_text.splitlines()]
    lines = [ln for ln in lines if ln]

    item_idx = [i for i, ln in enumerate(lines) if re.match(r'^Item #\d+$', ln)]

    if not item_idx:
        invoice.parse_errors.append("No line items found")
        invoice.needs_review = True
        return

    invoice.fulfillment_method = "Delivery"

    for idx, sku_line_idx in enumerate(item_idx):
        sku = re.search(r'Item #(\d+)', lines[sku_line_idx]).group(1)

        # Description block: lines from after previous item's unit/discount lines up to this Item #
        prev_end = 0
        if idx > 0:
            prev_sku_idx = item_idx[idx - 1]
            # Skip past unit price and discount lines after previous Item #
            prev_end = prev_sku_idx + 1
            while prev_end < sku_line_idx:
                ln = lines[prev_end]
                if re.match(r'^\$[\d,]+\.\d{2}$', ln) or re.match(r'^Discount \$', ln):
                    prev_end += 1
                else:
                    break

        desc_lines = []
        qty = 1
        line_total = 0.0

        for ln in lines[prev_end:sku_line_idx]:
            # First line of item has qty/status/total appended by pdfplumber
            m = re.search(r'^(.*?)\s+(\d+)\s+(?:Delivered|Shipped|Processing|Cancelled)\s+\$([0-9,]+\.\d{2})\s*$', ln)
            if m:
                desc_part = m.group(1).strip()
                qty = int(m.group(2))
                line_total = float(m.group(3).replace(',', ''))
                if desc_part:
                    desc_lines.append(desc_part)
            else:
                desc_lines.append(ln)

        item_description = ' '.join(desc_lines).strip()

        # Unit price — line immediately after "Item #XXXXX"
        unit_price = 0.0
        if sku_line_idx + 1 < len(lines):
            up_m = re.match(r'^\$([0-9,]+\.\d{2})$', lines[sku_line_idx + 1])
            if up_m:
                unit_price = float(up_m.group(1).replace(',', ''))

        invoice.items.append(LineItem(
            item_description=item_description,
            sku_model_color=f"Item #{sku}",
            quantity=qty,
            unit_price=unit_price,
            line_total=line_total,
        ))


def validate(invoice: CostcoInvoice):
    if not invoice.items or invoice.price_total is None:
        return
    if all(i.line_total == 0 for i in invoice.items):
        invoice.parse_errors.append("All line totals are zero")
        invoice.needs_review = True


def parse(pdf_path: str) -> Optional[CostcoInvoice]:
    text = extract_text(pdf_path)
    if not is_costco_invoice(text):
        return None
    invoice = CostcoInvoice()
    parse_order_header(text, invoice)
    parse_order_summary(text, invoice)
    parse_line_items(text, invoice)
    validate(invoice)
    return invoice


def to_db_rows(invoice: CostcoInvoice, user_id: int, company_id: int,
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
        "costco_taxes_paid":   invoice.costco_taxes_paid,
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
        print("Usage: python costco_parser.py <path_to_pdf>")
        sys.exit(1)

    invoice = parse(pdf)
    if not invoice:
        print("Not a Costco invoice.")
        sys.exit(1)

    print(f"\n{'='*50}\nCOSTCO INVOICE PARSED\n{'='*50}")
    print(f"Order #:       {invoice.order_number}")
    print(f"Date:          {invoice.purchase_date}")
    print(f"Card last 4:   {invoice.card_last4}")
    print(f"Fulfillment:   {invoice.fulfillment_method}")
    print(f"Order Total:   ${invoice.price_total:,.2f}")
    print(f"Taxes paid:    ${invoice.costco_taxes_paid:,.2f}")
    print(f"Needs review:  {invoice.needs_review}")
    if invoice.parse_errors:
        print(f"Errors:        {invoice.parse_errors}")
    print(f"\nLine Items ({len(invoice.items)}):")
    for i, item in enumerate(invoice.items, 1):
        print(f"  {i}. {item.item_description[:65]}")
        print(f"     SKU: {item.sku_model_color}  |  Qty: {item.quantity}  |  "
              f"Unit: ${item.unit_price:,.2f}  |  Total: ${item.line_total:,.2f}")
    print(f"\nDB rows:")
    print(json.dumps(to_db_rows(invoice, 999, 999, "test.pdf"), indent=2, default=str))
