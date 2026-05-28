import re
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional
import pdfplumber


@dataclass
class LineItem:
    item_description: str
    sku_model_color: str   # Amazon has no SKU on print view — stores color/variant if any
    quantity: int
    unit_price: float
    line_total: float


@dataclass
class AmazonInvoice:
    retailer: str = "Amazon"
    order_number: Optional[str] = None
    purchase_date: Optional[str] = None
    purchase_year_month: Optional[str] = None
    card_last4: Optional[str] = None
    fulfillment_method: str = "Delivery"   # Amazon is always delivery
    price_total: Optional[float] = None
    items: list = field(default_factory=list)
    parse_errors: list = field(default_factory=list)
    needs_review: bool = False
    _format: str = "unknown"   # A_en, A_es, B for internal tracking


# ── Spanish month map ─────────────────────────────────────────────────────────
ES_MONTHS = {
    'enero': 1, 'febrero': 2, 'marzo': 3, 'abril': 4,
    'mayo': 5, 'junio': 6, 'julio': 7, 'agosto': 8,
    'septiembre': 9, 'octubre': 10, 'noviembre': 11, 'diciembre': 12,
}

EN_MONTHS = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
}


# ── Spanish → English product description translator ─────────────────────────
# Covers the vocabulary that commonly appears in Amazon tech/electronics titles.
# Brand names, model numbers, and spec values (digits, units, acronyms) are
# left untouched. Phrases are sorted longest-first so multi-word entries always
# match before their component words are consumed.

_ES_PHRASES: list[tuple[str, str]] = [
    # Multi-word phrases
    ("para juegos",             "Gaming"),
    ("para portátil",           "Laptop"),
    ("para laptop",             "Laptop"),
    ("de escritorio",           "Desktop"),
    ("disco duro",              "Hard Drive"),
    ("tarjeta gráfica",         "Graphics Card"),
    ("tarjeta de video",        "Video Card"),
    ("tarjeta de sonido",       "Sound Card"),
    ("unidad de estado sólido", "Solid State Drive"),
    ("placa base",              "Motherboard"),
    ("placa madre",             "Motherboard"),
    ("fuente de alimentación",  "Power Supply"),
    ("tasa de refresco",        "Refresh Rate"),
    ("tiempo de respuesta",     "Response Time"),
    ("duración de batería",     "Battery Life"),
    ("carga rápida",            "Fast Charging"),
    ("carga inalámbrica",       "Wireless Charging"),
    ("resistente al agua",      "Water Resistant"),
    ("a prueba de agua",        "Waterproof"),
    ("compatible con",          "Compatible with"),
    ("funciona con",            "Works with"),
    ("control remoto",          "Remote Control"),
    ("gris espacial",           "Space Gray"),
    ("espacial gris",           "Space Gray"),
    ("azul medianoche",         "Midnight Blue"),
    ("blanco estrella",         "Starlight"),
    ("velocidad de",            "Speed"),
    ("más de",                  "over"),
    ("hasta",                   "up to"),
    ("incluye",                 "includes"),
    ("núcleos",                 "Cores"),
    ("subprocesos",             "Threads"),
    ("pulgadas",                "inches"),
    # Single words
    ("procesador",    "Processor"),
    ("pantalla",      "Display"),
    ("teclado",       "Keyboard"),
    ("ratón",         "Mouse"),
    ("memoria",       "Memory"),
    ("almacenamiento","Storage"),
    ("batería",       "Battery"),
    ("cargador",      "Charger"),
    ("adaptador",     "Adapter"),
    ("portátil",      "Laptop"),
    ("auriculares",   "Headphones"),
    ("audífonos",     "Earphones"),
    ("altavoz",       "Speaker"),
    ("parlante",      "Speaker"),
    ("cámara",        "Camera"),
    ("micrófono",     "Microphone"),
    ("impresora",     "Printer"),
    ("escáner",       "Scanner"),
    ("televisor",     "TV"),
    ("controlador",   "Controller"),
    ("mando",         "Controller"),
    ("consola",       "Console"),
    ("tableta",       "Tablet"),
    ("inalámbrico",   "Wireless"),
    ("inalámbrica",   "Wireless"),
    ("plateado",      "Silver"),
    ("plateada",      "Silver"),
    ("dorado",        "Gold"),
    ("dorada",        "Gold"),
    ("negro",         "Black"),
    ("negra",         "Black"),
    ("blanco",        "White"),
    ("blanca",        "White"),
    ("gris",          "Gray"),
    ("rojo",          "Red"),
    ("roja",          "Red"),
    ("azul",          "Blue"),
    ("verde",         "Green"),
    ("amarillo",      "Yellow"),
    ("naranja",       "Orange"),
    ("rosa",          "Pink"),
    ("morado",        "Purple"),
    ("violeta",       "Purple"),
    # Short connectors — must come last so content words match first
    ("con",  "with"),
    ("para", "for"),
    ("y",    "and"),
    ("o",    "or"),
    ("en",   "in"),
    ("de",   ""),   # spec connector: "SSD Gen 4 de 1TB" → "SSD Gen 4 1TB"
]

def _build_translate_re() -> tuple[re.Pattern, dict[str, str]]:
    sorted_phrases = sorted(_ES_PHRASES, key=lambda t: len(t[0]), reverse=True)
    pattern = '|'.join(
        r'(?<!\w)' + re.escape(phrase) + r'(?!\w)'
        for phrase, _ in sorted_phrases
    )
    phrase_map = {phrase.lower(): english for phrase, english in sorted_phrases}
    return re.compile(pattern, re.IGNORECASE), phrase_map

_TRANSLATE_RE, _PHRASE_MAP = _build_translate_re()


def translate_es_to_en(text: str) -> str:
    """
    Translate Spanish Amazon product description fragments to English using
    a static phrase map. Brand names, model numbers, and spec values are
    preserved unchanged. Collapses any double-spaces left by empty replacements
    (e.g. "de" → "").
    """
    def _replace(m: re.Match) -> str:
        return _PHRASE_MAP.get(m.group(0).lower(), m.group(0))

    translated = _TRANSLATE_RE.sub(_replace, text)
    return re.sub(r' {2,}', ' ', translated).strip()


# ── Helpers ───────────────────────────────────────────────────────────────────
def parse_amount(s: str) -> float:
    """Parse US$1,797.00 or $1,797.00 into float."""
    s = re.sub(r'US\$|,', '', s.strip())
    s = s.replace('$', '').strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


def extract_text(pdf_path: str) -> str:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                # Strip timestamp header lines
                t = re.sub(r'^\d+/\d+/\d+,?\s+\d+:\d+\s*[ap]\.?m\.?\s+.*$', '',
                           t, flags=re.MULTILINE | re.IGNORECASE)
                # Strip footer URL lines
                t = re.sub(r'^https://www\.amazon\.com.*$', '', t,
                           flags=re.MULTILINE)
                pages.append(t)
    return '\n'.join(pages)


def is_amazon_invoice(text: str) -> bool:
    return bool(re.search(
        r'amazon\.com|Resumen del pedido|Order Summary|Final Details for Order',
        text, re.IGNORECASE))


def detect_format(text: str) -> str:
    """Detect which Amazon invoice format this is."""
    if re.search(r'Final Details for Order', text, re.IGNORECASE):
        return 'B'
    if re.search(r'Resumen del pedido|N\.º de pedido|Pedido realizado', text):
        return 'A_es'
    return 'A_en'


# ── Order number ──────────────────────────────────────────────────────────────
def parse_order_number(text: str, invoice: AmazonInvoice):
    patterns = [
        r'N\.º de pedido\s+([\d-]+)',
        r'Order #\s*([\d-]+)',
        r'order number:\s*([\d-]+)',
        r'Order #?\s*([\d]{3}-[\d]+-[\d]+)',
        r'orderID=([\d-]+)',
        r'Order\s+#\s*([\d]{3}-[\d]+-[\d]+)',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            invoice.order_number = m.group(1).strip()
            return
    invoice.parse_errors.append("order_number not found")
    invoice.needs_review = True


# ── Purchase date ─────────────────────────────────────────────────────────────
def parse_date(text: str, invoice: AmazonInvoice):
    # Spanish: "Pedido realizado 13 de marzo de 2026"
    m = re.search(r'Pedido realizado\s+(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})', text)
    if m:
        day, month_es, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
        month = ES_MONTHS.get(month_es)
        if month:
            invoice.purchase_date = f"{year}-{month:02d}-{day:02d}"
            invoice.purchase_year_month = f"{year}-{month:02d}"
            return

    # English Format A: "Order placed March 5, 2026"
    m = re.search(r'Order placed\s+(\w+ \d{1,2},\s*\d{4})', text, re.IGNORECASE)
    if m:
        dt = datetime.strptime(m.group(1).strip(), "%B %d, %Y")
        invoice.purchase_date = dt.strftime("%Y-%m-%d")
        invoice.purchase_year_month = dt.strftime("%Y-%m")
        return

    # English Format B: "Order Placed: February 9, 2026"
    m = re.search(r'Order Placed:\s*(\w+ \d{1,2},\s*\d{4})', text, re.IGNORECASE)
    if m:
        dt = datetime.strptime(m.group(1).strip(), "%B %d, %Y")
        invoice.purchase_date = dt.strftime("%Y-%m-%d")
        invoice.purchase_year_month = dt.strftime("%Y-%m")
        return

    invoice.parse_errors.append("purchase_date not found")
    invoice.needs_review = True


# ── Card last 4 ───────────────────────────────────────────────────────────────
def parse_card(text: str, invoice: AmazonInvoice):
    patterns = [
        r'termina en (\d{4})',
        r'ending in\s*(\d{4})',
        r'Last digits:\s*(\d{4})',
        r'(?:Visa|Mastercard|Amex|American Express|Discover).*?(\d{4})\b',
        r'Card ending in (\d{4})',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            invoice.card_last4 = m.group(1)
            return

    # Cross-line fallback: pdfplumber merges columns so card number may wrap
    lines_list = text.splitlines()
    for i, ln in enumerate(lines_list):
        if not re.search(r'ending in', ln, re.IGNORECASE):
            continue
        m = re.search(r'ending in[^0-9]{0,20}?([0-9]{4})(?![0-9])', ln, re.IGNORECASE)
        if m:
            invoice.card_last4 = m.group(1)
            return
        for j in range(i + 1, min(i + 3, len(lines_list))):
            nxt = lines_list[j]
            candidates = re.findall(r'(?<=[\s,])([0-9]{4})(?=[\s])', nxt)
            for cand in candidates:
                if re.match(r'(19[0-9]{2}|20[012][0-9])', cand):
                    continue
                invoice.card_last4 = cand
                return

    invoice.parse_errors.append("card_last4 not found")
    invoice.needs_review = True


# ── Price total ───────────────────────────────────────────────────────────────
def parse_total(text: str, invoice: AmazonInvoice):
    # Rewards order — Grand Total is $0.00, use Item(s) Subtotal
    rewards_m = re.search(r'Rewards Points:\s*-[US\$]*([\d,]+\.\d{2})', text)
    if rewards_m:
        sub_m = re.search(r'Item\(?s\)? Subtotal:\s*[US\$]*([\d,]+\.\d{2})', text)
        if sub_m:
            invoice.price_total = parse_amount(sub_m.group(1))
            return

    # Format B
    m = re.search(r'Order Total:\s*\$?([\d,]+\.\d{2})', text)
    if m:
        invoice.price_total = parse_amount(m.group(1))
        return

    # Format A_es
    m = re.search(r'Total \(I\.V\.A\. Incluido\):\s*(US\$[\d,]+\.\d{2})', text)
    if m:
        invoice.price_total = parse_amount(m.group(1))
        return

    # Format A_en
    m = re.search(r'Grand Total:\s*\$?([\d,]+\.\d{2})', text)
    if m:
        val = parse_amount(m.group(1))
        if val > 0:
            invoice.price_total = val
            return
        sub_m = re.search(r'Item\(?s\)? Subtotal:\s*\$?([\d,]+\.\d{2})', text)
        if sub_m:
            invoice.price_total = parse_amount(sub_m.group(1))
            return

    invoice.parse_errors.append("price_total not found")
    invoice.needs_review = True


# ── Line items ────────────────────────────────────────────────────────────────
def parse_items_format_b(text: str, invoice: AmazonInvoice):
    """
    Format B (Final Details): Items use "N of: description ... $price" pattern.
    Multiple shipment blocks possible — same item in multiple shipments = consolidate.
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    payment_idx = next((i for i, ln in enumerate(lines)
                        if re.match(r'Payment information', ln, re.IGNORECASE)), len(lines))
    items_section_lines = lines[:payment_idx]

    item_pattern = re.compile(r'^(\d+) of:\s+(.+?)\s+\$([\d,]+\.\d{2})$')

    raw_items = []
    i = 0
    while i < len(items_section_lines):
        ln = items_section_lines[i]
        m = item_pattern.match(ln)
        if m:
            quantity = int(m.group(1))
            desc_part = m.group(2).strip()
            unit_price = parse_amount(m.group(3))

            j = i + 1
            while j < len(items_section_lines):
                next_ln = items_section_lines[j]
                if re.match(
                    r'(Sold by|Vendido por|Condition:|Shipping Address:|'
                    r'Shipping Speed:|-----|Business Price|Delivered by)',
                    next_ln, re.IGNORECASE):
                    break
                if item_pattern.match(next_ln):
                    break
                desc_part += ' ' + next_ln.strip()
                j += 1

            desc_part = re.sub(r'\s+', ' ', desc_part).strip()
            desc_part = re.sub(
                r'\s*(Sold by.*|Business Price.*|Condition:.*|Delivered by.*)$',
                '', desc_part, flags=re.IGNORECASE).strip()
            desc_part = re.sub(
                r'^(Delivered by \S+\s*)', '', desc_part, flags=re.IGNORECASE).strip()
            line_total = round(unit_price * quantity, 2)

            raw_items.append(LineItem(
                item_description=desc_part,
                sku_model_color='',
                quantity=quantity,
                unit_price=unit_price,
                line_total=line_total,
            ))
            i = j
        else:
            i += 1

    seen = {}
    for item in raw_items:
        key = (item.item_description[:80], item.unit_price)
        if key in seen:
            seen[key].quantity += item.quantity
            seen[key].line_total = round(seen[key].line_total + item.line_total, 2)
        else:
            seen[key] = item

    invoice.items = list(seen.values())


def parse_items_format_a(text: str, invoice: AmazonInvoice, lang: str):
    """
    Format A (Order Summary web view): Items don't have explicit qty prefix.
    Quantity appears as a standalone digit (circled in UI, plain digit in PDF).

    Structure per item (pdfplumber flattens 2-column layout):
      <description line 1>
      <description line 2>
      Sold by: Amazon.com  /  Vendido por: Amazon.com
      [Supplied by: Other / Proporcionado por: Otro]
      [Return or replace items: ...]
      N              ← quantity; may render as bare "4", "4)", or "(4)"
      $unit_price    ← or US$unit_price

    Changes (2026-05):
      FIX 1 — Added "Entregado" to delivery header detection (Spanish past-tense).
      FIX 2 — Added "Tu paquete" to delivery note skip list (Spanish delivery note).
      FIX 3 — Quantity regex now handles "(N)" and "N)" badge renderings.
      NEW   — A_es item descriptions are translated to English via translate_es_to_en().
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    # Strip footer lines
    cleaned = []
    for ln in lines:
        if re.match(r'(Conditions of Use|Condiciones de uso|Back to top|Inicio de|'
                    r'© 199\d)', ln, re.IGNORECASE):
            break
        cleaned.append(ln)
    lines = cleaned

    sold_by_pat = re.compile(
        r'^(Sold by:|Vendido por:|Supplied by:|Proporcionado por:)', re.IGNORECASE)
    return_pat = re.compile(r'^(Return or replace|Devolver)', re.IGNORECASE)

    # FIX 1: "Entregado" added — Spanish past-tense delivery header was missing,
    # causing item_start to stay at 0 and header lines to bleed into descriptions.
    item_start = 0
    for i, ln in enumerate(lines):
        if re.match(r'(Delivered|Arriving|Llega el|Entregado|Your package)', ln, re.IGNORECASE):
            item_start = i + 1
            break

    # FIX 2: "Tu paquete" added — Spanish delivery note was not being skipped.
    while item_start < len(lines) and re.match(
            r'(Your package|We.ll hold|Left at|Package was|Tu paquete)',
            lines[item_start], re.IGNORECASE):
        item_start += 1

    item_lines = lines[item_start:]

    blocks = []
    current_block = []
    for ln in item_lines:
        if re.match(r'^(US\$|\$)[\d,]+\.\d{2}$', ln):
            current_block.append(ln)
            blocks.append(current_block)
            current_block = []
        else:
            current_block.append(ln)
    if current_block:
        blocks.append(current_block)

    for block in blocks:
        if not block:
            continue

        price_line = block[-1] if re.match(r'^(US\$|\$)', block[-1]) else None
        if not price_line:
            continue
        unit_price = parse_amount(price_line)
        if unit_price == 0.0:
            continue

        # FIX 3: quantity badge may render as bare "4", "4)", or "(4)".
        quantity = 1
        desc_lines = []
        for ln in block[:-1]:
            qty_m = re.match(r'^\(?(\d+)\)?$', ln)
            if qty_m and int(qty_m.group(1)) <= 99:
                quantity = int(qty_m.group(1))
            elif sold_by_pat.match(ln) or return_pat.match(ln):
                continue
            elif re.match(r'(Your package|Left at|Package was|Delivering|Estimated delivery)',
                          ln, re.IGNORECASE):
                continue
            else:
                desc_lines.append(ln)

        item_description = ' '.join(desc_lines).strip()
        item_description = re.sub(r'\s+', ' ', item_description)
        item_description = re.sub(
            r'\s*(Sold by.*|Business Price.*|Condition:.*|Delivered by Amazon.*)$',
            '', item_description, flags=re.IGNORECASE).strip()
        item_description = re.sub(
            r'^(Delivered by \S+\s*)', '', item_description, flags=re.IGNORECASE).strip()

        # Translate Spanish descriptions to English
        if item_description and lang == 'A_es':
            item_description = translate_es_to_en(item_description)

        line_total = round(unit_price * quantity, 2)

        if item_description:
            invoice.items.append(LineItem(
                item_description=item_description,
                sku_model_color='',
                quantity=quantity,
                unit_price=unit_price,
                line_total=line_total,
            ))


def parse_items(text: str, invoice: AmazonInvoice):
    if invoice._format == 'B':
        parse_items_format_b(text, invoice)
    else:
        parse_items_format_a(text, invoice, invoice._format)


def validate(invoice: AmazonInvoice):
    if not invoice.items:
        invoice.parse_errors.append("No line items found")
        invoice.needs_review = True
        return
    sum_items = round(sum(i.line_total for i in invoice.items), 2)
    if invoice.price_total and abs(sum_items - invoice.price_total) > 0.50:
        invoice.parse_errors.append(
            f"Item totals (${sum_items}) vs Order Total (${invoice.price_total}) — check")
        invoice.needs_review = True


def parse(pdf_path: str) -> Optional[AmazonInvoice]:
    text = extract_text(pdf_path)
    if not is_amazon_invoice(text):
        return None
    invoice = AmazonInvoice()
    invoice._format = detect_format(text)
    parse_order_number(text, invoice)
    parse_date(text, invoice)
    parse_card(text, invoice)
    parse_total(text, invoice)
    parse_items(text, invoice)
    validate(invoice)
    return invoice


def to_db_rows(invoice: AmazonInvoice, user_id: int, company_id: int,
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
        print("Usage: python amazon_parser.py <path>"); sys.exit(1)

    invoice = parse(pdf)
    if not invoice:
        print("Not an Amazon invoice."); sys.exit(1)

    print(f"\n{'='*55}")
    print(f"AMAZON INVOICE PARSED  [format: {invoice._format}]")
    print(f"{'='*55}")
    print(f"Order #:      {invoice.order_number}")
    print(f"Date:         {invoice.purchase_date}")
    print(f"Card last 4:  {invoice.card_last4}")
    print(f"Fulfillment:  {invoice.fulfillment_method}")
    print(f"Order Total:  ${invoice.price_total:,.2f}" if invoice.price_total else "Order Total:  None")
    print(f"Needs review: {invoice.needs_review}")
    if invoice.parse_errors:
        print(f"Errors:       {invoice.parse_errors}")
    print(f"\nLine Items ({len(invoice.items)}):")
    for i, item in enumerate(invoice.items, 1):
        print(f"  {i}. {item.item_description[:70]}")
        print(f"     Qty: {item.quantity}  |  Unit: ${item.unit_price:,.2f}  |  Total: ${item.line_total:,.2f}")
