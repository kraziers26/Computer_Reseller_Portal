"""
PDF watermark utility for iGamer Corp portal.
Stamps a print badge on the first page only — original DB copy stays clean.
"""
import io
from datetime import datetime


def stamp_pdf(pdf_bytes: bytes, batch_id: str, company_name: str,
              print_date: str = None) -> bytes:
    """
    Stamp a print badge on the top-right corner of page 1.
    Returns stamped PDF bytes. Original is never modified.
    """
    from pypdf import PdfReader, PdfWriter
    from reportlab.pdfgen import canvas
    from reportlab.lib.colors import black, white

    if print_date is None:
        print_date = datetime.now().strftime('%b %d, %Y')

    stamp_text = f"PRINTED  ·  {print_date}  ·  {batch_id}  ·  {company_name}"

    reader     = PdfReader(io.BytesIO(pdf_bytes))
    writer     = PdfWriter()
    first_page = reader.pages[0]
    page_w     = float(first_page.mediabox.width)
    page_h     = float(first_page.mediabox.height)

    # Build stamp overlay
    stamp_buf = io.BytesIO()
    c = canvas.Canvas(stamp_buf, pagesize=(page_w, page_h))

    font_size = 7.5
    c.setFont('Helvetica-Bold', font_size)
    text_w = c.stringWidth(stamp_text, 'Helvetica-Bold', font_size)

    pad_x, pad_y = 10, 6
    badge_w = text_w + pad_x * 2
    badge_h = font_size + pad_y * 2
    badge_x = page_w - badge_w - 20
    badge_y = page_h - badge_h - 16

    # White box with black border
    c.setFillColor(white)
    c.roundRect(badge_x, badge_y, badge_w, badge_h, 3, fill=1, stroke=0)
    c.setStrokeColor(black)
    c.setLineWidth(0.8)
    c.roundRect(badge_x, badge_y, badge_w, badge_h, 3, fill=0, stroke=1)

    # Black bold text
    c.setFillColor(black)
    c.setFont('Helvetica-Bold', font_size)
    c.drawString(badge_x + pad_x, badge_y + pad_y, stamp_text)
    c.save()
    stamp_buf.seek(0)

    stamp_page = PdfReader(stamp_buf).pages[0]

    for i, page in enumerate(reader.pages):
        if i == 0:
            page.merge_page(stamp_page)
        writer.add_page(page)

    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()
