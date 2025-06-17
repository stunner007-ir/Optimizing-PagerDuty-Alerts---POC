from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_LEFT
from reportlab.lib import colors
import re

def parse_rca_sections(text):
    sections = re.split(r'(?=\n?\d+\.\s)', text.strip())
    parsed = []
    for section in sections:
        lines = section.strip().splitlines()
        if len(lines) >= 2:
            title = lines[0].strip()
            content = "\n".join(lines[1:]).strip()
            parsed.append((title, content))
    return parsed

def generate_rca_pdf(input_txt_file, output_pdf_file):
    with open(input_txt_file, 'r') as f:
        text = f.read()

    sections = parse_rca_sections(text)

    doc = SimpleDocTemplate(output_pdf_file, pagesize=A4,
                            rightMargin=40, leftMargin=40,
                            topMargin=60, bottomMargin=40)

    styles = getSampleStyleSheet()
    style_title = styles['Heading1']
    style_section = styles['Heading3']
    style_text = styles['BodyText']
    style_text.alignment = TA_LEFT
    style_text.leading = 15

    content = []
    content.append(Paragraph("Root Cause Analysis (RCA) Report", style_title))
    content.append(Spacer(1, 0.2 * inch))

    for title, body in sections:
        content.append(Paragraph(f"<b>{title}</b>", style_section))
        content.append(Paragraph(body.replace('\n', '<br/>'), style_text))
        content.append(Spacer(1, 0.2 * inch))

    doc.build(content)
    print(f"PDF generated: {output_pdf_file}")

# Usage
# generate_rca_pdf("rca/COKWD999_scheduled__2025-06-09T09:30:00+00:00_20250617_144358.txt", "rca/rca_pdf/rca_report_output.pdf")