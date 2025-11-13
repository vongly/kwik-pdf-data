import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'tax_summary'
pdf_report_start_phrase = 'Tax Collection Summary Report'
pdf_report_end_phrase = 'Car Wash Tax Report'

columns = [
    {'name': 'tax_type', 'dtype': 'string', 'index': 0},
    {'name': 'tax_collected', 'dtype': 'float', 'index': 1},
    {'name': 'sales_amount', 'dtype': 'float', 'index': 2},
    {'name': 'sales_forgiven_amount', 'dtype': 'float', 'index': 3},
    {'name': 'actual_sales', 'dtype': 'float', 'index': 4},
    {'name': 'exempt_amount', 'dtype': 'float', 'index': 5},
]

def parse_pdf(filename, filepath, processed_utc):
    parse_object = ExtractReport(
        filename=filename,
        filepath=filepath,
        pdf_report_start_phrase=pdf_report_start_phrase,
        pdf_report_end_phrase=pdf_report_end_phrase,
        table_name=table_name,
        processed_utc=processed_utc,
    )

    output = parse_object.output.copy()

    # Checks if Report is in PDF
    if not parse_object.check_for_report():
        output['has_report'] = False
        output['status'] = 'success'
        output['data'] = 'No Report: Required phrases not in report'

        return output

    # Pulls Report Text
    parse_object.get_report_text(
        skip_lead_rows=5,
        skip_trailing_rows=1,
    )

    # Logic to identify columns in parsed words
    # --> identifies phrases that belong to 1 column
    # --> every report is different
    parsed_text = parse_object.cleaned_text
    parsed_words = parsed_text.split(' ')

    column_phrases = helpers.combine_column_phrases(parsed_words)

    parsed_text = helpers.format_phrase_as_one_word(
        text=parsed_text,
        phrase_list=column_phrases
    )

    parsed_words = parsed_text.split(' ')

    # Processing Data
    data = parse_object.build_report_dictionary(
        word_list=parsed_words,
        columns=columns,
    )

    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output
