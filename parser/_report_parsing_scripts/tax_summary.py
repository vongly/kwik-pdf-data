import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'tax_summary'
pdf_report_start_phrase = 'Tax Collection Summary Report'
pdf_report_end_phrase = 'Car Wash Tax Report'

columns = [
    {'name': 'tax_type', 'data_type': 'string', 'index': 0},
    {'name': 'tax_collected', 'data_type': 'float', 'index': 1},
    {'name': 'sales_amount', 'data_type': 'float', 'index': 2},
    {'name': 'sales_forgiven_amount', 'data_type': 'float', 'index': 3},
    {'name': 'actual_sales', 'data_type': 'float', 'index': 4},
    {'name': 'exempt_amount', 'data_type': 'float', 'index': 5},
]

def parse_pdf(filename, filepath, processed_utc, s3_client=None):
    parse_object = ExtractReport(
        filename=filename,
        filepath=filepath,
        pdf_report_start_phrase=pdf_report_start_phrase,
        pdf_report_end_phrase=pdf_report_end_phrase,
        table_name=table_name,
        processed_utc=processed_utc,
        s3_client=s3_client,
    )

    output = parse_object.output.copy()

    # Checks if Report is in PDF
    if not parse_object.check_for_report():
        output = helpers.output_for_no_report(
            output=output,
            columns=columns,
            report_id=parse_object.report_id,
            store_id=parse_object.store_id,
            report_name=parse_object.table_name,
            original_filename=parse_object.filename,
            processed_utc=parse_object.processed_utc,
        )

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

    output = helpers.output_for_report(output, data)

    return output
