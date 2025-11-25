import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'card_transaction_counts'
pdf_report_start_phrase = 'Transaction Count by Card Type'
pdf_report_end_phrase = 'METHOD OF PAYMENT TOTALS REPORT'

columns = [
    {'name': 'card_type', 'data_type': 'string', 'index': 0},
    {'name': 'count', 'data_type': 'int', 'index': 1},
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
    parse_object.get_report_text()

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

    # Processing Data
    parsed_words = parsed_text.split(' ')
    
    data = parse_object.build_report_dictionary(
        columns=columns,
        word_list=parsed_words,
    )

    output = helpers.output_for_report(output, data)

    return output
