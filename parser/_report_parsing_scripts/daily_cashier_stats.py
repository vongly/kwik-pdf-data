import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'daily_cashier_stats'
pdf_report_start_phrase = 'Store Till Summary Cashier Statistics'
pdf_report_end_phrase = 'Test Fuel'

columns = [
    {'name': 'description', 'dtype' : 'string', 'index': 0},
    {'name': 'transaction_count', 'dtype' : 'int', 'index': 1},
    {'name': 'transaction_amount', 'dtype' : 'float', 'index': 2},
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
        add_more_characters=30,
        skip_trailing_rows=1,
        skip_mid_rows=['Sales', 'Cash', 'Other', 'Customers', 'No', 'CarWash', 'ReWash'],
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
