import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'fuel_sales_by_grade'
pdf_report_start_phrase = 'Store Sales Summary Report'
pdf_report_end_phrase = 'Total Sales'

columns = [
    {'name': 'grade_number', 'dtype': 'int', 'index': 0},
    {'name': 'grade_name', 'dtype': 'string', 'index': 1},
    {'name': 'volume', 'dtype': 'float', 'index': 2},
    {'name': 'sales_amount', 'dtype': 'float', 'index': 3},
    {'name': 'percent_of_total', 'dtype': 'float', 'index': 4},
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
        skip_lead_rows=4,
        skip_trailing_rows=6,
    )

    # Logic to identify columns in parsed words
    # --> identifies phrases that belong to 1 column
    # --> every report is different
    parsed_text = parse_object.cleaned_text
    parsed_words = parsed_text.split(' ')

    '''
        CUSTOM LOGIC

        - fuels_sales_by_grade requires to remove 'Grade' from grade_number
    '''
    parsed_words = [ word for word in parsed_words if word != 'Grade' ]

    # Rebuilds parsed_text variable without 'Grade'
    parsed_text = ' '.join(parsed_words)

    column_phrases = helpers.combine_column_phrases(parsed_words)

    parsed_text = helpers.format_phrase_as_one_word(
        text=parsed_text,
        phrase_list=column_phrases
    )

    # Processing Data
    parsed_words = parsed_text.split(' ')

    data = parse_object.build_report_dictionary(
        word_list=parsed_words,
        columns=columns,
    )

    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output
