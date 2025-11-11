import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from _core.pdf_parser import ExtractReport
from _util import helpers

table_name = 'daily_sales_summary'
pdf_report_start_phrase = 'Store Sales Summary Report'
pdf_report_end_phrase = 'Store Tender Reading'

columns = [
    {'name': 'summary_description', 'dtype': 'string', 'index': 0},
    {'name': 'amount', 'dtype': 'float', 'index': 1},
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
        skip_lead_rows=8,
        skip_trailing_rows=2,
    )

    '''
        CUSTOM LOGIC

        - Total Fuel Sales has a Volume amount that needs to be removed
            -> the value is identified as a float w/o a '$' symbol
    '''
    parsed_text = parse_object.cleaned_text
    parsed_words = parsed_text.split(' ')
    
    for word in parsed_words:
        try:
            float(word.replace(',',''))
            parsed_words.remove(word)
        except:
            pass

    # Rebuilds parsed_text variable without Total Fuel Sales Volume
    parsed_text = ' '.join(parsed_words)

    # Logic to identify columns in parsed words
    # --> identifies phrases that belong to 1 column
    # --> every report is different
    parsed_text = parsed_text
    parsed_words = parsed_text.split(' ')

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
