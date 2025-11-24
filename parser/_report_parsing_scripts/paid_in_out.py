import sys
from pathlib import Path
import dateutil

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'paid_in_out'

columns = [
    {'name': 'transaction_timestamp', 'dtype': 'timestamp', 'index': 0},
    {'name': 'account_description', 'dtype': 'string', 'index': 1},
    {'name': 'amount', 'dtype': 'float', 'index': 2},
    {'name': 'tender_type', 'dtype': 'string', 'index': 3},
]


def sub_parse_pdf(filename, filepath, processed_utc, paid_in_or_out, s3_client=None):
    if paid_in_or_out == 'paid_in':
        pdf_report_start_phrase = 'Paid In\nDate Time'
        pdf_report_end_phrase = 'Paid In Tender Totals'
    elif paid_in_or_out == 'paid_out':
        pdf_report_start_phrase = 'Paid Out\nDate Time'
        pdf_report_end_phrase = 'Paid Out Tender Totals'

    parse_object = ExtractReport(
        filename=filename,
        filepath=filepath,
        pdf_report_start_phrase=pdf_report_start_phrase,
        pdf_report_end_phrase=pdf_report_end_phrase,
        table_name=table_name,
        processed_utc=processed_utc,
        s3_client=s3_client,
    )

    parse_object.output['sub_report_name'] = paid_in_or_out

    output = parse_object.output.copy()

    # Checks if Report is in PDF
    if not parse_object.check_for_report():
        output['has_report'] = False
        output['status'] = 'success'

        data = parse_object.build_report_dictionary(
            word_list=None,
            columns=columns,
            has_report=output['has_report'],
        )

        output['data'] = data

        return output

    # Pulls Report Text
    parse_object.get_report_text(
        skip_lead_rows=2,
        skip_trailing_rows=2,
    )

    '''
        CUSTOM LOGIC

        - If the report contains 'No Data To Report'
        - skip
    '''

    text = helpers.get_report_text(
        raw_text=parse_object.raw_text,
        report_start_phrase=pdf_report_start_phrase,
        report_end_phrase=pdf_report_end_phrase,
    )

    if 'No Data To Report' in text:
        parse_object.output['has_report'] = False
        parse_object.output['status'] = 'success'
        parse_object.output['data'] = 'No data to report'
        return parse_object.output


    # Logic to identify columns in parsed words
    # --> identifies phrases that belong to 1 column
    # --> every report is different
    parsed_text = parse_object.cleaned_text
    parsed_words = parsed_text.split(' ')

    column_phrases = []
    phrase_parts = []

    for i, word in enumerate(parsed_words):
        try:
            int(word)
        except:
            try:
                dateutil.parser.parse(word)
                if phrase_parts:
                    column_phrases.append(' '.join(phrase_parts))
                    phrase_parts = []
                continue
            except:
                pass

        if word in ['AM', 'PM'] or word.startswith('end'):
            continue

        if '$' in word:
            is_string = False
        else:
            is_string = True

        if is_string:
            phrase_parts.append(word)
        if not is_string and phrase_parts:
            column_phrases.append(' '.join(phrase_parts))
            phrase_parts = []

    parsed_text = helpers.format_phrase_as_one_word(
        text=parsed_text,
        phrase_list=column_phrases
    )

    parsed_words = parsed_text.split(' ')
    parsed_words = helpers.parse_datetime_parts(word_list=parsed_words)

    # Processing Data
    data = parse_object.build_report_dictionary(
        word_list=parsed_words,
        columns=columns,
        transaction_type=paid_in_or_out,

    )
    
    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output

def parse_pdf(paid_in_or_out_values = ['paid_in', 'paid_out'], **kwargs):

    outputs_sub_report = []

    for paid_in_or_out in paid_in_or_out_values:
        merged_kwargs = {**kwargs, 'paid_in_or_out': paid_in_or_out}

        output_sub_report = sub_parse_pdf(**merged_kwargs)
        outputs_sub_report.append(output_sub_report)

    # Reconstructs Output
    output = {
        'has_report': False,
        'status': None,
        'table_name': table_name,
        'report_id': outputs_sub_report[0]['report_id'],
        'data': None,
    }

    if all(op.get('status') == 'success' for op in outputs_sub_report):
        output['status'] = 'success'
    else:
        output['has_report'] = False
        output['status'] = 'failed'
        output['data'] = 'One or more jobs failed: ' + str(outputs_sub_report)

        return output

    if any(op.get('has_report') == True for op in outputs_sub_report):
        output['has_report'] = True

        for output_stage in outputs_sub_report:
            if output_stage['has_report']:
                if output['data']:
                    output['data'].extend(output_stage['data'])
                else:
                    output['data'] = output_stage['data']

    return output