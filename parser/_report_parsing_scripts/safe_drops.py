import sys
from pathlib import Path
import dateutil

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers, safe_drops

table_name = 'safe_drops'
pdf_report_start_phrase = 'Safe Drop Report\nRegister'
pdf_report_end_phrase = None

columns = [
    {'name': 'pouch_envelope_number', 'dtype': 'string', 'index': 0},
    {'name': 'drop_amount', 'dtype': 'float', 'index': 1},
    {'name': 'tender_type', 'dtype': 'string', 'index': 2},
    {'name': 'total_drop', 'dtype': 'float', 'index': 3},
    {'name': 'drop_time', 'dtype': 'datetime', 'index': 4},
]

'''
    Safe drop reports are formated by register

    - each register must be parsed individually
    in order to pull the full report
    - custom logic required
'''

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
        output['has_report'] = False
        output['status'] = 'success'
        output['data'] = 'No Report: Required phrases not in report'

        return output

    # Pulls Report Text
    text = helpers.get_report_text(
        raw_text=parse_object.raw_text,
        report_start_phrase=pdf_report_start_phrase,
        report_end_phrase=pdf_report_end_phrase,
    )

    '''
        CUSTOM LOGIC

        - Multiple Registers
        - iterates through each register for data
        - requires iterative start/end keys
        - sets the end of each register as "end__{i}""
    '''

    iterative_key = 'Till Totals' # Identifies a register
    iterations = text.count(iterative_key) # Total registers

    iter_start_key = 'Register'
    iter_end_key = 'Till Totals'

    parsed_text_list = [] # Text from each iteration will be appended here
    register_details = [] # Data required for records of each register

    # Iterates through each register
    for i in range(iterations):

        text_iteration = helpers.get_report_text(
            raw_text=text,
            report_start_phrase=iter_start_key,
            report_end_phrase=iter_end_key,
        )

        first_line_word_list = text_iteration.split('\n')[0].split(' ')
        first_line_word_count = len(first_line_word_list)
        
        # Register Details
        register_number = first_line_word_list[1]
        cashier_name = []
        for j in range(4,first_line_word_count):
            cashier_name.append(first_line_word_list[j])

        register_detail = {
            'register_number': register_number,
            'cashier_name': ' '.join(cashier_name),
        }
        register_details.append(register_detail)

        # Cleaned Text for Report
        parsed_iteration = helpers.clean_text(
            text=text_iteration,
            skip_lead_rows=2,
            skip_trailing_rows=0,
        )
        parsed_text_list.append(parsed_iteration)
        parsed_text_list.append(f'end__{i}')

        # Removes iteration after processing
        # Allows next iteration to key off the same Start/End phrases
        text = text.replace(text_iteration, '')

    # Logic to identify columns in parsed words
    # --> identifies phrases that belong to 1 column
    # --> every report is different
    parsed_text = ' '.join(parsed_text_list)    # joins list of phrases
    parsed_words = parsed_text.split(' ')       # splits into list of words

    combine_one_word = []
    phrase_parts = []

    for i, word in enumerate(parsed_words):
        try:
            int(word)
        except:
            try:
                dateutil.parser.parse(word)
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
            combine_one_word.append(' '.join(phrase_parts))
            phrase_parts = []

    parsed_text = helpers.format_phrase_as_one_word(
        text=parsed_text,
        phrase_list=combine_one_word
    )

    parsed_words = parsed_text.split(' ')

    '''
        CUSTOM LOGIC

        - pouch/envelope is sometimes blank
        - fill in as 'blank'
    '''
    parsed_words = safe_drops.fill_in_missing_pouch_envelope(word_list=parsed_words)
    parsed_words = helpers.parse_datetime_parts(word_list=parsed_words)

    data = []

    '''
        CUSTOM LOGIC

        - needs to iterate through each register to parse data
        - the end of each register is marked by "end__{i}"
    '''
    for i, register_detail in zip(range(iterations), register_details):
        if i == 0:
            start = 0
        else:
            start = parsed_words.index(f'end__{i-1}') + 1

        end = parsed_words.index(f'end__{i}')

        parse_words_iteration = parsed_words[start:end]
        
        data_iteration = helpers.build_report_dictionary(
            word_list=parse_words_iteration,
            columns= columns,
            report_id=parse_object.report_id,
            store_id=parse_object.store_id,
            report_name=parse_object.table_name,
            processed_utc=parse_object.processed_utc,
            register_number=register_detail['register_number'],
            cashier_name=register_detail['cashier_name'],            
        )
        data.extend(data_iteration)

    helpers.clean_field_values(
        dictionary_list=data,
        columns=columns
    )

    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output
