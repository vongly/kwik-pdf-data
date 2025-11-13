import sys
from pathlib import Path
import dateutil

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers

table_name = 'fuel_dispenser_sales'
pdf_report_start_phrase = 'Fuel Sales/Pump Totals Reconciliation Report'
pdf_report_end_phrase = None

columns = [
    {'name': 'grade_name', 'dtype': 'string', 'index': 0},
    {'name': 'grade_sales', 'dtype': 'float', 'index': 1},
    {'name': 'sales_in_progress', 'dtype': 'float', 'index': 2},
    {'name': 'ppu_discount', 'dtype': 'float', 'index': 3},
    {'name': 'post_pay_discount', 'dtype': 'float', 'index': 4},
    {'name': 'cash_credit_conversion', 'dtype': 'float', 'index': 5},
    {'name': 'book_sales', 'dtype': 'float', 'index': 6},
    {'name': 'metered_money', 'dtype': 'float', 'index': 7},
    {'name': 'volume_sold', 'dtype': 'float', 'index': 8},
    {'name': 'volume_in_progress', 'dtype': 'float', 'index': 9},
    {'name': 'book_volume', 'dtype': 'float', 'index': 10},
    {'name': 'metered_volume', 'dtype': 'float', 'index': 11},
]

'''
    Fuel Sales/Pump Totals reports are formated by pump dispenser

    - each dispenser must be parsed individually in order to pull the full report
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

    iterative_key = 'Dispenser Id' # Identifies a dispenser
    iterations = text.count(iterative_key) # Total dispensers

    iter_start_key = 'Dispenser Id'
    iter_end_key = 'Totals'

    parsed_text_list = [] # Text from each iteration will be appended here
    iteration_details = [] # Data required for records of each register

    # Iterates through each register
    for i in range(iterations):

        text_iteration = helpers.get_report_text(
            raw_text=text,
            report_start_phrase=iter_start_key,
            report_end_phrase=iter_end_key,
            report_end_phrase_occurance=2,
        )

        first_line_word_list = text_iteration.split('\n')[0].split(' ')
        first_line_word_count = len(first_line_word_list)
        
        # Dispenser Details
        dispenser_id = first_line_word_list[2]
        dispenser_detail = {'dispenser_id': dispenser_id}
        iteration_details.append(dispenser_detail)

        # Cleaned Text for Report
        parsed_iteration = helpers.clean_text(
            text=text_iteration,
            first_word_of_line_to_remove=['Totals', 'Grade', 'Progress', 'Volume'],
            skip_lead_rows=4,
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

    column_phrases = helpers.combine_column_phrases(parsed_words)

    parsed_text = helpers.format_phrase_as_one_word(
        text=parsed_text,
        phrase_list=column_phrases
    )

    parsed_words = parsed_text.split(' ')

    '''
        CUSTOM LOGIC

        - fuel sales and volumes are formatted on different lines
        - PDF displays all fuel grade name for sales
        before displaying volumes for the same descriptions
        - custom logic in place to search for correlating volumes
        - lineates bothe sales + volumes for one fuel grade name
    '''

    parsed_words_stage = parsed_words

    #  Compiles all Grade Names
    all_grade_names = []

    # Identified by non float values
    for word in parsed_words_stage:
        try:
            float(word.replace('$','').replace('%','').replace(',',''))
        except:
            if not word.startswith('end__') and word not in all_grade_names:
                all_grade_names.append(word)

    # Custom Parsing Logic
    parsed_words = []

    seen_grades = []
    current_grade = None
    j = 0
    for i, word in enumerate(parsed_words_stage):

        # When iteration reaches end marker -> new dispenser
        # --> appends end marker
        # --> resets seen_grades
        # --> resets current_grade
        # --> skips to next iteration
        if word.startswith('end__'):
            parsed_words.append(word)
            seen_grades = []
            current_grade = None
            continue

        # New grade description -> sales
        # --> assigns current_grade
        # --> adds current_grade to seen_grades
        # --> collects grade description
        if j == 0:
            if word in all_grade_names and word not in seen_grades:
                current_grade = word
                seen_grades.append(current_grade)
                parsed_words.append(word)
                j = 1
            else:
                continue

        # Initial 6 sales values
        # --> collects value and moves forward in loop
        elif 1 <= j <= 6:
            # Checks current word
            # --> not a grade
            # --> not an end marker
            # --> appends sale
            if word not in all_grade_names and not word.startswith('end__'):
                parsed_words.append(word)
                j = j + 1
                continue

        # Last sales values
        # --> collects last sales value
        # --> cycles through logic to retrieve respective volumes
        if j == 7:
            # same as other sales
            if word not in all_grade_names and not word.startswith('end__'):
                parsed_words.append(word)

            # Scanning for respective volumes
            for k in range(i+1, len(parsed_words_stage)):
                next_word = parsed_words_stage[k]
                if next_word == current_grade:
                    volumes = parsed_words_stage[k+1:k+5]
                    parsed_words.extend(volumes)
                    j = 0
                    current_grade = None
                    break

                elif next_word.startswith('end__'):
                    current_grade = None
                    break

    data = []

    '''
        CUSTOM LOGIC

        - needs to iterate through each dispenser to parse data
        - the end of each register is marked by "end__{i}"
    '''
    for i, iteration_detail in zip(range(iterations), iteration_details):
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
            dispenser_id=iteration_detail['dispenser_id'],
        )
        data.extend(data_iteration)

    helpers.clean_field_values(
        dictionary_list=data,
        columns=columns,
    )

    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output
