import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport
from _utils import helpers, tender_summary

table_name = 'tender_summary'
pdf_report_start_phrase = 'PLU Sales Report'
pdf_report_end_phrase = 'Total PLU'

# Columns are out of order due to processing logic
columns = [
    {'name': 'plu_no', 'dtype': 'string', 'index': 0},
    {'name': 'pkg_qty', 'dtype': 'int', 'index': 1},
    {'name': 'tender_type', 'dtype': 'string', 'index': 8},
    {'name': 'department_name', 'dtype': 'string', 'index': 7},
    {'name': 'count', 'dtype': 'int', 'index': 6},
    {'name': 'price', 'dtype': 'float', 'index': 5},
    {'name': 'sales', 'dtype': 'float', 'index': 4},
    {'name': 'percent_dept', 'dtype': 'float', 'index': 3},
    {'name': 'percent_total', 'dtype': 'float', 'index': 2},
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
        output['has_report'] = False
        output['status'] = 'success'
        output['data'] = 'No Report: Required phrases not in report'

        return output

    # Pulls Report Text
    parse_object.get_report_text()

    '''
        CUSTOM LOGIC

        - report has 2 text fields next to each other
        - default parser is dependent on alternating text/float fields
        - Requires departments -> 4th column in tender_summary are departments
    '''

    # Departments

    dept_sales_report = 'Department Sales Report'
    dept_report_end_phrase = '100%'
    dept_text = helpers.get_report_text(
        raw_text=parse_object.raw_text,
        report_start_phrase=dept_sales_report,
        report_end_phrase=dept_report_end_phrase,
    )

    dept_parsed_text = helpers.clean_text(dept_text, skip_lead_rows=4)
    dept_parsed_words = dept_parsed_text.split(' ')
    
    departments = list(set(helpers.combine_column_phrases(dept_parsed_words)))
    # Sometimes 'Fees' is an unlisted department
    departments.append('Fees')
    # This parser's convention for combining phrases into 1 word is by replace spaces with '**'
    departments_starred = [ dep.replace(' ','**') for dep in departments ]


    # Back to PLU Transactions
    '''
        CUSTOM PARSING LOGIC
    '''
    parsed_text = parse_object.cleaned_text
    parsed_words_stage = [ word for word in parsed_text.split(' ') if word.strip() != '' ]

    parsed_words = tender_summary.parse_columns(
        word_list=parsed_words_stage,
        departments=departments,
    )

    # Processing Data
    data = parse_object.build_report_dictionary(
        word_list=parsed_words,
        columns=columns,
    )

    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output

