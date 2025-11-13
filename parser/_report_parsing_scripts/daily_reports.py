import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from _core.pdf_parser import ExtractReport

'''
    daily_reports has a diffferent structure that all reports
    -> 1 record per report
    -> only pulls in data from PDF header
'''

table_name = 'daily_reports'
pdf_report_start_phrase = 'Store Close'
pdf_report_end_phrase = None

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

    data = parse_object.build_report_dictionary(
        word_list=None,
        columns=None,
        store_id=parse_object.store_id,
        report_date=parse_object.report_date,
        report_id=parse_object.report_id,
        operator_name=parse_object.operator_name,
        operator_id=parse_object.operator_id,
        software_version=parse_object.software_version,
        report_printed_at=parse_object.report_printed_at,
        report_period_start=parse_object.report_period_start,
        report_period_end=parse_object.report_period_end,
        processed_utc=parse_object.processed_utc,
    )

    output['has_report'] = True
    output['status'] = 'success'
    output['data'] = data

    return output

