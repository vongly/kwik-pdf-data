from datetime import datetime, timezone

from _utils.helpers import write_csv_s3

from _report_parsing_scripts import (
    card_transaction_counts,
    daily_cashier_stats,
    daily_reports,
    daily_sales_summary,
    department_sales,
    fuel_dispensers_sales,
    fuel_sales_by_grade,
    paid_in_out,
    safe_drops,
    tax_summary,
    tender_summary,
)

from env import (
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_REGION,
    S3_ENDPOINT,
    S3_BUCKET,
    PDF_TO_BE_PROCESSED_FOLDER_S3,
    PDF_PROCESSED_FOLDER_S3,
    CSV_BASE_FOLDER_S3,
)
    
import sys
from pathlib import Path
import traceback

sys.path.append(str(Path(__file__).resolve().parents[2]))

from _utils.s3 import (
    connect_s3,
    get_files_s3_folder,
    create_s3_folder,
    move_s3_file,
)

parse_functions = [
    card_transaction_counts,
    daily_cashier_stats,
    daily_reports,
    daily_sales_summary,
    department_sales,
    fuel_dispensers_sales,
    fuel_sales_by_grade,
    paid_in_out,
    safe_drops,
    tax_summary,
    tender_summary,
]


s3_client = connect_s3(
    region=S3_REGION,
    endpoint=S3_ENDPOINT,
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
)


PARSE_FUNCTIONS = [{'name': pf.__name__.split('.')[-1], 'function': pf.parse_pdf} for pf in parse_functions]

def get_all_pdf_files(pdf_to_be_process_folder=PDF_TO_BE_PROCESSED_FOLDER_S3):
    pdf_filepaths = get_files_s3_folder(
        client=s3_client,
        bucket_name=S3_BUCKET,
        prefix=pdf_to_be_process_folder,
    )
    
    output = [ {'filepath': f} for f in pdf_filepaths ]

    return output

def parse_report(
        filepath,
        bucket_name=S3_BUCKET,
        pdf_processed_folder=PDF_PROCESSED_FOLDER_S3,
        csv_base_folder=CSV_BASE_FOLDER_S3,
        jobs=PARSE_FUNCTIONS,
        test=False,
    ):
        
        processed_timestamp = datetime.now(timezone.utc)

        create_s3_folder(
            client=s3_client,
            folder_path=pdf_processed_folder,
            bucket_name=S3_BUCKET,
        )
        create_s3_folder(
            client=s3_client,
            folder_path=csv_base_folder,
            bucket_name=S3_BUCKET,
        )

        results = []
        result = {}

        for job in jobs:
            job_function = job['function']
            result = job_function(
                filename=filepath,
                filepath=bucket_name,
                processed_utc=processed_timestamp,
                s3_client=s3_client,
            )
            my_result = result.copy()

            my_result['original_filepath'] = filepath

            if isinstance(my_result['data'], list):
                my_result['records_processed'] = len(my_result['data'])

            results.append(my_result)

        for r in results:
            report_id = r['report_id']
            report_name = r['table_name']
            has_report = r['has_report']
            data = r['data']
            report_folder_path = f'{csv_base_folder}{report_name}/'
            processed_timestamp_string = processed_timestamp.strftime('%Y.%m.%d.%H.%M.%S')

#            if report_name == 'paid_in_out':
#                print(data)
#                exit()

            for row in data:
                row['has_report'] = has_report

            try:
                if test is False:
                    create_s3_folder(
                        client=s3_client,
                        folder_path=report_folder_path,
                        bucket_name=S3_BUCKET,
                    )
                    write_csv_s3(
                        client=s3_client,
                        dictionary=data,
                        filename=f'{report_id}-{report_name}-{processed_timestamp_string}.csv',
                        destination=report_folder_path,
                    )
                if has_report:
                    if not test:
                        print(f'CSV WRITTEN - {report_name}')
                    else:
                        print(f'REPORT FOUND - {report_name}')

                elif not r['has_report']:
                    if not test:
                        print(f'REPORT NOT FOUND - EMPTY CSV WRITTEN - {report_name}')
                    else:
                        print(f'REPORT NOT FOUND - {report_name}')

            except Exception as e:
                print(f'ERROR - CSV NOT WRITTEN - {report_name}: {e}')
                traceback.print_exc()
                exit()

        source = filepath
        destination = f'{pdf_processed_folder}{report_id}-{processed_timestamp_string}.pdf' if test is False else pdf_processed_folder

        move_details = move_s3_file(
            client=s3_client,
            source=source,
            destination=destination,
            bucket_name=bucket_name,
            test=test,
        )

        for r in results:
            r['move_details'] = move_details

        return results

if __name__ == '__main__':
    print('\n')
    files = get_all_pdf_files()
    if not files:
        print('NO FILES TO PROCESS')
        exit()
    for file in files:
        print('PROCESSING PDF:', file['filepath'].split('/')[-1], '\n')
        parse_report(filepath=file['filepath'])
        print('\nPDF Completed\n')