from datetime import datetime, timezone

from _util.helpers import (
    get_files_s3_folder,
    write_csv_s3,
    create_s3_folder,
    move_s3_file,
)

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
    S3_BUCKET,
    PDF_TO_BE_PROCESSED_FOLDER_S3,
    PDF_PROCESSED_FOLDER_S3,
    CSV_BASE_FOLDER_S3,
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

PARSE_FUNCTIONS = [{'name': pf.__name__.split('.')[-1], 'function': pf.parse_pdf} for pf in parse_functions]

def get_all_pdf_files(pdf_to_be_process_folder=PDF_TO_BE_PROCESSED_FOLDER_S3):
    pdf_filepaths = get_files_s3_folder(prefix=pdf_to_be_process_folder)

    output = [ {'filepath': f} for f in pdf_filepaths ]

    return output

def parse_report(
        filepath,
        bucket_name=S3_BUCKET,
        pdf_process_folder=PDF_PROCESSED_FOLDER_S3,
        csv_base_folder=CSV_BASE_FOLDER_S3,
        jobs=PARSE_FUNCTIONS,
        test=False,
    ):

        processed_timestamp = datetime.now(timezone.utc)

        create_s3_folder(folder_path=pdf_process_folder)
        create_s3_folder(folder_path=csv_base_folder)

        print('PROCESSING PDF:', filepath.split('/')[-1], '\n')

        results = []
        result = {}

        for job in jobs:
            job_function = job['function']
            result = job_function(
                filename=filepath,
                filepath=bucket_name,
                processed_utc=processed_timestamp,
            )

            result['original_filepath'] = filepath
            results.append(result)
            
            my_response = result.copy()
            if isinstance(my_response['data'], list):
                my_response['records_processed'] = len(my_response['data'])

        for r in results:
            report_id = r['report_id']
            report_name = r['table_name']

            if r['has_report']:
                processed_timestamp_string = processed_timestamp.strftime('%Y.%m.%d.%H.%M.%S')
                try:
                    # Report Written
                    report_folder_path = f'{csv_base_folder}{report_name}/'
                    create_s3_folder(folder_path=report_folder_path)

                    if test is False:
                        write_csv_s3(
                            dictionary=r['data'],
                            filename=f'{report_id}-{report_name}-{processed_timestamp_string}.csv',
                            destination=report_folder_path,
                        )
                        print(f'CSV WRITTEN - {report_name}')
                    elif test is True:
                        print(f'REPORT FOUND - {report_name}')

                except Exception as e:
                    # Error
                    print(f'ERROR - CSV NOT WRITTIEN - {report_name}: {e}')
                    exit()
            else:
                # No Report
                print(f'NO REPORT - CSV NOT WRITTIEN - {report_name}')

        source = filepath
        destination = f'{pdf_process_folder}{report_id}-{processed_timestamp_string}.pdf'


        move_details = move_s3_file(
            source=source,
            destination=destination,
            test=test,
        )

        for r in results:
            r['move_details'] = move_details

        print('\nPDF Completed\n')

        return results

if __name__ == '__main__':
    print('\n')
    files = get_all_pdf_files()

    for file in files:
        parse_report(filepath=file['filepath'])