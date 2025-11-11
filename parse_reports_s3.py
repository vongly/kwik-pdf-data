from datetime import datetime, timezone

from _util.helpers import (
    get_files_s3_folder,
    write_csv_s3,
    create_s3_folder,
    move_s3_file,
)

from env import (
    S3_BUCKET,
    PDF_TO_BE_PROCESSED_FOLDER_S3,
    PDF_PROCESSED_FOLDER_S3,
    CSV_BASE_FOLDER_S3,
)

from config import PARSE_FUNCTIONS


def process_pdf_s3(
        self,
        filepath,
        bucket_name=S3_BUCKET,
        pdf_process_folder=PDF_PROCESSED_FOLDER_S3,
        csv_base_folder=CSV_BASE_FOLDER_S3,
        jobs=PARSE_FUNCTIONS,
    ):

        processed_timestamp = datetime.now(timezone.utc)

        create_s3_folder(folder_path=pdf_process_folder)
        create_s3_folder(folder_path=csv_base_folder)

        print(filepath, '\n')

        results = []
        result = {}

        for job in jobs:
            job_function = job['function']
            result = job_function(
                filename=filepath,
                filepath=S3_BUCKET,
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

                    write_csv_s3(
                        dictionary=r['data'],
                        filename=f'{report_id}-{report_name}-{processed_timestamp_string}.csv',
                        destination=report_folder_path,
                    )
                except Exception as e:
                    # Error
                    exit()
            else:
                # No Report
                print(f'{report_name} - not written: No report')

        for r in results:
            destination = f'{pdf_process_folder}{report_id}-{processed_timestamp_string}.pdf'

            move_details = move_s3_file(
                source = filepath,
                destination=destination,
            )

            r['move_details'] = move_details
        
        return results

if __name__ == '__main__':
    pass