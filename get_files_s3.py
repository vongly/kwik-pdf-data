from _util.helpers import get_files_s3_folder
from env import PDF_TO_BE_PROCESSED_FOLDER_S3

def get_all_pdf_files(pdf_to_be_process_folder=PDF_TO_BE_PROCESSED_FOLDER_S3):
    pdf_filepaths = get_files_s3_folder(prefix=pdf_to_be_process_folder)

    return pdf_filepaths

if __name__ == '__main__':
    get_all_pdf_files()