from parse_pdfs_s3 import (
    parse_report,
    get_all_pdf_files,
)

if __name__ == '__main__':
    print('\n')
    files = get_all_pdf_files()

    for file in files:
        parse_report(
            filepath=file['filepath'],
            test=True,
        )