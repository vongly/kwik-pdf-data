from parse_pdfs_s3 import (
    parse_report,
    get_all_pdf_files,
)

if __name__ == '__main__':
    print('\n')
    files = get_all_pdf_files()

    for file in files:
        print('PROCESSING PDF (TEST):', file['filepath'].split('/')[-1], '\n')
        parse_report(
            filepath=file['filepath'],
            test=True,
        )
        print('\nTEST Completed\n')