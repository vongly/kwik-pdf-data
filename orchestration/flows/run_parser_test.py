from prefect import flow

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from tasks.parser import parse_pdfs_s3_test


@flow(name='Test Parse PDF Files from S3 Bucket')
def run_parser_test():
    parse_pdfs_s3_test()

if __name__ == '__main__':
    run_parser_test()