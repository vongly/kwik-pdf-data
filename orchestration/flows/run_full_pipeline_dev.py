import subprocess
from prefect import flow

from run_parser import run_parser
from run_extract_load_dev import run_extract_load_dev
from run_transformations_dev import run_transformations_dev

@flow(name='Full Data Pipeline - Dev')
def run_full_pipeline_dev():
    
    run_parser()
    run_extract_load_dev()
    run_transformations_dev()

if __name__ == '__main__':
    run_full_pipeline_dev()