import subprocess
from prefect import flow

from run_parser import run_parser
from run_extract_load import run_extract_load
from run_transformations import run_transformations

@flow(name='Full Data Pipeline')
def run_full_pipeline():
    
    run_parser()
    run_extract_load()
    run_transformations()

if __name__ == '__main__':
    run_full_pipeline()