import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _core.pipeline import LoadPipelineS3

from _utils.connections.s3 import S3Call
from _utils.helpers import pretty_all_jsons

from env import (
    POSTGRES_USER_PROD,
    POSTGRES_PASSWORD_PROD,
    POSTGRES_HOST_PROD,
    POSTGRES_PORT_PROD,
    POSTGRES_DB_PROD,
    POSTGRES_CERTIFICATION_PROD,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_REGION,
    S3_ENDPOINT,
    S3_BUCKET,
    S3_REPORT_FOLDERS_PATH_BASE,
)

S3_DETAILS = {
    'region': S3_REGION,
    'endpoint': S3_ENDPOINT,
    'access_key': S3_ACCESS_KEY,
    'secret_key': S3_SECRET_KEY,
    'bucket_name':S3_BUCKET,
}

sys.path.append(str(Path(__file__).resolve().parents[2]))

from _utils.s3 import get_files_s3_folder, connect_s3

s3_client = connect_s3(
    region=S3_REGION,
    endpoint=S3_ENDPOINT,
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
)

all_reports_and_folders = get_files_s3_folder(
    client=s3_client,
    bucket_name=S3_BUCKET,
    prefix=S3_REPORT_FOLDERS_PATH_BASE,
)

S3_REPORT_FOLDER_PATHS = [ obj for obj in all_reports_and_folders if obj.endswith('/') ]

class FullPipeline:
    def __init__(
        self,
        pipeline_name='kwik_pdf_s3_to_postgres',
        dataset='kwik_data_raw',
        call=S3Call,
        s3_details=S3_DETAILS,
        objects=S3_REPORT_FOLDER_PATHS,
        db_host=POSTGRES_HOST_PROD,
        db_port=POSTGRES_PORT_PROD,
        db_user=POSTGRES_USER_PROD,
        db_password=POSTGRES_PASSWORD_PROD,
        db_database=POSTGRES_DB_PROD,
        db_certification_path=POSTGRES_CERTIFICATION_PROD,
        test=False,
    ):

        self.pipeline_name = pipeline_name
        self.dataset = dataset
        
        self.call = call
        self.s3_details = s3_details

        self.objects = objects

        self.db_host = db_host
        self.db_port = db_port
        self.db_user = db_user
        self.db_password = db_password
        self.db_database = db_database
        self.db_certification_path = db_certification_path

        self.test = test

        self.pipeline = None
        self.resources = None

    def create_pipeline(self):
        self.pipeline = LoadPipelineS3(
            pipeline_name=self.pipeline_name,
            dataset=self.dataset,
            destination='postgres',
            test=self.test,
        )
        self.pipeline.build_destination_db_env(
            host=self.db_host,
            port=self.db_port,
            user=self.db_user,
            password=self.db_password,
            database=self.db_database,
            certification_path=self.db_certification_path,
        )
        return self.pipeline

    def build_resources(self):
        self.resources = self.pipeline.build_file_resources(
            call=self.call,
            sources=self.objects,
            incremental_attribute='processed_utc',
            write_disposition='append',
            s3_details=self.s3_details,
        )
        return self.resources

    def load_resources(self):
        pipeline_loaded = self.pipeline.build_pipeline(resources=self.resources)
        return pipeline_loaded

    def run_pipeline(self):
        self.pipeline.run_pipeline()
        return True
    
    def print_output(self):
        self.pipeline.print_output()
        pretty_all_jsons()

    def run_all(self):
        self.create_pipeline()
        self.build_resources()
        self.load_resources()
        self.run_pipeline()
        self.print_output()

if __name__ == '__main__':
    pipeline = FullPipeline()
    pipeline.run_all()
