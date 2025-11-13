import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from _core.pipeline import LoadPipelineS3

from _utils.connections.s3 import S3Call
from _utils.helpers import pretty_all_jsons

from env import (
    POSTGRES_USER,
    POSTGRES_PASSWORD,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_CERTIFICATION,
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_REGION,
    S3_ENDPOINT,
    S3_BUCKET,
    S3_PATHS,
)

S3_DETAILS = {
    'region': S3_REGION,
    'endpoint': S3_ENDPOINT,
    'access_key': S3_ACCESS_KEY,
    'secret_key': S3_SECRET_KEY,
    'bucket_name':S3_BUCKET,
}

class FullPipeline:
    def __init__(
        self,
        call=S3Call,
        objects=S3_PATHS,
        test=False
    ):

        self.call = call

        self.objects = objects
        self.test = test

        self.pipeline = None
        self.resources = None

    def create_pipeline(self):
        self.pipeline = LoadPipelineS3(
            pipeline_name='kwik_pdf_s3_to_postgres',
            dataset='kwik_pdf_raw',
            destination='postgres',
            test=self.test
        )
        self.pipeline.build_destination_db_env(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            database=POSTGRES_DB,
            certification_path=POSTGRES_CERTIFICATION,
        )
        return self.pipeline

    def build_resources(self):
        self.resources = self.pipeline.build_file_resources(
            call=self.call,
            sources=self.objects,
            incremental_attribute='processed_utc',
            write_disposition='append',
            s3_details=S3_DETAILS,
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
