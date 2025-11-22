from s3_to_postgres import FullPipeline

from env import (
    POSTGRES_USER_DEV,
    POSTGRES_PASSWORD_DEV,
    POSTGRES_HOST_DEV,
    POSTGRES_PORT_DEV,
    POSTGRES_DB_DEV,
    POSTGRES_CERTIFICATION_DEV,
)

if __name__ == '__main__':
    pipeline = FullPipeline(
        pipeline_name='kwik_pdf_s3_to_postgres_dev',
        db_host=POSTGRES_HOST_DEV,
        db_port=POSTGRES_PORT_DEV,
        db_user=POSTGRES_USER_DEV,
        db_password=POSTGRES_PASSWORD_DEV,
        db_database=POSTGRES_DB_DEV,
        db_certification_path=POSTGRES_CERTIFICATION_DEV,
        test=True,
    )
    pipeline.run_all()
