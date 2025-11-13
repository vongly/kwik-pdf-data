from datetime import datetime, timezone
import duckdb


class S3Call():
    def __init__(
        self,
        test=False,
        test_limit=1,
    ):

        self.test = test
        self.test_limit = test_limit

    def yield_records(
        self,
        object_name, # filepath
        filetype='csv',
        where_clause= None,
        incremental:dict=None,
        s3_details=None,
        processed_timestamp=datetime.now(timezone.utc),
    ):

        con = duckdb.connect()

        if where_clause and not incremental['value']:
            pass # -> where_clause = where_clause
        elif not where_clause and incremental['value']:
            where_clause = f''' WHERE {incremental['attribute']} > '{incremental['value']}' '''
        elif where_clause and incremental['value']:
            where_clause = f''' {where_clause} AND {incremental['attribute']} > '{incremental['value']}' '''
        else:
            where_clause = ''

        if self.test:
            if not self.test_limit or self.test_limit < 1:
                self.test_limit = 1
        
        limit_clause = f' LIMIT {self.test_limit}' if self.test else ''

        if filetype == 'csv':
            duck_db_function = 'read_csv_auto'
        else:
            duck_db_function = f'read_{filetype}'

        '''
        must align with: extract-load/utils/resources/file.py

        s3_details = {
            'region': string,
            'endpoint': string,
            'access_key': string,
            'secret_key': string,
            'bucket_name':string,
        }

        '''

        region = s3_details.get('region', None)
        endpoint = s3_details.get('endpoint', None)
        access_key = s3_details.get('access_key', None)
        secret_key = s3_details.get('secret_key', None)
        bucket_name = s3_details.get('bucket_name', None)

        if s3_details:
            target_folder = f'''s3://{bucket_name}/{object_name}'''

            s3_credentials = f'''
            set s3_region='{region}';
            set s3_endpoint='{endpoint.replace('https://', '')}';
            set s3_access_key_id='{access_key}';
            set s3_secret_access_key='{secret_key}';
            set s3_use_ssl=true;
            set s3_url_style='path';'''
        else:
            target_folder = object_name
            s3_credentials = ''

        query = f'''
            {s3_credentials}

            select
                *,
                cast('{processed_timestamp}' as timestamp) as _dlt_processed_utc
            from
                {duck_db_function}('{target_folder}*.csv')
        ''' + where_clause + limit_clause + ';'

        con.execute(query)

        result = con.execute(query)
        records = result.fetchall()

        cols = [col[0] for col in result.description]
        rows = [dict(zip(cols, row)) for row in records]

        for row in rows:
            yield row

if __name__ == '__main__':
    import sys
    from pathlib import Path

    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

    from env import (
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
    call = S3Call()

    call.yield_records(
        object_name='parsed_reports/to_be_uploaded/fuel_sales_by_grade/',
        filetype='csv',
        s3_details=S3_DETAILS,
    )