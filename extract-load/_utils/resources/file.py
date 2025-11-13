import dlt
from dlt.sources import incremental


class FileResource:
    def __init__(
        self,
        call,
        object_name, # path
        incremental_attribute=None,
        write_disposition='append',
        **kwargs,
    ):

        self.call = call
        self.object_name = object_name

        self.incremental_attribute = incremental_attribute
        self.incremental_obj = incremental(incremental_attribute, initial_value=None) if incremental_attribute is not None else None
        self.write_disposition = write_disposition


        # Expected kwargs
        self.table_name_suffix = kwargs.get('table_name_suffix', '')
        self.s3_details = kwargs.get('s3_details', None)
        
    def yield_query_results(self, s3_details=None, incremental_obj=None):
        '''
        must align with: extract-load/utils/connections/s3.py
        
        s3_details = {
            'region': string,
            'endpoint': string,
            'access_key': string,
            'secret_key': string,
            'bucket_name':string,
        }

        '''

        # Incremental Filter -> requires incremental attribute
        if self.incremental_attribute:
            if incremental_obj:
                if incremental_obj.last_value:
                    incremental_value = incremental_obj.last_value
                else:
                    incremental_value = None
            else:
                incremental_value = None
        else:
            incremental_value = None

        args = {
            'object_name': self.object_name,
            'incremental': {
                'attribute': self.incremental_attribute,
                'value': incremental_value,
            }
        }

        if s3_details:
            args['s3_details'] = s3_details

        yield from self.call.yield_records(**args)

    def create_resource(self):
        table_name = self.object_name.split('/')[-2] + self.table_name_suffix

        @dlt.resource(
            name=table_name,
            table_name=table_name,
            write_disposition=self.write_disposition,
            primary_key=None,
        )
        def my_resource(incremental_obj=self.incremental_obj):
            s3_details = self.s3_details
            yield from self.yield_query_results(incremental_obj=incremental_obj, s3_details=s3_details)
        return my_resource()