import boto3

def connect_s3(
    region,
    endpoint,
    access_key,
    secret_key,
):
    client = boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    return client

# S3 Orchestration

def get_files_s3_folder(client, prefix, bucket_name):

    objects = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in objects:
        filepaths = [
            obj['Key'] for obj in objects['Contents']
                if obj['Key'] != prefix
        ]
    else:
        filepaths = []

    return filepaths

def create_s3_folder(client, folder_path, bucket_name):

    client.put_object(Bucket=bucket_name, Key=folder_path)

    return {
        'folder': folder_path
    }

def move_s3_file(client, source, destination, bucket_name, test=False):

    if test is False:
        copy_source = {'Bucket': bucket_name, 'Key': source}
        client.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=destination)
        client.delete_object(Bucket=bucket_name, Key=source)
    elif test is True:
        client.head_object(Bucket=bucket_name, Key=source)
        client.head_object(Bucket=bucket_name, Key=destination)

    return {
        'source': source,
        'destination': destination,
    }