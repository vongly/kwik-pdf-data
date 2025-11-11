import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from parse_report_s3 import parse_report

def handler(event, context):
    '''Worker — parses a single file from S3.'''
    filename = event.get('filename')
    if not filename:
        return {'statusCode': 400, 'body': 'Missing filename'}

    try:
        parse_report(filename)
        return {'statusCode': 200, 'body': f'Parsed {filename}'}
    except Exception as e:
        return {'statusCode': 500, 'body': f'Error parsing {filename}: {e}'}