import os
import json
import requests

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent))

from get_files_s3 import get_all_pdf_files
from env import (
    DIGITAL_OCEAN_TOKEN,
    DIGITAL_OCEAN_FUNCTION_NAME_SPACE,
)

API_BASE = 'https://faas-sfo3-7872a1dd.doserverless.co/api/v1/web/fn-748daf29-b84b-4763-8dd6-15b55d63f6e8/default/parse_reports_s3'
NAMESPACE = DIGITAL_OCEAN_FUNCTION_NAME_SPACE
TOKEN = DIGITAL_OCEAN_TOKEN

def handler(event, context):
    'Dispatcher function — triggers worker for each file in S3.'
    try:
        files = get_all_pdf_files()
    except Exception as e:
        return {'statusCode': 500, 'body': f'Failed to get files: {e}'}

    if not files:
        return {'statusCode': 200, 'body': 'No files to process.'}

    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'
    }

    for f in files:
        payload = {'filename': f}
        requests.post(
            f'{API_BASE}/{NAMESPACE}/actions/kwik_pdf/worker?blocking=false',
            headers=headers,
            data=json.dumps(payload),
        )

    return {'statusCode': 200, 'body': f'Triggered {len(files)} worker jobs.'}