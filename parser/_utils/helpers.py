from pypdf import PdfReader
import csv
from itertools import chain
from dateutil import parser
import boto3
from io import BytesIO, StringIO

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from env import (
    S3_ACCESS_KEY,
    S3_SECRET_KEY,
    S3_REGION,
    S3_ENDPOINT,
    S3_BUCKET,
)

def connect_s3(
    region=S3_REGION,
    endpoint=S3_ENDPOINT,
    access_key=S3_ACCESS_KEY,
    secret_key=S3_SECRET_KEY,
):
    s3 = boto3.client(
        's3',
        region_name=region,
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    return s3

# Write csv file

def write_csv_local(dictionary, filename, destination='./'):
    with open(destination + filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=dictionary[0].keys())
        writer.writeheader()
        writer.writerows(dictionary)

def write_csv_s3(dictionary, filename, destination='./', bucket_name=S3_BUCKET):
    s3 = connect_s3()

    csv_buffer = StringIO()
    fieldnames = dictionary[0].keys()          # auto-detect columns from first dict
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(dictionary)

    full_filepath = f'{destination}{filename}'

    s3.put_object(Bucket=bucket_name, Key=full_filepath, Body=csv_buffer.getvalue())
 

# Read PDF Functions    

def read_from_file(filepath, filename):
    reader = PdfReader(filepath + filename)
    pages = reader.pages

    text_list = []

    for page in pages:
        page_text = page.extract_text()
        text_list.append(page_text)
        
    text = '\n'.join(text_list)

    #removes Page markers
    text_lines = [ line for line in text.split('\n') if not line.startswith('Page') ]
    text = '\n'.join(text_lines)

    return text

def read_from_s3(bucket_name, filename):
    s3 = connect_s3()

    response = s3.get_object(Bucket=bucket_name, Key=filename)
    pdf_bytes = response['Body'].read()
    pdf_file = BytesIO(pdf_bytes)

    reader = PdfReader(pdf_file)
    pages = reader.pages

    text_list = []

    for page in pages:
        page_text = page.extract_text()
        text_list.append(page_text)
        
    text = '\n'.join(text_list)

    #removes Page markers
    text_lines = [ line for line in text.split('\n') if not line.startswith('Page') ]
    text = '\n'.join(text_lines)

    return text

# Parse Functions

def get_report_text(
    raw_text,
    report_start_phrase,
    report_end_phrase,
    report_end_phrase_occurance=1,
    add_more_characters=0,
):
    report_start_phrase
    key_start_pos = raw_text.find(report_start_phrase)
    report_end_phrase

    if key_start_pos != -1:
        if report_end_phrase:
            temp_start_pos = key_start_pos
            for n in range(report_end_phrase_occurance):
                key_end_pos = raw_text.find(report_end_phrase, temp_start_pos + 1)
                temp_start_pos = key_end_pos

            key_end_pos = key_end_pos + add_more_characters

        else:
            key_end_pos = len(raw_text)


    text = raw_text[key_start_pos:key_end_pos]

    return text

def clean_text(text, first_word_of_line_to_remove=[], skip_lead_rows=2, skip_trailing_rows=0, skip_mid_rows=[]):

    parsed_text = text.split('\n') # splits by line break

    # auto removes lines that start with Page
    first_word_of_line_to_remove.append('Page')

    parsed_text = [ line for line in parsed_text if line.split(' ')[0] not in first_word_of_line_to_remove ]

    parsed_text = [ line for i, line in enumerate(parsed_text) if i not in range(skip_lead_rows) ] # removes leading rows -> title, headers...
    if skip_trailing_rows > 0:
        parsed_text = parsed_text[:skip_trailing_rows * -1]

    parsed_text = [ line for line in parsed_text if line != [''] ] # removes empty lines
    if skip_mid_rows:
        parsed_text = [ line for line in parsed_text if line.split(' ')[0] not in skip_mid_rows ] # removes rows with matching first word


    parsed_text = [ line.split(' ') for line in parsed_text ] # splits each line into lists of words
    parsed_words = list(chain.from_iterable(parsed_text)) # splits lists of lists into 1 list of all words
    parsed_words = [ word for word in parsed_words if word != '' ]

    parsed_text = ' '.join(parsed_words) # rejoins words back into 1 text string 

    return parsed_text

def combine_column_phrases(word_list):
    # Requires report columns w/ text phrases to be separated by columns with floats
    column_phrases = []
    phrase_parts = []

    for i, word in enumerate(word_list):
        word = word.replace('$','').replace('%','').replace(',','')
        if i == 0:
            previous_is_num = True

        if word.startswith('end__'):
            continue

        try:
            float(word)
            current_is_num = True
        except:
            current_is_num = False

        if not current_is_num:
            phrase_parts.append(word)
        elif current_is_num and not previous_is_num:
            column_phrases.append(' '.join(phrase_parts))
            phrase_parts = []

        previous_is_num = current_is_num

    return column_phrases


def build_report_dictionary(word_list, columns=None, **kwargs):
    # Requires word_list to be organized into a sequential list
    # Requires columns to be processed in sequential order
    data = []

    if not columns:
        data = [kwargs]

        return data

    if not all('index' in c for c in columns):
        for col_i, col in enumerate(columns):
            col['index'] = col_i

    # index refers to the order the column is processed -> not the order on report
    columns = sorted(columns, key=lambda x: x['index'])

    # iterates through all words and assignes that to a key/value pair
    for i, word in enumerate(word_list):
        # i identifies the index value of each word in word_list
        # j identifies the index order of column to be prcessed

        # sets intitial value of j
        if i == 0:
            j = i

        col = columns[j]

        # if first column -> creates/leans line dictionary and sets report info
        if j == 0:
            row = {}

            # Add all kwargs as fields
            for key, value in kwargs.items():
                row[key] = value

            row[col['name']] = word
            j = j + 1
            continue

        # if last column -> appends line and resets j index
        elif j == len(columns) - 1:
            row[col['name']] = word
            data.append(row)
            j = 0
            continue

        else:
            row[col['name']] = word
            j = j + 1
            continue
            
    return data

def clean_field_values(dictionary_list, columns):
    for row in dictionary_list:
        if columns:
            for col in columns:
                name = col['name']
                dtype = col['dtype']

                if dtype in ['string']:
                    row[name] = row[name].replace('**',' ')
                if dtype in ['float']:
                    row[name] = float(row[name].replace('$','').replace('%','').replace(',',''))
                if dtype in ['int']:
                    row[name] = int(row[name].replace('$','').replace('%','').replace(',',''))


def parse_datetime_parts(word_list):
    parsed_words = []
    datetime_phrase = []
    for word in word_list:
        try:
            # ignore if word can be an int
            int(word)
            parsed_words.append(word)
            continue
        except:
            pass

        try:
            # ignore if word can be a float
            float(word)
            parsed_words.append(word)
            continue
        except:
            pass

        try:
            # If word is in a date format
            parser.parse(word)
            datetime_phrase.append(word)
        except:
            try:
                # if word is in a time format
                parser.parse(word).time()
                datetime_phrase.append(word)
            except:
                # if word is AM or PM
                # --> build a timestamp object w/datetime_phrase
                # --> clear datetime_phrase
                if word in ['AM', 'PM']:
                    datetime_phrase.append(word)
                    my_datetime = parser.parse(' '.join(datetime_phrase))
                    datetime_phrase = []
                    parsed_words.append(my_datetime)
                else:
                    # ignores word
                    parsed_words.append(word)

    return parsed_words

def format_phrase_as_one_word(text, phrase_list):

    # Sorts from longest to shortest in order to not partially convert matching substrings
    # i.e. '24/7 RED BOX 100' and '24/7 RED BOX'
    phrase_list = sorted(phrase_list, key=len, reverse=True)
    phrase_list_starred = [ word.replace(' ','**') for word in phrase_list ]

    for word, starred_word in zip(phrase_list, phrase_list_starred):
        text = text.replace(word, starred_word)

    return text

# S3 Orchestration

def get_files_s3_folder(prefix, bucket_name=S3_BUCKET):
    s3 = connect_s3()

    prefix = 'pdf_files/to_be_processed/'

    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    if 'Contents' in objects:
        filepaths = [
            obj['Key'] for obj in objects['Contents']
                if obj['Key'] != prefix and not obj['Key'].endswith('/processed/')
        ]
    else:
        filepaths = []

    return filepaths

def create_s3_folder(folder_path, bucket_name=S3_BUCKET):
    s3 = connect_s3()

    s3.put_object(Bucket=bucket_name, Key=folder_path)

    return {
        'folder': folder_path
    }

def move_s3_file(source, destination, bucket_name=S3_BUCKET, test=False):
    s3 = connect_s3()

    if test is False:
        copy_source = {'Bucket': bucket_name, 'Key': source}
        s3.copy_object(Bucket=bucket_name, CopySource=copy_source, Key=destination)
        s3.delete_object(Bucket=bucket_name, Key=source)
    elif test is True:
        s3.head_object(Bucket=bucket_name, Key=source)
        s3.head_object(Bucket=bucket_name, Key=destination)

    return {
        'source': source,
        'destination': destination,
    }