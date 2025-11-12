import dateutil

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

import _util.helpers as helpers

class ExtractReport:
    def __init__(
        self,
        filename, # For S3 -> full file path
        filepath, # For S3 -> bucket name
        pdf_report_start_phrase,
        pdf_report_end_phrase,
        table_name,
        processed_utc,
        location='s3',
    ):
        
        self.filename = filename

        if location == 's3':
            self.raw_text = helpers.read_from_s3(
                bucket_name=filepath,
                filename=filename
            )
        elif location == 'local':
            self.raw_text = helpers.read_from_file(
                filepath=filepath,
                filename=filename
            )

        self.pdf_report_start_phrase = pdf_report_start_phrase
        self.pdf_report_end_phrase = pdf_report_end_phrase
        self.table_name = table_name
        self.processed_utc = processed_utc

        # Report details
        raw_text_lines = self.raw_text.split('\n')

        self.store_id = raw_text_lines[2].split(' ')[0]

        operator_name_line_word_len = len(raw_text_lines[5].split(' '))
        operator_name_list = []
        
        for i in range(2, operator_name_line_word_len):
            operator_name_list.append(raw_text_lines[5].split(' ')[i])

        self.operator_name = ' '.join(operator_name_list)
        self.operator_id = raw_text_lines[6].split(' ')[2]
        self.software_version = raw_text_lines[7].split(' ')[2]

        report_printed_at_string = raw_text_lines[8].split(' ')[2]+ ' ' + raw_text_lines[8].split(' ')[3] + ' ' + raw_text_lines[8].split(' ')[4]
        self.report_printed_at = dateutil.parser.parse(report_printed_at_string)

        report_period_start_string = raw_text_lines[9].split(' ')[2][:3] + ' ' + raw_text_lines[9].split(' ')[3] + ' ' + raw_text_lines[9].split(' ')[4] + ' ' + raw_text_lines[9].split(' ')[5] + ' ' + raw_text_lines[9].split(' ')[6]
        self.report_period_start = dateutil.parser.parse(report_period_start_string)
        self.report_date = self.report_period_start.date()

        report_period_end_string = raw_text_lines[9].split(' ')[8][:3] + ' ' + raw_text_lines[9].split(' ')[9] + ' ' + raw_text_lines[9].split(' ')[10] + ' ' + raw_text_lines[9].split(' ')[11] + ' ' + raw_text_lines[9].split(' ')[12]
        self.report_period_end = dateutil.parser.parse(report_period_end_string)
        self.report_date_end = self.report_period_end.date()

        report_start_string = self.report_date.strftime('%Y.%m.%d')
        report_end_string = self.report_date_end.strftime('%Y.%m.%d')
        self.report_id = f'{self.store_id}-{report_start_string}-{report_end_string}'

        self.data_initial = None
        self.status_inital = 'initiated' 
        self.has_report_initial = None

        self.output = {
            'has_report': self.has_report_initial,
            'status': self.status_inital,
            'table_name': self.table_name,
            'report_id': self.report_id,
            'data': self.data_initial,
            'processed_utc': processed_utc,
        }

    def check_for_report(self):
        check = True if self.pdf_report_start_phrase in self.raw_text else False

        return check

    def get_report_text(self,**kwargs):
        kwargs = kwargs.copy()

        report_end_phrase_occurance = kwargs.pop('report_end_phrase_occurance', None)
        add_more_characters = kwargs.pop('add_more_characters', None)

        get_report_text_kwargs = {
            'raw_text': self.raw_text,
            'report_start_phrase': self.pdf_report_start_phrase,
            'report_end_phrase': self.pdf_report_end_phrase,
        }

        if report_end_phrase_occurance is not None:
            get_report_text_kwargs['report_end_phrase_occurance'] = report_end_phrase_occurance
        if add_more_characters is not None:
            get_report_text_kwargs['add_more_characters'] = add_more_characters

        # Call helper — any missing args use its own defaults
        self.text = helpers.get_report_text(**get_report_text_kwargs)

        self.cleaned_text = helpers.clean_text(
            text=self.text,
            **kwargs,
        )
        return self.cleaned_text

    def build_report_dictionary(self, word_list, columns, **kwargs):
        default_kwargs = {
            'report_id': self.report_id,
            'store_id': self.store_id,
            'report_name': self.table_name,
            'original_filename': self.filename,
            'processed_utc': self.processed_utc,
        }

        merged_kwargs = {**default_kwargs, **kwargs}

        self.data = helpers.build_report_dictionary(
            word_list,
            columns,
            **merged_kwargs,
        )

        helpers.clean_field_values(
            dictionary_list=self.data,
            columns=columns,
        )

        return self.data
    
