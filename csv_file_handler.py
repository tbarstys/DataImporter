import logging
import re
import pandas as pd
import os

logger = logging.getLogger(__name__)


class CSVFileHandler:
    def __init__(self, directory_path):
        self.directory_path = directory_path
        self.filename_pattern = r'(.+)_(\d{8})\.csv$'

    def _is_valid_file(self, file_path):
        """
        Check if the file is a CSV file and holds corresponding .complete file

        :param file_path:
        :return:
        """
        match = re.match(self.filename_pattern, file_path)
        logger.debug(f'Checking file {file_path}')
        if not match:
            return False
        else:
            base_filename = match.group(1)
            filename_date = match.group(2)
            complete_filename = f'{base_filename}_{filename_date}.complete'
            logger.debug(f'Complete file found: {complete_filename}')
            return os.path.exists(os.path.join(self.directory_path, complete_filename))

    def list_eligible_files(self):
        """
        List all the files in the directory that have corresponding .complete files
        :return:
        """
        return [f for f in os.listdir(self.directory_path) if self._is_valid_file(f)]

    def parse_csv(self, filepath):
        """
        Parse the CSV file and return table name and the data as a Pandas DataFrame

        :param filepath:
        :return:
        """
        try:
            table_name = re.match(self.filename_pattern, os.path.basename(filepath)).group(1)
            df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
            return table_name, df
        except Exception as e:
            logger.error(f'Error parsing CSV file: {e}', exc_info=True)
            return None, None
