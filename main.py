import argparse
import logging
import logging.config
import os
import shutil
import zipfile
import yaml
from csv_file_handler import CSVFileHandler
from database_importer import DatabaseImporter
from data_migrator import DataMigrator

# Setting up logger
logger = logging.getLogger(__name__)


def setup_logging(default_path='logging.yaml', default_level=logging.INFO, env_key='LOG_CFG'):
    """
    Set up logging configuration.

    :param default_path: The default path for the logging configuration file.
    :param default_level: The default logging level.
    :param env_key: The environment variable key for the logging configuration file path.
    """
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def archive_file(csv_file_path, complete_file_path, archive_dir_path):
    """
    Archive the CSV file and move the .complete file

    :param csv_file_path: The path of the CSV file to be archived.
    :param complete_file_path: The path of the .complete file to be moved.
    :param archive_dir_path: The path of the directory where the files will be moved.
    """
    try:
        logger.info(f'Archiving file {csv_file_path} to {archive_dir_path}')

        base_name = os.path.splitext(os.path.basename(csv_file_path))[0]
        zip_file_path = os.path.join(archive_dir_path, f'{base_name}.zip')
        with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
            archive_name = os.path.basename(csv_file_path)
            zip_file.write(csv_file_path, archive_name)

        # Remove the original .csv file after compressing
        os.remove(csv_file_path)
        logger.info(f'File {csv_file_path} has been archived to {zip_file_path}')

        # Move the .complete file to the archive folder
        shutil.move(complete_file_path, archive_dir_path)
        logger.info(f'File {complete_file_path} has been moved to {archive_dir_path}')
    except Exception as e:
        logger.error(f'Error archiving file: {e}', exc_info=True)


def main(path, server, database_stg, database_dwh):
    """
    Main function to import data from CSV to SQL, archive the files and run the data migration process.

    :param path: The path of the CSV files.
    :param server: The SQL server name.
    :param database_stg: The SQL STG database name.
    :param database_dwh: The SQL DWH database name.
    """
    setup_logging(default_level=logging.DEBUG)

    try:
        logger.info(f'Importing data from {path} to {server}/{database_stg}')
        file_handler = CSVFileHandler(path)
        eligible_files = file_handler.list_eligible_files()
        for file in eligible_files:
            file_with_path = os.path.join(path, file)

            table_name, df = file_handler.parse_csv(file_with_path)
            if table_name and df is not None:
                logger.info(f'Parsed CSV file {file} into DataFrame and table name {table_name}')

                # Import the data to SQL
                data_importer = DatabaseImporter(server, database_stg)
                data_importer.insert_data(table_name, df, file_with_path)

                # Archive the file
                archive_dir_path = os.path.join(path, 'Archive')
                complete_file_path = file_with_path.replace('.csv', '.complete')
                if not os.path.exists(archive_dir_path):
                    os.makedirs(archive_dir_path)
                archive_file(file_with_path, complete_file_path, archive_dir_path)

            else:
                logger.error(f'Error parsing CSV file {file}')

        # Run the data migration process
        migrator = DataMigrator(server, database_stg, database_dwh)
        migrator.run_migration()
    except Exception as e:
        logger.error(f'Error importing data: {e}', exc_info=True)
        raise


if __name__ == '__main__':
    # Argument parser for command line arguments
    parser = argparse.ArgumentParser(description='Data Importer from CSV to SQL')
    parser.add_argument('--path', type=str, required=True, help='CSV file path')
    parser.add_argument('--server', type=str, required=True, help='SQL Server to import data')
    parser.add_argument('--database_stg', type=str, required=True, help='SQL STG Database to import data')
    parser.add_argument('--database_dwh', type=str, required=True, help='SQL DWH Database to import data')
    args = parser.parse_args()

    main(args.path, args.server, args.database_stg, args.database_dwh)
