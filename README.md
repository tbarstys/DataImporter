# DataImport

DataImport solution is a data migration tool that imports data from CSV files into a SQL database, archives
the processed files, and then runs a data migration process.  

The script begins by importing necessary modules and setting up a logger for logging events throughout the script's 
execution. The logger is set up in the setup_logging function, which takes a path to a logging configuration file, 
a default logging level, and an environment variable key for the logging configuration file path as parameters.

```python
def setup_logging(default_path='logging.yaml', default_level=logging.INFO, env_key='LOG_CFG'):
```

The archive_file function is used to archive a given CSV file and move the corresponding .complete file to an archive 
directory. It takes the paths of the CSV file, the .complete file, and the archive directory as parameters.

```python
def archive_file(csv_file_path, complete_file_path, archive_dir_path):
```

The main function is the heart of the script. It takes the path of the CSV files, the SQL server name, and the names of 
the STG and DWH databases as parameters. It starts by setting up the logger and then proceeds to import data from the 
CSV files to the STG database. It does this by creating a CSVFileHandler object and using it to list eligible files and 
parse them into a DataFrame and a table name. The data is then imported to the SQL database using a DatabaseImporter 
object. After the data is imported, the CSV file and its corresponding .complete file are archived. Finally, the data 
migration process is run using a DataMigrator object.

```python
def main(path, server, database_stg, database_dwh):
```

The script is run from the command line, and it uses the argparse module to parse command line arguments. The arguments 
required are the path of the CSV files, the SQL server name, and the names of the STG and DWH databases.

```python
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Data Importer from CSV to SQL')
    parser.add_argument('--path', type=str, required=True, help='CSV file path')
    parser.add_argument('--server', type=str, required=True, help='SQL Server name to import data')
    parser.add_argument('--database_stg', type=str, required=True, help='SQL STG Database name to import data')
    parser.add_argument('--database_dwh', type=str, required=True, help='SQL DWH Database name to import data')
    args = parser.parse_args()

    main(args.path, args.server, args.database_stg, args.database_dwh)
```

The CSVFileHandler, DatabaseImporter, and DataMigrator classes are imported from separate modules and are used to handle 
CSV files, import data to the SQL database, and run the data migration process, respectively.

