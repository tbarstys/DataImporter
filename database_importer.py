import logging
import math
import pandas as pd
from sqlalchemy import create_engine, Index, MetaData, Table, Column, Integer, String, DateTime, BigInteger, DECIMAL, \
    NVARCHAR, DATE, Boolean, text, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.ddl import CreateIndex

# Setting up logger
logger = logging.getLogger(__name__)


class DatabaseImporter:
    """
    A class used to import data from a DataFrame into a database.

    ...

    Attributes
    ----------
    server : str
        The server where the database is hosted.
    database : str
        The name of the database to connect to.
    engine : Engine
        The SQLAlchemy engine object representing the database connection.
    table : Table
        The SQLAlchemy Table object representing the table in the database.

    Methods
    -------
    connect()
        Establishes a connection to the database.
    insert_data(table_name, df, path_to_csv)
        Creates a table in the database and inserts data from a DataFrame or a CSV file.
    create_table(table_name, df)
        Creates a table in the database based on the DataFrame columns and their data types.
    infer_data_types(df)
        Analyzes the data types of the DataFrame columns and returns a list of columns with their data types.
    infer_sql_type(series)
        Maps the Pandas data types to SQL data types.
    roundup(x)
        Rounds up a number to the nearest 10.
    """

    def __init__(self, server, database):
        """
        Constructs all the necessary attributes for the DatabaseImporter object.

        :param server: str
            The server where the database is hosted.
        :param database: str
            The name of the database to connect to.
        """
        self.server = server
        self.database = database
        self.engine = None
        self.table = None

    def connect(self):
        """
        Establishes a connection to the database.

        This method uses the server and database attributes of the DatabaseImporter instance to form the connection
        string. The connection is established using SQLAlchemy's create_engine function, with the fast_executemany
        and echo parameters set to True. If the connection is successful, the method logs a success message. If an
        exception occurs during the connection process, the method logs the error and re-raises the exception.

        :raises Exception:
            If there is an error connecting to the database.
        """
        try:
            logger.info(f'Connecting to database {self.server}/{self.database}')
            self.engine = create_engine(r'mssql+pyodbc://TADAS-THINK\T/NT_STG?driver=ODBC+Driver+17+for+SQL+Server',
                                        fast_executemany=True, echo=True)
            logger.info(f'Connected to database {self.server}/{self.database}')
        except Exception as e:
            logger.error(f'Error connecting to database: {e}', exc_info=True)
            raise

    def insert_data(self, table_name, df, path_to_csv):
        """
        Creates a table in the database and inserts data from a DataFrame or a CSV file.

        This method first establishes a connection to the database, then creates a table in the database based on the
        DataFrame columns and their data types. If the DataFrame has less than 1,000,000 rows, the data is inserted
        into the table in batches of 10,000 rows. If the DataFrame has 1,000,000 rows or more, the data is inserted
        into the table using a BULK INSERT query. If an exception occurs during the data insertion process, the method
        logs the error and re-raises the exception.

        :param table_name: str
            The name of the table to be created in the database.
        :param df: DataFrame
            The DataFrame containing the data to be inserted into the table.
        :param path_to_csv: str
            The path to the CSV file containing the data to be inserted into the table.
        :raises Exception:
            If there is an error inserting data into the table.
        """
        try:
            self.connect()
            logger.info(f'Creating table {table_name}')
            self.create_table(table_name, df)

            if len(df) < 1000000:
                with self.engine.connect() as connection:
                    with connection.begin() as transaction:
                        try:
                            batch_size = 10000
                            for start in range(0, len(df), batch_size):
                                end = min(start + batch_size, len(df))
                                df[start:end].to_sql(table_name, connection, if_exists='append', index=False)
                                logger.debug(f'Batch inserted: {start} - {end}')
                            transaction.commit()
                        except Exception as e:
                            logger.error(f'Error inserting data from DataFrame: {e}', exc_info=True)
                            transaction.rollback()
                            raise
            else:
                bulk_insert_query = (f'BULK INSERT [dbo].[{table_name}] FROM \'{path_to_csv}\' '
                                     f'WITH (FIRSTROW=2, FIELDTERMINATOR=\',\', '
                                     f'ROWTERMINATOR=\'0x0a\', CODEPAGE=\'65001\', TABLOCK)')
                with self.engine.connect() as connection:
                    try:
                        connection.execute(text(bulk_insert_query))
                        connection.commit()
                        logger.info(f'Data inserted into table {table_name}')
                    except Exception as e:
                        logger.error(f'Error bulk inserting data from CSV: {e}', exc_info=True)
                        connection.rollback()
                        raise
        except Exception as e:
            logger.error(f'Error inserting data: {e}', exc_info=True)
            raise

    def create_table(self, table_name, df):
        """
        Creates a table in the database based on the DataFrame columns and their data types.

        This method first defines a custom compiler for the CreateIndex function that creates a clustered columnstore
        index. It then creates a new table in the database with the same columns as the DataFrame, and adds a clustered
        columnstore index to the table. If a table with the same name already exists in the database, it is dropped
        before the new table is created. If an exception occurs during the table creation process, the method logs the
        error and re-raises the exception.

        :param table_name: str
            The name of the table to be created in the database.
        :param df: DataFrame
            The DataFrame based on which the table is to be created.
        :raises Exception:
            If there is an error creating the table.
        """
        try:
            @compiles(CreateIndex, "mssql")
            def compile_create_index(create, compiler, **kw):
                preparer = compiler.preparer
                mssql_opts = create.element.dialect_options['mssql']
                columnstore = mssql_opts.get('columnstore', None)
                if not columnstore:
                    stmt = compiler.visit_create_index(create, **kw)
                    return stmt
                name = preparer.format_index(create.element)
                table_name_compiled: object = preparer.format_table(create.element.table)
                stmt = f'CREATE CLUSTERED COLUMNSTORE INDEX {name} ON {table_name_compiled}'
                return stmt

            Index.argument_for('mssql', 'columnstore', True)
            metadata = MetaData()

            columns = self.infer_data_types(df)
            self.table = Table(table_name, metadata, Index(f'CCI_{table_name}',
                                                           mssql_columnstore=True), *columns, schema='dbo')

            metadata.drop_all(self.engine, checkfirst=True)
            metadata.create_all(self.engine)
            logger.info(f'Table {table_name} created in database {self.server}/{self.database}')
        except Exception as e:
            logger.error(f'Error creating table: {e}', exc_info=True)
            raise

    def infer_data_types(self, df):
        """
        Analyzes the data types of the DataFrame columns and returns a list of columns with their data types.

        This method iterates over the columns of the DataFrame and infers their data types using the infer_sql_type
        method. It returns a list of Column objects with the inferred data types. If an exception occurs during the
        data type inference process, the method logs the error and re-raises the exception.

        :param df: DataFrame
            The DataFrame for which to infer the data types.
        :return: list
            A list of Column objects with the inferred data types.
        :raises Exception:
            If there is an error inferring the data types.
        """
        try:
            columns = []
            for column in df.columns:
                columns.append(Column(column, self.infer_sql_type(df[column])))
                logger.debug(f'Column {column} has data type {df[column].dtype}')
            return columns
        except Exception as e:
            logger.error(f'Error inferring data types: {e}', exc_info=True)
            raise

    def infer_sql_type(self, series):
        """
        Maps the Pandas data types to SQL data types.

        This method checks the data type of a Pandas Series and returns the corresponding SQL data type. If the Series
        contains float data, it returns a DECIMAL data type. If the Series contains numeric data, it returns an Integer
        or BigInteger data type depending on the maximum and minimum values in the Series. If the Series contains
        datetime data, it returns a DateTime data type. If the Series contains string data, it returns a String, NVARCHAR,
        or DATE data type depending on the contents of the strings. If the Series contains boolean data, it returns a
        Boolean data type. If the Series contains any other data type, it returns a String data type. If an exception
        occurs during the data type mapping process, the method logs the error and re-raises the exception.

        :param series: Series
            The Pandas Series for which to infer the SQL data type.
        :return: TypeEngine
            The SQLAlchemy TypeEngine object representing the inferred SQL data type.
        :raises Exception:
            If there is an error inferring the SQL data type.
        """
        try:
            if pd.api.types.is_float_dtype(series):
                return DECIMAL(precision=38, scale=8)
            elif pd.api.types.is_numeric_dtype(series.dropna()):
                if series.max() < 32767 and series.min() > -32767:
                    return Integer()
                else:
                    return BigInteger()
            elif pd.api.types.is_datetime64_any_dtype(series):
                return DateTime()
            elif pd.api.types.is_string_dtype(series) or pd.api.types.is_object_dtype(series):
                length = 'MAX' if int(series.str.len().max()) > 4000 else self.roundup(series.str.len().max())
                if series.str.contains(r'^\d{4}-\d{2}-\d{2}').all():
                    return DATE
                elif series.str.contains(r'[^\x00-\x7F]').any():
                    return NVARCHAR(length + 10 if type(length) is int else length)
                else:
                    return String(length)
            elif pd.api.types.is_bool_dtype(series):
                return Boolean
            else:
                return String(255)
        except Exception as e:
            logger.error(f'Error inferring SQL data type: {e}', exc_info=True)
            raise

    @staticmethod
    def roundup(x):
        """
        Rounds up a number to the nearest 10.

        This method takes a number and rounds it up to the nearest 10. It uses the math.ceil function to round up the
        number divided by 10, then multiplies the result by 10.

        :param x: int
            The number to be rounded up.
        :return: int
            The number rounded up to the nearest 10.
        """
        return math.ceil(x / 10.0) * 10
