import logging
import math
import pandas as pd
from sqlalchemy import create_engine, Index, MetaData, Table, Column, Integer, String, DateTime, BigInteger, DECIMAL, \
    NVARCHAR, DATE, Boolean, text, select
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.ddl import CreateIndex

logger = logging.getLogger(__name__)


class DatabaseImporter:
    def __init__(self, server, database):
        self.server = server
        self.database = database
        self.engine = None
        self.table = None

    def connect(self):
        """
        Connect to the database
        :return:
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
        Creates table in the database and insert data
        Uses BULK INSERT for faster data insertion
        :param table_name:
        :param df:
        :param path_to_csv:
        :return:
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
        Dynamically creates table in the database based on the DataFrame columns and their data types
        Adds Clustered Columnstore index to the table
        Database schema is hardcoded to dbo
        Drops a table if it already exists
        :param table_name:
        :param df:
        :return:
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
        Analyzes the data types of the DataFrame columns and returns a list of columns with their data types
        :param df:
        :return:
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
        Maps the Pandas data types to SQL data types
        :return:
        :param series:
        :return:
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
        return math.ceil(x / 10.0) * 10
