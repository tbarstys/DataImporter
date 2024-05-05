import logging
from sqlalchemy import MetaData, Table, select, and_, Column, Integer, String, DateTime, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import hashlib
from concurrent.futures import ThreadPoolExecutor
from database_connector import DatabaseConnector
from datetime import datetime

# Setting up logger
logger = logging.getLogger(__name__)
Base = declarative_base()


class DataMigrator:
    """
    A class used to migrate data from a staging database to a data warehouse.

    ...

    Attributes
    ----------
    stg_engine : Engine
        The SQLAlchemy engine object representing the staging database connection.
    dwh_engine : Engine
        The SQLAlchemy engine object representing the data warehouse database connection.
    stg_metadata : MetaData
        The MetaData object for the staging database.
    dwh_metadata : MetaData
        The MetaData object for the data warehouse database.
    session_stg : Session
        The SQLAlchemy session object for the staging database.
    session_dwh : Session
        The SQLAlchemy session object for the data warehouse database.

    Methods
    -------
    hash_row(row)
        Hashes a database row.
    get_staging_tables()
        Returns a list of tables from the staging database.
    ensure_dwh_table(stg_table_name)
        Ensures the DWH table exists and matches the staging table schema, with additional SCD columns.
    process_table(stg_table_name)
        Processes a table from the staging database and migrates it to the data warehouse.
    run_migration()
        Runs the data migration process for all staging tables.
    """

    def __init__(self, server, stg_database, dwh_database):
        """
        Constructs all the necessary attributes for the DataMigrator object.

        :param server: str
            The server where the databases are hosted.
        :param stg_database: str
            The name of the staging database to connect to.
        :param dwh_database: str
            The name of the data warehouse database to connect to.
        """
        self.stg_engine = DatabaseConnector(server, stg_database).connect()
        self.dwh_engine = DatabaseConnector(server, dwh_database).connect()
        self.stg_metadata = MetaData()
        self.dwh_metadata = MetaData()
        self.session_stg = sessionmaker(bind=self.stg_engine)
        self.session_dwh = sessionmaker(bind=self.dwh_engine)

    @staticmethod
    def hash_row(row):
        """
        Hashes a database row.

        :param row: tuple
            The row to be hashed.
        :return: str
            The hash of the row.
        """
        try:
            logger.debug(f"Hashing row: {row}")
            row_data = ''.join(str(col) for col in row)
            logger.debug(f"Row data: {row_data}")
            return hashlib.sha256(row_data.encode('utf-8')).hexdigest()
        except Exception as e:
            logger.error(f"Error hashing row: {e}")
            raise

    def get_staging_tables(self):
        """
        Returns a list of tables from the staging database.

        :return: list
            A list of table names from the staging database.
        """
        with self.stg_engine.connect() as conn:
            return conn.execute(
                text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")).fetchall()

    def ensure_dwh_table(self, stg_table_name):
        """
        Ensures the DWH table exists and matches the staging table schema, with additional SCD columns.

        :param stg_table_name: str
            The name of the staging table.
        :return: str
            The name of the DWH table.
        """
        logger.debug(f"Ensuring DWH table for {stg_table_name}")
        region_code, dwh_table_name = stg_table_name.split('_', 1)
        stg_table = Table(stg_table_name, self.stg_metadata, autoload_with=self.stg_engine)

        logger.debug(f"Checking if DWH table {dwh_table_name} exists")
        with self.dwh_engine.connect() as conn:
            if not self.dwh_engine.dialect.has_table(conn, dwh_table_name):
                columns = [Column(col.name, col.type) for col in stg_table.columns.values()] + [
                    Column('RegionCode', String(10), default=region_code),
                    Column('RowHash', String(64)),
                    Column('IsActive', Integer, default=1),
                    Column('ValidFrom', DateTime, default=datetime.now()),
                    Column('ValidTo', DateTime)
                ]
                dwh_table = Table(dwh_table_name, self.dwh_metadata, *columns, extend_existing=True)
                logger.debug(f"Creating DWH table {dwh_table_name}")
                dwh_table.create(self.dwh_engine)
        return dwh_table_name

    def process_table(self, stg_table_name):
        """
        Processes a table from the staging database and migrates it to the data warehouse.

        :param stg_table_name: str
            The name of the staging table.
        """
        logger.info(f"Processing table {stg_table_name}")
        dwh_table_name = self.ensure_dwh_table(stg_table_name)
        stg_table = Table(stg_table_name, self.stg_metadata, autoload_with=self.stg_engine)
        dwh_table = Table(dwh_table_name, self.dwh_metadata, autoload_with=self.dwh_engine)

        session_stg = self.session_stg()
        session_dwh = self.session_dwh()

        try:
            # Fetch data from the staging table
            stg_rows = session_stg.execute(select(stg_table)).fetchall()
            logger.debug(f"Processing {len(stg_rows)} rows from {stg_table_name}")
            for stg_row in stg_rows:
                row_hash = self.hash_row(stg_row)
                # Check if the row already exists and is active
                existing_row = session_dwh.execute(
                    select(dwh_table).where(and_(dwh_table.c.RowHash == row_hash, dwh_table.c.IsActive == 1))).first()
                if not existing_row:
                    # Convert the row to a dictionary
                    row_dict = {column: value for column, value in zip(stg_table.columns.keys(), stg_row)}
                    # Insert new active row
                    new_row = {**row_dict, 'RowHash': row_hash, 'IsActive': 1, 'ValidFrom': datetime.now(),
                               'ValidTo': None}
                    session_dwh.execute(dwh_table.insert(), new_row)
                    # Deactivate old rows
                    session_dwh.execute(dwh_table.update().where(dwh_table.c.RowHash != row_hash)
                                        .values(IsActive=0,
                                                ValidTo=datetime.now()))
            session_dwh.commit()
        except SQLAlchemyError as e:
            session_dwh.rollback()
            logger.error(f"Error processing table {stg_table_name}: {e}")
            raise
        finally:
            session_dwh.close()

    def run_migration(self):
        """
        Runs the data migration process for all staging tables.
        """
        tables = self.get_staging_tables()
        # with ThreadPoolExecutor(max_workers=4) as executor:
        #     executor.map(self.process_table, [table[0] for table in tables])
        try:
            for table in tables:
                self.process_table(table[0])
                logger.info(f"Processed table {table[0]}")
        except Exception as e:
            logger.error(f"Error processing tables: {e}")
            raise
        finally:
            logger.info("Data migration complete")
