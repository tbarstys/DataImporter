import logging
from sqlalchemy import create_engine

# Setting up logger
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """
    A class used to establish a connection to a database.

    ...

    Attributes
    ----------
    server : str
        The server where the database is hosted.
    database : str
        The name of the database to connect to.
    engine : Engine
        The SQLAlchemy engine object representing the database connection.

    Methods
    -------
    connect()
        Establishes a connection to the database and returns the engine object.
    """

    def __init__(self, server, database):
        """
        Constructs all the necessary attributes for the DatabaseConnector object.

        :param server: str
            The server where the database is hosted.
        :param database: str
            The name of the database to connect to.
        """
        self.server = server
        self.database = database
        self.engine = None

    def connect(self):
        """
        Establishes a connection to the database.

        This method uses the server and database attributes of the DatabaseConnector instance to form the connection
        string. The connection is established using SQLAlchemy's create_engine function, with the fast_executemany
        and echo parameters set to True. If the connection is successful, the method logs a success message and
        returns the engine object. If an exception occurs during the connection process, the method logs the error
        and re-raises the exception.

        :return: Engine
            An SQLAlchemy engine object representing the database connection.
        :raises Exception:
            If there is an error connecting to the database.
        """
        try:
            logger.info(f'Connecting to database {self.server}/{self.database}')
            self.engine = create_engine(rf'mssql+pyodbc://{self.server}/{self.database}?'
                                        rf'driver=ODBC+Driver+17+for+SQL+Server',
                                        fast_executemany=True, echo=True)
            logger.info(f'Connected to database {self.server}/{self.database}')
            return self.engine
        except Exception as e:
            logger.error(f'Error connecting to database: {e}', exc_info=True)
            raise
