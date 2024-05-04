import unittest
from unittest import TestCase, mock
from sqlalchemy import create_engine
from database_connector import DatabaseConnector


class TestDatabaseConnector(TestCase):
    @mock.patch('database_connector.create_engine')
    def test_successful_connection_stg(self, mock_create_engine):
        connector = DatabaseConnector(r'TADAS-THINK\T', 'NT_STG')
        connector.connect()
        mock_create_engine.assert_called_once_with(
            r'mssql+pyodbc://TADAS-THINK\T/NT_STG?driver=ODBC+Driver+17+for+SQL+Server',
            fast_executemany=True, echo=True)

    @mock.patch('database_connector.create_engine')
    def test_successful_connection_dwh(self, mock_create_engine):
        connector = DatabaseConnector(r'TADAS-THINK\T', 'NT_DWH')
        connector.connect()
        mock_create_engine.assert_called_once_with(
            r'mssql+pyodbc://TADAS-THINK\T/NT_DWH?driver=ODBC+Driver+17+for+SQL+Server',
            fast_executemany=True, echo=True)

    @mock.patch('database_connector.create_engine')
    def test_connection_exception(self, mock_create_engine):
        mock_create_engine.side_effect = Exception('Connection error')
        connector = DatabaseConnector('server', 'database')
        with self.assertRaises(Exception):
            connector.connect()


if __name__ == '__main__':
    unittest.main()
