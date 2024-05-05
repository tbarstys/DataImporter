import unittest
from unittest.mock import patch, MagicMock
from data_migrator import DataMigrator
from sqlalchemy.exc import SQLAlchemyError


class TestDataMigrator(unittest.TestCase):

    @patch('data_migrator.DatabaseConnector')
    def test_initialization(self, mock_db_connector):
        mock_db_connector.return_value.connect.return_value = 'mock_engine'
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        self.assertEqual(migrator.stg_engine, 'mock_engine')
        self.assertEqual(migrator.dwh_engine, 'mock_engine')

    def test_hash_row(self):
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        row = ('value1', 'value2', 'value3')
        expected_hash = '6a31538580a68d5586352f0d53278d740de4420387730d339f296e731268ec17'
        self.assertEqual(migrator.hash_row(row), expected_hash)

    @patch('data_migrator.DataMigrator.get_staging_tables')
    @patch('data_migrator.DataMigrator.process_table')
    def test_run_migration(self, mock_process_table, mock_get_staging_tables):
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        mock_get_staging_tables.return_value = [('table1',), ('table2',)]
        migrator.run_migration()
        mock_process_table.assert_any_call('table1')
        mock_process_table.assert_any_call('table2')

    @patch('data_migrator.DataMigrator.get_staging_tables')
    @patch('data_migrator.DataMigrator.process_table')
    def test_run_migration_with_exception(self, mock_process_table, mock_get_staging_tables):
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        mock_get_staging_tables.return_value = [('table1',), ('table2',)]
        mock_process_table.side_effect = SQLAlchemyError
        with self.assertRaises(SQLAlchemyError):
            migrator.run_migration()

    @patch('data_migrator.DataMigrator.process_table')
    @patch('data_migrator.Table')
    @patch('data_migrator.sessionmaker')
    def test_process_table(self, mock_sessionmaker, mock_table, mock_process_table):
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        mock_process_table.return_value = 'dwh_table'
        mock_session = MagicMock()
        mock_sessionmaker.return_value = mock_session
        migrator.process_table('stg_table')

    @patch('data_migrator.DataMigrator.process_table')
    @patch('data_migrator.Table')
    @patch('data_migrator.sessionmaker')
    def test_process_table_with_exception(self, mock_sessionmaker, mock_table, mock_process_table):
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        mock_process_table.return_value = 'dwh_table'
        mock_session = MagicMock()
        mock_session.execute.side_effect = SQLAlchemyError
        mock_sessionmaker.return_value = mock_session
        with self.assertRaises(SQLAlchemyError):
            migrator.process_table('stg_table')

    @patch('data_migrator.DataMigrator.ensure_dwh_table')
    @patch('data_migrator.Table')
    @patch('data_migrator.sessionmaker')
    def test_process_table_with_exception(self, mock_sessionmaker, mock_table, mock_ensure_dwh_table):
        migrator = DataMigrator('server', 'stg_db', 'dwh_db')
        mock_ensure_dwh_table.return_value = 'dwh_table'
        mock_session = MagicMock()
        mock_session.execute.side_effect = SQLAlchemyError
        mock_sessionmaker.return_value = mock_session
        with self.assertRaises(SQLAlchemyError):
            migrator.process_table('stg_table')


if __name__ == '__main__':
    unittest.main()
