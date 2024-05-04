import unittest
from unittest import TestCase, mock
from sqlalchemy import create_engine, text
from data_migrator import DataMigrator


class DataMigratorTest(TestCase):
    def setUp(self):
        self.server = r'TADAS-THINK\T'
        self.stg_database = 'NT_STG'
        self.dwh_database = 'NT_DWH'

        self.migrator = DataMigrator(self.server, self.stg_database, self.dwh_database)
        
    @mock.patch('sqlalchemy.create_engine')
    def test_staging_tables_retrieval(self, mock_create_engine):
        mock_conn = mock.MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = ['table1', 'table2']
        mock_create_engine.return_value.connect.return_value.__enter__.return_value = mock_conn

        migrator = DataMigrator(self.server, self.stg_database, self.dwh_database)
        result = migrator.get_staging_tables()

        self.assertEqual(result, ['table1', 'table2'])
        mock_conn.execute.assert_called_once_with(
            text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))

    @mock.patch('sqlalchemy.create_engine')
    def test_staging_tables_retrieval_no_tables(self, mock_create_engine):
        mock_conn = mock.MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = []
        mock_create_engine.return_value.connect.return_value.__enter__.return_value = mock_conn

        migrator = DataMigrator(self.server, self.stg_database, self.dwh_database)
        result = migrator.get_staging_tables()

        self.assertEqual(result, [])
        mock_conn.execute.assert_called_once_with(
            text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))

    @mock.patch('sqlalchemy.create_engine')
    @mock.patch('data_migrator.Table')
    def test_dwh_table_exists(self, mock_table, mock_create_engine):
        mock_conn = mock.MagicMock()
        mock_create_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        mock_table.return_value = mock.MagicMock()
        mock_create_engine.return_value.dialect.has_table.return_value = True

        migrator = DataMigrator(self.server, self.stg_database, self.dwh_database)
        result = migrator.ensure_dwh_table('region_table')

        self.assertEqual(result, 'DWH_table')
        mock_create_engine.return_value.dialect.has_table.assert_called_once()

    @mock.patch('sqlalchemy.create_engine')
    @mock.patch('data_migrator.Table')
    def test_dwh_table_does_not_exist(self, mock_table, mock_create_engine):
        mock_conn = mock.MagicMock()
        mock_create_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
        mock_table.return_value = mock.MagicMock()
        mock_create_engine.return_value.dialect.has_table.return_value = False

        migrator = DataMigrator(self.server, self.stg_database, self.dwh_database)
        result = migrator.ensure_dwh_table('region_table')

        self.assertEqual(result, 'DWH_table')
        mock_table.assert_called_once()
        mock_table.return_value.create.assert_called_once()


if __name__ == '__main__':
    unittest.main()
