import unittest
from unittest.mock import patch, MagicMock
from csv_file_handler import CSVFileHandler


class TestCSVFileHandler(unittest.TestCase):
    @patch('csv_file_handler.os')
    @patch('csv_file_handler.re')
    def test_valid_file_happy_path(self, mock_re, mock_os):
        mock_re.match.return_value = MagicMock()
        mock_os.path.exists.return_value = True

        handler = CSVFileHandler('test_directory')
        result = handler._is_valid_file('test_file.csv')

        self.assertTrue(result)

    @patch('csv_file_handler.os')
    @patch('csv_file_handler.re')
    def test_valid_file_no_match(self, mock_re, mock_os):
        mock_re.match.return_value = None

        handler = CSVFileHandler('test_directory')
        result = handler._is_valid_file('test_file.csv')

        self.assertFalse(result)

    @patch('csv_file_handler.os')
    @patch('csv_file_handler.re')
    def test_valid_file_no_complete_file(self, mock_re, mock_os):
        mock_re.match.return_value = MagicMock()
        mock_os.path.exists.return_value = False

        handler = CSVFileHandler('test_directory')
        result = handler._is_valid_file('test_file.csv')

        self.assertFalse(result)

    @patch('csv_file_handler.os')
    def test_valid_file_in_directory(self, mock_os):
        mock_os.listdir.return_value = ['file1.csv', 'file2.csv']
        mock_os.path.join.side_effect = lambda *args: f'{args[0]}/{args[1]}'
        handler = CSVFileHandler('test_directory')
        handler._is_valid_file = MagicMock(return_value=True)
        result = handler.list_eligible_files()
        self.assertEqual(result, ['file1.csv', 'file2.csv'])

    @patch('csv_file_handler.os')
    def test_no_valid_files_in_directory(self, mock_os):
        mock_os.listdir.return_value = ['file1.csv', 'file2.csv']
        handler = CSVFileHandler('test_directory')
        handler._is_valid_file = MagicMock(return_value=False)
        result = handler.list_eligible_files()
        self.assertEqual(result, [])

    @patch('csv_file_handler.os')
    def test_some_valid_files_in_directory(self, mock_os):
        mock_os.listdir.return_value = ['file1.csv', 'file2.csv']
        handler = CSVFileHandler('test_directory')
        handler._is_valid_file = MagicMock(side_effect=[True, False])
        result = handler.list_eligible_files()
        self.assertEqual(result, ['file1.csv'])

    @patch('csv_file_handler.os')
    def test_empty_directory(self, mock_os):
        mock_os.listdir.return_value = []
        handler = CSVFileHandler('test_directory')
        result = handler.list_eligible_files()
        self.assertEqual(result, [])

    @patch('csv_file_handler.os')
    def test_list_eligible_files_with_no_valid_files(self, mock_os):
        mock_os.listdir.return_value = ['file1_20220101.csv', 'file2_20220101.csv']
        mock_os.path.exists.return_value = False
        handler = CSVFileHandler('test_directory')
        result = handler.list_eligible_files()
        self.assertEqual(result, [])

    @patch('csv_file_handler.os')
    def test_list_eligible_files_with_empty_directory(self, mock_os):
        mock_os.listdir.return_value = []
        handler = CSVFileHandler('test_directory')
        result = handler.list_eligible_files()
        self.assertEqual(result, [])


if __name__ == '__main__':
    unittest.main()
