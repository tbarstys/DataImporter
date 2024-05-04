import unittest
from unittest.mock import patch, MagicMock
from main import archive_file


class TestArchiveFile(unittest.TestCase):
    @patch('main.os')
    @patch('main.shutil')
    @patch('main.zipfile.ZipFile')
    def test_archive_file_happy_path(self, mock_zipfile, mock_shutil, mock_os):
        mock_zipfile.return_value.__enter__.return_value = MagicMock()

        archive_file('test.csv', 'test.complete', 'archive')

        mock_os.path.splitext.assert_called()
        mock_os.path.basename.assert_called()
        mock_os.path.join.assert_called()
        mock_zipfile.assert_called()
        mock_os.remove.assert_called()
        mock_shutil.move.assert_called()

    @patch('main.zipfile.ZipFile')
    def test_archive_file_with_exception(self, mock_zipfile):
        def side_effect():
            raise Exception('Test exception')

        with self.assertRaises(Exception) as context:
            mock_zipfile.side_effect = side_effect
            archive_file('test.csv', 'test.complete', 'archive')
            self.assertTrue('Test exception' in str(context.exception))


if __name__ == '__main__':
    unittest.main()
