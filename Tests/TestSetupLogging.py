import unittest
from unittest.mock import patch, MagicMock
from main import setup_logging


class TestSetupLogging(unittest.TestCase):
    @patch('main.os')
    @patch('main.logging')
    @patch('main.yaml')
    def test_logging_setup_happy_path(self, mock_yaml, mock_logging, mock_os):
        mock_os.getenv.return_value = None
        mock_os.path.exists.return_value = True
        mock_yaml.safe_load.return_value = MagicMock()

        setup_logging(default_path='..\\logging.yaml', default_level='logging.INFO')

        mock_os.getenv.assert_called()
        mock_os.path.exists.assert_called()
        mock_yaml.safe_load.assert_called()
        mock_logging.config.dictConfig.assert_called()

    @patch('main.os.path.exists')
    @patch('main.os.getenv')
    @patch('main.logging')
    @patch('main.yaml')
    def test_logging_setup_with_env_var(self, mock_yaml, mock_logging, mock_getenv, mock_exists):
        mock_getenv.return_value = '..\\logging.yaml'
        mock_exists.side_effect = lambda filepath: filepath == mock_getenv.return_value
        mock_yaml.safe_load.return_value = MagicMock()

        setup_logging()

        mock_getenv.assert_called()
        mock_exists.assert_called()
        mock_yaml.safe_load.assert_called()
        mock_logging.config.dictConfig.assert_called()

    @patch('main.os')
    @patch('main.logging')
    def test_logging_setup_with_no_config_file(self, mock_logging, mock_os):
        mock_os.getenv.return_value = None
        mock_os.path.exists.return_value = False

        setup_logging()

        mock_os.getenv.assert_called()
        mock_os.path.exists.assert_called()
        mock_logging.basicConfig.assert_called()


if __name__ == '__main__':
    unittest.main()
