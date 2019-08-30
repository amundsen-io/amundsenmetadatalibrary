import json
import unittest
from typing import Any, Dict, List, Tuple  # noqa: F401

from mock import MagicMock, patch
from flask import Flask

from metadata_service import create_app
from metadata_service.proxy.search_proxy import SearchProxy


class TestSearchProxy(unittest.TestCase):

    def setUp(self) -> None:
        self.app: Flask = create_app(config_module_class='metadata_service.config.LocalConfig')
        self.app_context = self.app.app_context()
        self.app_context.push()

    def tearDown(self) -> None:
        self.app_context.pop()

    @patch('metadata_service.proxy.search_proxy.time')
    @patch('metadata_service.proxy.search_proxy.request_search')
    def test_update_elastic(self, mock_search: MagicMock, mock_time: MagicMock) -> None:
        mock_time.time.return_value = 12345
        search_proxy = SearchProxy(config=self.app.config)
        search_proxy.update_elastic(table_uri='test', data={'name': 'test_name'})
        expected_data = json.dumps({"data": '[{"name": "test_name", "last_updated_epoch": 12345}]'})
        mock_search.assert_called_once_with(url='http://0.0.0.0:60479/document_table',
                                            data=expected_data,
                                            method='PUT',
                                            config=self.app.config)
