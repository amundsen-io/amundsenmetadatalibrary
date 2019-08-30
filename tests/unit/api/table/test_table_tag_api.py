from http import HTTPStatus
from unittest import mock
from mock import MagicMock

from metadata_service.exception import NotFoundException
from tests.unit.api.table.table_test_case import TableTestCase

TABLE_URI = 'wizards'
TAG = 'underage_wizards'


class TestTableTagAPI(TableTestCase):

    @mock.patch('metadata_service.api.table.SearchProxy')
    def test_should_update_tag_without_elastic(self, SearchProxy: MagicMock) -> None:
        self.app.config['SHOULD_UPDATE_ELASTIC'] = False
        mock_search = SearchProxy(config=self.app.config)
        self.mock_proxy.get_table_search_document.return_value = {'name': 'test'}
        response = self.app.test_client().put(f'/table/{TABLE_URI}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.add_tag.assert_called_once_with(table_uri=TABLE_URI,
                                                        tag=TAG,
                                                        tag_type='default')
        self.mock_proxy.get_table_search_document.assert_not_called()
        mock_search.update_elastic.assert_not_called()

    @mock.patch('metadata_service.api.table.SearchProxy')
    def test_should_update_tag(self, SearchProxy: MagicMock) -> None:
        self.app.config['SHOULD_UPDATE_ELASTIC'] = True
        mock_search = SearchProxy(config=self.app.config)
        data = {'name': 'test'}
        self.mock_proxy.get_table_search_document.return_value = data
        response = self.app.test_client().put(f'/table/{TABLE_URI}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.add_tag.assert_called_once_with(table_uri=TABLE_URI,
                                                        tag=TAG,
                                                        tag_type='default')
        self.mock_proxy.get_table_search_document.assert_called_once_with(table_uri=TABLE_URI)
        mock_search.update_elastic.assert_called_once_with(table_uri=TABLE_URI, data=data)

    def test_should_fail_to_update_tag_when_table_not_found(self) -> None:
        self.mock_proxy.add_tag.side_effect = NotFoundException(message='cannot find table')

        response = self.app.test_client().put(f'/table/{TABLE_URI}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    def test_should_delete_tag(self) -> None:
        response = self.app.test_client().delete(f'/table/{TABLE_URI}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.delete_tag.assert_called_once_with(table_uri=TABLE_URI,
                                                           tag=TAG,
                                                           tag_type='default')

    def test_should_fail_to_delete_tag_when_table_not_found(self) -> None:
        self.mock_proxy.delete_tag.side_effect = NotFoundException(message='cannot find table')

        response = self.app.test_client().delete(f'/table/{TABLE_URI}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
