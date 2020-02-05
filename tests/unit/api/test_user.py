import unittest

from http import HTTPStatus
from unittest import mock
from mock import MagicMock

from metadata_service.api.user import (UserDetailAPI, UserFollowAPI, UserFollowsAPI,
                                       UserOwnsAPI, UserOwnAPI, UserReadsAPI)
from metadata_service.util import UserResourceRel


class UserDetailAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserDetailAPI()

    def test_get(self) -> None:
        self.mock_client.get_user.return_value = {}
        response = self.api.get(id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_user.assert_called_once_with(id='username')

    def test_gets(self) -> None:
        self.mock_client.get_users.return_value = []
        response = self.api.get()
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_users.assert_called_once_with()


class UserFollowsAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserFollowsAPI()

    def test_get(self) -> None:
        self.mock_client.get_table_by_user_relation.return_value = {'table': []}
        response = self.api.get(id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_table_by_user_relation.assert_called_once_with(user_email='username',
                                                                            relation_type=UserResourceRel.follow)


class UserFollowAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserFollowAPI()

    def test_put(self) -> None:
        response = self.api.put(id='username', resource_type='2', table_uri='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.add_table_relation_by_user.assert_called_once_with(user_email='username',
                                                                            table_uri='3',
                                                                            relation_type=UserResourceRel.follow)

    def test_delete(self) -> None:
        response = self.api.delete(id='username', resource_type='2', table_uri='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.delete_table_relation_by_user.assert_called_once_with(user_email='username',
                                                                               table_uri='3',
                                                                               relation_type=UserResourceRel.follow)


class UserOwnsAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserOwnsAPI()

    def test_get(self) -> None:
        self.mock_client.get_table_by_user_relation.return_value = {'table': []}
        response = self.api.get(id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_table_by_user_relation.assert_called_once_with(user_email='username',
                                                                            relation_type=UserResourceRel.own)


class UserOwnAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserOwnAPI()

    def test_put(self) -> None:
        response = self.api.put(id='username', resource_type='2', table_uri='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.add_owner.assert_called_once_with(owner='username', table_uri='3')

    def test_delete(self) -> None:
        response = self.api.delete(id='username', resource_type='2', table_uri='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.delete_owner.assert_called_once_with(owner='username', table_uri='3')


class UserReadsAPITest(unittest.TestCase):
    @mock.patch('metadata_service.api.user.get_proxy_client')
    def test_get(self, mock_get_proxy_client: MagicMock) -> None:
        mock_client = mock.Mock()
        mock_get_proxy_client.return_value = mock_client
        mock_client.get_table_by_user_relation.return_value = {'table': []}
        api = UserReadsAPI()
        response = api.get(id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        mock_client.get_frequently_used_tables.assert_called_once_with(user_email='username')
