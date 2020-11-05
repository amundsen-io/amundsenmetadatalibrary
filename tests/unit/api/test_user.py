# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import unittest

from http import HTTPStatus
from unittest import mock
from unittest.mock import MagicMock
from metadata_service import create_app

from metadata_service.api.user import (UserDetailAPI, UserFollowAPI, UserFollowsAPI,
                                       UserOwnsAPI, UserOwnAPI, UserReadsAPI)

from metadata_service.util import UserResourceRel
from metadata_service.entity.resource_type import ResourceType


class UserDetailAPITest(unittest.TestCase):
    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        """
        Create a mock proxy for this proxy.

        Args:
            self: (todo): write your description
            mock_get_proxy_client: (float): write your description
        """
        self.app = create_app(config_module_class='metadata_service.config.LocalConfig')
        self.app_context = self.app.app_context()
        self.app_context.push()

        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserDetailAPI()

    def test_get(self) -> None:
        """
        Get the test status of the test.

        Args:
            self: (todo): write your description
        """
        self.mock_client.get_user.return_value = {}
        response = self.api.get(id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_user.assert_called_once_with(id='username')

    def test_gets(self) -> None:
        """
        : return :

        Args:
            self: (todo): write your description
        """
        self.mock_client.get_users.return_value = []
        response = self.api.get()
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_users.assert_called_once()


class UserFollowsAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        """
        Sets the proxy.

        Args:
            self: (todo): write your description
            mock_get_proxy_client: (float): write your description
        """
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserFollowsAPI()

    def test_get(self) -> None:
        """
        Return the current dash dockboard.

        Args:
            self: (todo): write your description
        """
        self.mock_client.get_table_by_user_relation.return_value = {'table': []}
        self.mock_client.get_dashboard_by_user_relation.return_value = {'dashboard': []}

        response = self.api.get(user_id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_table_by_user_relation.assert_called_once()


class UserFollowAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        """
        Sets the proxy.

        Args:
            self: (todo): write your description
            mock_get_proxy_client: (float): write your description
        """
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserFollowAPI()

    def test_table_put(self) -> None:
        """
        Updates the current resource.

        Args:
            self: (todo): write your description
        """
        response = self.api.put(user_id='username', resource_type='table', resource_id='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.add_resource_relation_by_user.assert_called_with(id='3',
                                                                          user_id='username',
                                                                          relation_type=UserResourceRel.follow,
                                                                          resource_type=ResourceType.Table)

    def test_dashboard_put(self) -> None:
        """
        Add dashboard dashboard.

        Args:
            self: (todo): write your description
        """
        response = self.api.put(user_id='username', resource_type='dashboard', resource_id='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.add_resource_relation_by_user.assert_called_with(id='3',
                                                                          user_id='username',
                                                                          relation_type=UserResourceRel.follow,
                                                                          resource_type=ResourceType.Dashboard)

    def test_table_delete(self) -> None:
        """
        Deletes the resource.

        Args:
            self: (todo): write your description
        """
        response = self.api.delete(user_id='username', resource_type='table', resource_id='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.delete_resource_relation_by_user.assert_called_with(id='3',
                                                                             user_id='username',
                                                                             relation_type=UserResourceRel.follow,
                                                                             resource_type=ResourceType.Table)

    def test_dashboard_delete(self) -> None:
        """
        Delete dashboard dashboard.

        Args:
            self: (todo): write your description
        """
        response = self.api.delete(user_id='username', resource_type='dashboard', resource_id='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.delete_resource_relation_by_user.assert_called_with(id='3',
                                                                             user_id='username',
                                                                             relation_type=UserResourceRel.follow,
                                                                             resource_type=ResourceType.Dashboard)


class UserOwnsAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        """
        Sets a mock for the given mock.

        Args:
            self: (todo): write your description
            mock_get_proxy_client: (float): write your description
        """
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserOwnsAPI()

    def test_get(self) -> None:
        """
        Return the dash dash dash dashboard.

        Args:
            self: (todo): write your description
        """
        self.mock_client.get_table_by_user_relation.return_value = {'table': []}
        self.mock_client.get_dashboard_by_user_relation.return_value = {'dashboard': []}
        response = self.api.get(user_id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.get_table_by_user_relation.assert_called_once()
        self.mock_client.get_dashboard_by_user_relation.assert_called_once()


class UserOwnAPITest(unittest.TestCase):

    @mock.patch('metadata_service.api.user.get_proxy_client')
    def setUp(self, mock_get_proxy_client: MagicMock) -> None:
        """
        Sets a mock for the given connection.

        Args:
            self: (todo): write your description
            mock_get_proxy_client: (float): write your description
        """
        self.mock_client = mock.Mock()
        mock_get_proxy_client.return_value = self.mock_client
        self.api = UserOwnAPI()

    def test_put(self) -> None:
        """
        Updates the resource.

        Args:
            self: (todo): write your description
        """
        response = self.api.put(user_id='username', resource_type='2', table_uri='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.add_owner.assert_called_once()

    def test_delete(self) -> None:
        """
        Deletes the test.

        Args:
            self: (todo): write your description
        """
        response = self.api.delete(user_id='username', resource_type='2', table_uri='3')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        self.mock_client.delete_owner.assert_called_once()


class UserReadsAPITest(unittest.TestCase):
    @mock.patch('metadata_service.api.user.get_proxy_client')
    def test_get(self, mock_get_proxy_client: MagicMock) -> None:
        """
        Get the value of a mock.

        Args:
            self: (todo): write your description
            mock_get_proxy_client: (todo): write your description
        """
        mock_client = mock.Mock()
        mock_get_proxy_client.return_value = mock_client
        mock_client.get_frequently_used_tables.return_value = {'table': []}
        api = UserReadsAPI()
        response = api.get(user_id='username')
        self.assertEqual(list(response)[1], HTTPStatus.OK)
        mock_client.get_frequently_used_tables.assert_called_once()
