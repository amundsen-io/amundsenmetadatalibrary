# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from http import HTTPStatus

from metadata_service.exception import NotFoundException
from metadata_service.entity.resource_type import ResourceType

from tests.unit.api.dashboard.dashboard_test_case import DashboardTestCase

ID = 'wizards'
TAG = 'underage_wizards'


class TestDashboardTagAPI(DashboardTestCase):

    def test_should_update_tag(self) -> None:
        """
        Check if the resource tag should be sentboard.

        Args:
            self: (todo): write your description
        """
        response = self.app.test_client().put(f'/dashboard/{ID}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.add_tag.assert_called_with(id=ID,
                                                   tag=TAG,
                                                   tag_type='default',
                                                   resource_type=ResourceType.Dashboard)

    def test_should_fail_to_update_tag_when_table_not_found(self) -> None:
        """
        Test if the test tag to make_tag_when_table.

        Args:
            self: (todo): write your description
        """
        self.mock_proxy.add_tag.side_effect = NotFoundException(message='foo')

        response = self.app.test_client().put(f'/dashboard/{ID}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)

    def test_should_delete_tag(self) -> None:
        """
        Ensures that the resource should be deleted.

        Args:
            self: (todo): write your description
        """
        response = self.app.test_client().delete(f'/dashboard/{ID}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.delete_tag.assert_called_with(id=ID,
                                                      tag=TAG,
                                                      tag_type='default',
                                                      resource_type=ResourceType.Dashboard)

    def test_should_fail_to_delete_tag_when_table_not_found(self) -> None:
        """
        This method to make_should_table_when_table_table.

        Args:
            self: (todo): write your description
        """
        self.mock_proxy.delete_tag.side_effect = NotFoundException(message='foo')

        response = self.app.test_client().delete(f'/dashboard/{ID}/tag/{TAG}')

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
