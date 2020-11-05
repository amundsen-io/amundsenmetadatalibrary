# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import unittest

from flask import current_app

from metadata_service import create_app


class BasicTestCase(unittest.TestCase):
    """
    Test the service if it can standup
    """

    def setUp(self) -> None:
        """
        Initialize the application.

        Args:
            self: (todo): write your description
        """
        self.app = create_app(
            config_module_class='metadata_service.config.LocalConfig')
        self.app_context = self.app.app_context()
        self.app_context.push()

    def tearDown(self) -> None:
        """
        Starts the application.

        Args:
            self: (todo): write your description
        """
        self.app_context.pop()

    def test_app_exists(self) -> None:
        """
        Check if app exists

        Args:
            self: (todo): write your description
        """
        self.assertFalse(current_app is None)
