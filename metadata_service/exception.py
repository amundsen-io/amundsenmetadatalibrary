# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0


class NotFoundException(Exception):
    def __init__(self, message: str) -> None:
        """
        Initialize the message.

        Args:
            self: (todo): write your description
            message: (str): write your description
        """
        super().__init__(message)
