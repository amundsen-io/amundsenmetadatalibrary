# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from metadata_service.proxy import BaseProxy
from abc import abstractmethod
from amundsen_common.models.table import Table, Application, Column, ProgrammaticDescription
from amundsen_common.models.user import User
from typing import List


class RoundtripBaseProxy(BaseProxy):
    """
    A base proxy that supports roundtrip tests
    """
    @abstractmethod
    def put_user(self, *, data: User) -> None:
        """
        Create a user.

        Args:
            self: (todo): write your description
            data: (str): write your description
        """
        pass

    @abstractmethod
    def post_users(self, *, data: List[User]) -> None:
        """
        Post / users / : login.

        Args:
            self: (todo): write your description
            data: (str): write your description
        """
        pass

    @abstractmethod
    def put_app(self, *, data: Application) -> None:
        """
        R appends an application.

        Args:
            self: (todo): write your description
            data: (todo): write your description
        """
        pass

    @abstractmethod
    def post_apps(self, *, data: List[Application]) -> None:
        """
        Create new apps.

        Args:
            self: (todo): write your description
            data: (array): write your description
        """
        pass

    @abstractmethod
    def put_table(self, *, table: Table) -> None:
        """
        Put a table.

        Args:
            self: (todo): write your description
            table: (str): write your description
        """
        pass

    @abstractmethod
    def post_tables(self, *, tables: List[Table]) -> None:
        """
        Post / update tables.

        Args:
            self: (todo): write your description
            tables: (list): write your description
        """
        pass

    @abstractmethod
    def put_column(self, *, table_uri: str, column: Column) -> None:
        """
        Puts a column.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            column: (todo): write your description
        """
        pass

    @abstractmethod
    def put_programmatic_table_description(self, *, table_uri: str, description: ProgrammaticDescription) -> None:
        """
        Put the description of a table_program.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            description: (str): write your description
        """
        pass

    @abstractmethod
    def add_read_count(self, *, table_uri: str, user_id: str, read_count: int) -> None:
        """
        Adds a read count.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            user_id: (str): write your description
            read_count: (int): write your description
        """
        pass
