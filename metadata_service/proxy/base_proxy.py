# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from abc import ABCMeta, abstractmethod
from typing import Any, Dict, List, Union

from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import Table
from amundsen_common.models.user import User
from amundsen_common.models.dashboard import DashboardSummary

from metadata_service.entity.dashboard_detail import DashboardDetail as DashboardDetailEntity
from metadata_service.entity.description import Description
from metadata_service.entity.resource_type import ResourceType
from metadata_service.util import UserResourceRel


class BaseProxy(metaclass=ABCMeta):
    """
    Base Proxy, which behaves like an interface for all
    the proxy clients available in the amundsen metadata service
    """

    @abstractmethod
    def get_user(self, *, id: str) -> Union[User, None]:
        """
        Get a user instance.

        Args:
            self: (todo): write your description
            id: (int): write your description
        """
        pass

    @abstractmethod
    def get_users(self) -> List[User]:
        """
        Get the list of all users.

        Args:
            self: (todo): write your description
        """
        pass

    @abstractmethod
    def get_table(self, *, table_uri: str) -> Table:
        """
        Gets the table.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
        """
        pass

    @abstractmethod
    def delete_owner(self, *, table_uri: str, owner: str) -> None:
        """
        Deletes the specified owner.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            owner: (todo): write your description
        """
        pass

    @abstractmethod
    def add_owner(self, *, table_uri: str, owner: str) -> None:
        """
        Add owner of the owner.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            owner: (todo): write your description
        """
        pass

    @abstractmethod
    def get_table_description(self, *,
                              table_uri: str) -> Union[str, None]:
        """
        Returns the description of a table.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
        """
        pass

    @abstractmethod
    def put_table_description(self, *,
                              table_uri: str,
                              description: str) -> None:
        """
        Adds a description for a table description.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            description: (str): write your description
        """
        pass

    @abstractmethod
    def add_tag(self, *, id: str, tag: str, tag_type: str, resource_type: ResourceType) -> None:
        """
        Add a tag

        Args:
            self: (todo): write your description
            id: (str): write your description
            tag: (str): write your description
            tag_type: (str): write your description
            resource_type: (str): write your description
        """
        pass

    @abstractmethod
    def add_badge(self, *, id: str, badge_name: str, category: str = '',
                  resource_type: ResourceType) -> None:
        """
        Add badge

        Args:
            self: (todo): write your description
            id: (todo): write your description
            badge_name: (str): write your description
            category: (str): write your description
            resource_type: (str): write your description
        """
        pass

    @abstractmethod
    def delete_tag(self, *, id: str, tag: str, tag_type: str, resource_type: ResourceType) -> None:
        """
        Delete a tag

        Args:
            self: (todo): write your description
            id: (str): write your description
            tag: (str): write your description
            tag_type: (str): write your description
            resource_type: (str): write your description
        """
        pass

    @abstractmethod
    def delete_badge(self, *, id: str, badge_name: str, category: str,
                     resource_type: ResourceType) -> None:
        """
        Delete a badge

        Args:
            self: (todo): write your description
            id: (str): write your description
            badge_name: (str): write your description
            category: (str): write your description
            resource_type: (str): write your description
        """
        pass

    @abstractmethod
    def put_column_description(self, *,
                               table_uri: str,
                               column_name: str,
                               description: str) -> None:
        """
        Add a column description.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            column_name: (str): write your description
            description: (str): write your description
        """
        pass

    @abstractmethod
    def get_column_description(self, *,
                               table_uri: str,
                               column_name: str) -> Union[str, None]:
        """
        Return the description of a column.

        Args:
            self: (todo): write your description
            table_uri: (str): write your description
            column_name: (str): write your description
        """
        pass

    @abstractmethod
    def get_popular_tables(self, *, num_entries: int) -> List[PopularTable]:
        """
        Returns the number of - tables that list of num_entries.

        Args:
            self: (todo): write your description
            num_entries: (int): write your description
        """
        pass

    @abstractmethod
    def get_latest_updated_ts(self) -> int:
        """
        Get the updated timestamp.

        Args:
            self: (todo): write your description
        """
        pass

    @abstractmethod
    def get_tags(self) -> List:
        """
        Get the list of tags.

        Args:
            self: (todo): write your description
        """
        pass

    @abstractmethod
    def get_badges(self) -> List:
        """
        Returns the list.

        Args:
            self: (todo): write your description
        """
        pass

    @abstractmethod
    def get_dashboard_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) \
            -> Dict[str, List[DashboardSummary]]:
        """
        Get a user s dashboard for the given user.

        Args:
            self: (todo): write your description
            user_email: (str): write your description
            relation_type: (str): write your description
        """
        pass

    @abstractmethod
    def get_table_by_user_relation(self, *, user_email: str,
                                   relation_type: UserResourceRel) -> Dict[str, Any]:
        """
        Returns the user - specified relations.

        Args:
            self: (todo): write your description
            user_email: (str): write your description
            relation_type: (str): write your description
        """
        pass

    @abstractmethod
    def get_frequently_used_tables(self, *, user_email: str) -> Dict[str, Any]:
        """
        Return a list of the user sdk_email.

        Args:
            self: (todo): write your description
            user_email: (todo): write your description
        """
        pass

    @abstractmethod
    def add_resource_relation_by_user(self, *,
                                      id: str,
                                      user_id: str,
                                      relation_type: UserResourceRel,
                                      resource_type: ResourceType) -> None:
        """
        Add a relationship between the given resource.

        Args:
            self: (todo): write your description
            id: (int): write your description
            user_id: (int): write your description
            relation_type: (str): write your description
            resource_type: (todo): write your description
        """
        pass

    @abstractmethod
    def delete_resource_relation_by_user(self, *,
                                         id: str,
                                         user_id: str,
                                         relation_type: UserResourceRel,
                                         resource_type: ResourceType) -> None:
        """
        Delete a relationship between the given resource type.

        Args:
            self: (todo): write your description
            id: (str): write your description
            user_id: (int): write your description
            relation_type: (str): write your description
            resource_type: (str): write your description
        """
        pass

    @abstractmethod
    def get_dashboard(self,
                      dashboard_uri: str,
                      ) -> DashboardDetailEntity:
        """
        Get a dashboard with the given dashboard.

        Args:
            self: (todo): write your description
            dashboard_uri: (str): write your description
        """
        pass

    @abstractmethod
    def get_dashboard_description(self, *,
                                  id: str) -> Description:
        """
        Get a dashboard description.

        Args:
            self: (todo): write your description
            id: (int): write your description
        """
        pass

    @abstractmethod
    def put_dashboard_description(self, *,
                                  id: str,
                                  description: str) -> None:
        """
        Put a dashboard description.

        Args:
            self: (todo): write your description
            id: (int): write your description
            description: (str): write your description
        """
        pass

    @abstractmethod
    def get_resources_using_table(self, *,
                                  id: str,
                                  resource_type: ResourceType) -> Dict[str, List[DashboardSummary]]:
        """
        Return the resource_type for the given resource.

        Args:
            self: (todo): write your description
            id: (int): write your description
            resource_type: (str): write your description
        """
        pass
