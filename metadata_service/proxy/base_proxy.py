from abc import ABCMeta, abstractmethod

from typing import Union, List, Dict, Any

from metadata_service.entity.popular_table import PopularTable
from metadata_service.entity.user_detail import User as UserEntity
from metadata_service.entity.table_detail import Table
from metadata_service.util import UserResourceRel


class BaseProxy(metaclass=ABCMeta):
    """
    Base Proxy, which behaves like an interface for all
    the proxy clients available in the amundsen metadata service
    """
    @abstractmethod
    def get_user_detail(self, *, user_id: str) -> Union[UserEntity, None]:
        pass

    @abstractmethod
    def get_table(self, *, key: str, table_info: Dict) -> Table:
        pass

    @abstractmethod
    def delete_owner(self, *, key: str, owner: str) -> None:
        pass

    @abstractmethod
    def add_owner(self, *, key: str, owner: str) -> None:
        pass

    @abstractmethod
    def get_table_description(self, *,
                              key: str) -> Union[str, None]:
        pass

    @abstractmethod
    def put_table_description(self, *,
                              key: str,
                              description: str) -> None:
        pass

    @abstractmethod
    def add_tag(self, *, key: str, tag: str) -> None:
        pass

    @abstractmethod
    def delete_tag(self, *, key: str, tag: str) -> None:
        pass

    @abstractmethod
    def put_column_description(self, *,
                               key: str,
                               column_name: str,
                               description: str) -> None:
        pass

    @abstractmethod
    def get_column_description(self, *,
                               key: str,
                               column_name: str) -> Union[str, None]:
        pass

    @abstractmethod
    def get_popular_tables(self, *,
                           num_entries: int =10) -> List[PopularTable]:
        pass

    @abstractmethod
    def get_latest_updated_ts(self) -> int:
        pass

    @abstractmethod
    def get_tags(self) -> List:
        pass

    @abstractmethod
    def get_table_by_user_relation(self, *, user_email: str,
                                   relation_type: UserResourceRel) -> Dict[str, Any]:
        pass

    @abstractmethod
    def add_table_relation_by_user(self, *,
                                   key: str,
                                   user_email: str,
                                   relation_type: UserResourceRel) -> None:
        pass

    @abstractmethod
    def delete_table_relation_by_user(self, *,
                                      key: str,
                                      user_email: str,
                                      relation_type: UserResourceRel) -> None:
        pass
