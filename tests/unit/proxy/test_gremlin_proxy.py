import copy
import textwrap
import unittest
from typing import Any, Dict  # noqa: F401

from amundsen_common.models.dashboard import DashboardSummary
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import (Application, Column, Source,
                                          Statistics, Table, Tag, User,
                                          Watermark, ProgrammaticDescription)
from amundsen_common.models.user import UserSchema
from mock import MagicMock, patch
from neo4j import GraphDatabase

from metadata_service.proxy.gremlin_proxy import AbstractGremlinProxy
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from metadata_service import create_app
from metadata_service.entity.dashboard_detail import DashboardDetail
from metadata_service.entity.dashboard_query import DashboardQuery
from metadata_service.entity.resource_type import ResourceType
from metadata_service.entity.tag_detail import TagDetail
from metadata_service.exception import NotFoundException
from metadata_service.proxy.neo4j_proxy import Neo4jProxy
from metadata_service.util import UserResourceRel


class TestGremlinProxy(unittest.TestCase):

    def setUp(self) -> None:
        self.app = create_app(config_module_class='metadata_service.config.LocalConfig')
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.proxy = AbstractGremlinProxy(key_property_name='key', remote_connection=DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

    def test_get_user(self):
        result = self.proxy.get_user(id="test_user@gmail.com")


if __name__ == '__main__':
    unittest.main()
