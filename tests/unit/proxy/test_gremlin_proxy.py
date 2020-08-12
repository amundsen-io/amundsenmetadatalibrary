import copy
import textwrap
import unittest
from typing import Any, Dict, List  # noqa: F401

from amundsen_common.models.dashboard import DashboardSummary
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import (Application, Column, Source,
                                          Statistics, Table, Tag, User,
                                          Watermark, ProgrammaticDescription)
from amundsen_common.models.user import UserSchema
from mock import MagicMock, patch
from neo4j import GraphDatabase

from metadata_service.proxy.gremlin_proxy import AbstractGremlinProxy
from gremlin_python.process.traversal import T, Cardinality
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from metadata_service import create_app
from metadata_service.entity.dashboard_detail import DashboardDetail
from metadata_service.entity.dashboard_query import DashboardQuery
from metadata_service.entity.resource_type import ResourceType
from metadata_service.entity.tag_detail import TagDetail
from metadata_service.exception import NotFoundException
from metadata_service.proxy.neo4j_proxy import Neo4jProxy
from metadata_service.util import UserResourceRel


TABLE_NODE_PROPERTIES = [
    'name',
    'is_view'
]


class TestGremlinProxy(unittest.TestCase):

    def setUp(self) -> None:
        self.app = create_app(config_module_class='metadata_service.config.LocalConfig')
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.proxy = AbstractGremlinProxy(key_property_name='key', remote_connection=DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))
        self.test_user_1 = User(user_id="test_user_1@gmail.com", email="test_user_1@gmail.com")
        self.test_user_2 = User(user_id="test_user_2@gmail.com", email="test_user_2@gmail.com")

    def tearDown(self) -> None:
        self.proxy.g.E().drop().iterate()
        self.proxy.g.V().drop().iterate()

    def _create_test_users(self, users: List[User]):
        for user in users:
            self._create_test_user(user)

    def _create_test_user(self, user: User):
        user_dict = user.__dict__.copy()
        user_dict = {key: value for key, value in user_dict.items() if value}
        proxy_transversal = self.proxy.g.addV('User').property(Cardinality.single, self.proxy.key_property_name, user.user_id)

        for user_property_name, user_property_value in user_dict.items():
            if user_property_name == 'user_id':
                continue
            proxy_transversal = proxy_transversal.property(Cardinality.single, user_property_name, user_property_value)

        proxy_transversal.next()

    def _create_test_table(self, table: Table):
        table_database = table.database
        table_cluster = table.cluster
        table_schema = table.schema
        table_source = table.source
        table_tags = table.tags
        table_badges = table.badges
        table_readers = table.table_readers
        table_description = table.description
        table_columns = table.columns
        table_owners = table.owners
        table_watermarks = table.watermarks

    def _create_test_database(self, database_name):
        database_id = 'database://{db}'.format(db=database_name)
        self.proxy.upsert_node(
            node_id=database_id,
            node_label='Database',
            node_properties={
                'name': database_name
            }
        )

    def test_get_user(self):
        self._create_test_users(users=[self.test_user_1])
        result = self.proxy.get_user(id="test_user@gmail.com")
        self.assertEqual(self.test_user_1.user_id, result.user_id)
        self.assertEqual(self.test_user_1.email, result.email)


if __name__ == '__main__':
    unittest.main()
