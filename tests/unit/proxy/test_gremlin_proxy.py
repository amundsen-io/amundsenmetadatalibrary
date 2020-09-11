import copy
from datetime import datetime
import unittest
from typing import Any, Dict, List  # noqa: F401

from amundsen_common.models.table import (
    Application, Column, Source,
    Statistics, Table, Tag, User,
    Watermark, ProgrammaticDescription,
    Reader,

)

from metadata_service.proxy.gremlin_proxy import AbstractGremlinProxy
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from metadata_service import create_app
from metadata_service.entity.dashboard_detail import DashboardDetail
from metadata_service.entity.dashboard_query import DashboardQuery
from metadata_service.entity.resource_type import ResourceType
from metadata_service.util import UserResourceRel

class TestGremlinProxy(unittest.TestCase):

    def setUp(self) -> None:
        self.app = create_app(config_module_class='metadata_service.config.LocalConfig')
        self.app_context = self.app.app_context()
        self.app_context.push()
        self.proxy = AbstractGremlinProxy(key_property_name='key', remote_connection=DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))
        self.clear_graph()
        self.test_user_1 = User(user_id="test_user_1@gmail.com", email="test_user_1@gmail.com")
        self.test_user_2 = User(user_id="test_user_2@gmail.com", email="test_user_2@gmail.com")
        self.test_column_1 = Column(
            name="test_column_1",
            description="test_column_1 description",
            col_type="VARCHAR(128)",
            sort_order=1,
            stats=[]
        )
        self.test_column_2 = Column(
            name="test_column_2",
            description="test_column_2 description",
            col_type="INTEGER",
            sort_order=2,
            stats=[Statistics(
                stat_type="AVERAGE",
                stat_val="3"
            )]
        )
        test_table_columns = [
            self.test_column_1,
            self.test_column_2
        ]
        self.test_table = Table(
            database='test_db',
            cluster='test_cluster',
            schema='test_schema',
            name='test_name',
            columns=test_table_columns,
            is_view=False
        )
        self.table_id = "{db}://{cluster}.{schema}/{tbl}".format(
            db=self.test_table.database,
            cluster=self.test_table.cluster,
            schema=self.test_table.schema,
            tbl=self.test_table.name
        )

    def tearDown(self) -> None:
        self.clear_graph()
        self.proxy.close_driver()

    def clear_graph(self):
        self.proxy.g.E().drop().iterate()
        self.proxy.g.V().drop().iterate()

    def _create_test_users(self, users: List[User]):
        for user in users:
            self._create_test_user(user)

    def _create_test_user(self, user: User):
        user_dict = user.__dict__.copy()
        user_dict = {key: value for key, value in user_dict.items() if value}
        self.proxy.upsert_node(
            node_id=user.user_id,
            node_label='User',
            node_properties=user_dict
        )

    def _create_test_table(self, table: Table):
        table_id = '{db}://{cluster}.{schema}/{tbl}'.format(
            db=table.database,
            cluster=table.cluster,
            schema=table.schema,
            tbl=table.name
        )
        self.proxy.upsert_node(
            node_id=table_id,
            node_label="Table",
            node_properties={
                'name': table.name,
                'is_view': table.is_view
            }
        )
        self._create_test_table_database(table.database)
        self._create_test_table_cluster(table)
        self._create_test_table_schema(table, table_id)
        if table.source:
            self._create_test_table_source(table, table.source)

        for tag in table.tags:
            self._create_test_tag(tag, table_id)

        for badge in table.badges:
            self._create_test_tag(badge, table_id)

        for reader in table.table_readers:
            self._create_test_table_reader(reader, table_id)

        if table.description:
            self._create_test_description(table.description, table_id)

        for column in table.columns:
            self._create_test_columns(column, table_id)

        for owner in table.owners:
            self._create_table_ownership(owner, table_id)

        for water_mark in table.watermarks:
            self._create_table_watermark(water_mark, table_id)

        if table.table_writer is not None:
            self._create_table_application(table.table_writer, table_id)

    def _create_test_table_database(self, database_name):
        database_id = 'database://{db}'.format(db=database_name)
        self.proxy.upsert_node(
            node_id=database_id,
            node_label='Database',
            node_properties={
                'name': database_name
            }
        )

    def _create_test_table_cluster(self, table: Table):
        cluster_id = '{db}://{cluster}'.format(
            db=table.database,
            cluster=table.cluster
        )
        database_id = 'database://{db}'.format(db=table.database)
        self.proxy.upsert_node(
            node_id=cluster_id,
            node_label='Cluster',
            node_properties={
                'name': table.cluster
            }
        )
        self.proxy.upsert_edge(
            start_node_id=cluster_id,
            end_node_id=database_id,
            edge_label="CLUSTER_OF",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=database_id,
            end_node_id=cluster_id,
            edge_label="CLUSTER",
            edge_properties={}
        )

    def _create_test_table_schema(self, table: Table, table_id: str):
        schema_id = '{db}://{cluster}.{schema}'.format(
            db=table.database,
            cluster=table.cluster,
            schema=table.schema
        )
        cluster_id = '{db}://{cluster}'.format(
            db=table.database,
            cluster=table.cluster
        )
        self.proxy.upsert_node(
            node_id=schema_id,
            node_label='Schema',
            node_properties={
                'name': table.schema
            }
        )
        # table to Schema
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=schema_id,
            edge_label="TABLE_OF",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=schema_id,
            end_node_id=table_id,
            edge_label="TABLE",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=cluster_id,
            end_node_id=schema_id,
            edge_label="SCHEMA",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=schema_id,
            end_node_id=cluster_id,
            edge_label="SCHEMA_OF",
            edge_properties={}
        )

    def _create_test_table_source(self, table: Table, source: Source):
        source_id = '{db}://{cluster}.{schema}/{tbl}/_source'.format(
            db=table.database,
            cluster=table.cluster,
            schema=table.schema,
            tbl=table.name
        )
        table_id = '{db}://{cluster}.{schema}/{tbl}'.format(
            db=table.database,
            cluster=table.cluster,
            schema=table.schema,
            tbl=table.name
        )
        self.proxy.upsert_node(
            node_id=source_id,
            node_label='Source',
            node_properties={
                'source': source.source,
                'source_type': source.source_type
            }
        )

        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=source_id,
            edge_label="SOURCE",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=source_id,
            end_node_id=table_id,
            edge_label="SOURCE_OF",
            edge_properties={}
        )

    def _create_test_tag(self, tag: Tag, entity_id: str):
        tag_id = tag.tag_name
        self.proxy.upsert_node(
            node_id=tag_id,
            node_label="Tag",
            node_properties={
                'tag_type': tag.tag_type
            }
        )
        self.proxy.upsert_edge(
            start_node_id=tag_id,
            end_node_id=entity_id,
            edge_label="TAG",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=entity_id,
            end_node_id=tag_id,
            edge_label="TAGGED_BY",
            edge_properties={}
        )

    def _create_test_table_reader(self, reader: Reader, table_id: str):
        self._create_test_user(reader.user)
        self.proxy.upsert_edge(
            start_node_id=reader.user.user_id,
            end_node_id=table_id,
            edge_label="READ",
            edge_properties={
                'read_count': reader.read_count
            }
        )
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=reader.user.user_id,
            edge_label="READ_BY",
            edge_properties={
                'read_count': reader.read_count
            }
        )

    def _create_test_description(self, description: str, entity_id: str):
        description_id = entity_id + "/_description"
        self.proxy.upsert_node(
            node_id=description_id,
            node_label="Description",
            node_properties={
                'description_source': 'description',
                'description': description
            }
        )
        self.proxy.upsert_edge(
            start_node_id=description_id,
            end_node_id=entity_id,
            edge_label="DESCRIPTION_OF",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=entity_id,
            end_node_id=description_id,
            edge_label="DESCRIPTION",
            edge_properties={}
        )

    def _create_test_columns(self, column: Column, table_id: str):
        column_id = table_id + '/' + column.name
        self.proxy.upsert_node(
            node_id=column_id,
            node_label="Column",
            node_properties={
                'name': column.name,
                'type': column.col_type,
                'sort_order': column.sort_order,
            }
        )
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=column_id,
            edge_label="COLUMN",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=column_id,
            end_node_id=table_id,
            edge_label="COLUMN_OF",
            edge_properties={}
        )

        if column.description:
            self._create_test_description(column.description, column_id)


        for stat in column.stats:
            self._create_column_statistics(stat, column_id)

    def _create_column_statistics(self, statistic: Statistics, column_id):
        statistic_id = column_id + '/' + statistic.stat_type
        self.proxy.upsert_node(
            node_id=statistic_id,
            node_label="Stat",
            node_properties={
                'stat_name': statistic.stat_type,
                'stat_val': statistic.stat_val,
                'start_epoch': statistic.start_epoch,
                'end_epoch': statistic.end_epoch
            }
        )
        self.proxy.upsert_edge(
            start_node_id=column_id,
            end_node_id=statistic_id,
            edge_label="STAT",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=statistic_id,
            end_node_id=column_id,
            edge_label="STAT_OF",
            edge_properties={}
        )

    def _create_table_ownership(self, user: User, table_id: str):
        self._create_test_user(user)
        self.proxy.upsert_edge(
            start_node_id=user.email,
            end_node_id=table_id,
            edge_label="OWNER",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=user.user_id,
            edge_label="OWNER_OF",
            edge_properties={}
        )

    def _create_table_watermark(self, water_mark: Watermark, table_id):
        water_mark_id = table_id + '/' + water_mark.watermark_type + '/'
        self.proxy.upsert_node(
            node_id=water_mark_id,
            node_label="Watermark",
            node_properties={
                'partition_key': water_mark.partition_key,
                'partition_value': water_mark.partition_value,
                'create_time': water_mark.create_time
            }
        )
        self.proxy.upsert_edge(
            start_node_id=water_mark_id,
            end_node_id=table_id,
            edge_label="BELONG_TO_TABLE",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=water_mark_id,
            edge_label="WATERMARK",
            edge_properties={}
        )

    def _create_table_application(self, app: Application, table_id):
        self.proxy.upsert_node(
            node_id=app.id,
            node_label="Application",
            node_properties={
                'application_url': app.application_url,
                'name': app.name,
                'description': app.description
            }
        )
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=app.id,
            edge_label="DERIVED_FROM",
            edge_properties={}
        )
        self.proxy.upsert_edge(
            start_node_id=table_id,
            end_node_id=app.id,
            edge_label="GENERATES",
            edge_properties={}
        )

    def test_get_user(self):
        self._create_test_users(users=[self.test_user_1])
        result = self.proxy.get_user(id=self.test_user_1.user_id)
        self.assertEqual(self.test_user_1.user_id, result.user_id)
        self.assertEqual(self.test_user_1.email, result.email)

    def test_get_users(self):
        self._create_test_users(users=[self.test_user_1, self.test_user_2])
        result = self.proxy.get_users()
        self.assertEqual(len(result), 2)

    def test_get_table(self):
        self.test_table.watermarks = [
            Watermark(
                watermark_type='test_water_mark',
                partition_key='1',
                partition_value='25',
                create_time=datetime.utcnow()
            )
        ]

        self._create_test_table(self.test_table)

        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertIsNotNone(result)
        self.assertEqual(self.test_table.database, result.database)
        self.assertEqual(self.test_table.cluster, result.cluster)
        self.assertEqual(self.test_table.schema, result.schema)
        self.assertEqual(self.test_table.name, result.name)
        self.assertEqual(self.test_table.is_view, result.is_view)
        result_columns = result.columns
        self.assertEqual(len(result_columns), 2)
        result_column_test_1 = [result_column for result_column in result_columns if result_column.name == self.test_column_1.name][0]
        result_column_test_2 = [result_column for result_column in result_columns if result_column.name == self.test_column_2.name][0]
        self.assertEqual(result_column_test_1.sort_order, self.test_column_1.sort_order)
        self.assertEqual(result_column_test_1.description, self.test_column_1.description)
        self.assertEqual(result_column_test_1.col_type, self.test_column_1.col_type)

        self.assertEqual(result_column_test_2.sort_order, self.test_column_2.sort_order)
        self.assertEqual(result_column_test_2.description, self.test_column_2.description)
        self.assertEqual(result_column_test_2.col_type, self.test_column_2.col_type)

    def test_get_table_with_tags(self):
        test_tag = Tag(
            tag_type="default",
            tag_name="test_tag"
        )
        self.test_table.tags = [
            test_tag
        ]
        self._create_test_table(self.test_table)

        result = self.proxy.get_table(table_uri=self.table_id)

        self.assertEqual(1, len(result.tags))
        result_tag = result.tags[0]
        self.assertEqual(result_tag.tag_type, test_tag.tag_type)
        self.assertEqual(result_tag.tag_name, test_tag.tag_name)

    def test_get_table_with_readers(self):
        test_reader = Reader(
            user=self.test_user_1,
            read_count=5
        )
        self.test_table.table_readers = [
            test_reader
        ]
        self._create_test_table(self.test_table)

        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual(1, len(result.table_readers))
        result_table_reader = result.table_readers[0]
        self.assertEqual(result_table_reader.user.user_id, self.test_user_1.user_id)
        self.assertEqual(result_table_reader.user.email, self.test_user_1.email)
        self.assertEqual(result_table_reader.read_count, test_reader.read_count)

    def test_get_table_with_description(self):
        self.test_table.description = "This is a test description"
        self._create_test_table(self.test_table)
        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual("This is a test description", result.description)

    def test_get_table_with_owners(self):
        self.test_table.owners = [
            self.test_user_1
        ]
        self._create_test_table(self.test_table)
        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual(1, len(result.owners))
        result_owner = result.owners[0]
        self.assertEqual(result_owner.email, self.test_user_1.email)

    def test_get_table_with_watermarks(self):
        now = datetime.utcnow()
        test_water_mark = Watermark(
            watermark_type='test_water_mark',
            partition_key='1',
            partition_value='25',
            create_time=now
        )
        self.test_table.watermarks = [
            test_water_mark
        ]
        self._create_test_table(self.test_table)
        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual(len(result.watermarks), 1)
        result_water_mark = result.watermarks[0]
        self.assertEqual(result_water_mark.watermark_type, test_water_mark.watermark_type)
        self.assertEqual(result_water_mark.partition_key, test_water_mark.partition_key)
        self.assertEqual(result_water_mark.partition_value, test_water_mark.partition_value)

    def test_get_table_with_writer(self):
        app = Application(
            application_url='http://somewhere',
            description='description',
            id='application://test_cluster.my_app/job/table',
            name='my_app'
        )
        self.test_table.table_writer = app
        self._create_test_table(self.test_table)
        result = self.proxy.get_table(table_uri=self.table_id)
        app = result.table_writer
        self.assertIsNotNone(app)
        self.assertEqual(app.id, 'application://test_cluster.my_app/job/table')
        self.assertEqual(app.name, 'my_app')

    def test_get_table_with_writer_empty_url(self):
        app = Application(
            description='description',
            id='application://test_cluster.my_app/job/table',
            name='my_app'
        )
        self.test_table.table_writer = app
        self._create_test_table(self.test_table)
        result = self.proxy.get_table(table_uri=self.table_id)
        app = result.table_writer
        self.assertIsNotNone(app)
        self.assertEqual(app.application_url, '')

    def test_delete_owner_from_table(self):
        self.test_table.owners = [
            self.test_user_1
        ]
        self._create_test_table(self.test_table)
        self.proxy.delete_owner(table_uri=self.table_id, owner=self.test_user_1.email)
        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual(0, len(result.owners))

    def test_add_owner_to_table(self):
        self._create_test_users(users=[self.test_user_1])
        self._create_test_table(self.test_table)
        self.proxy.add_owner(table_uri=self.table_id, owner=self.test_user_1.email)
        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual(1, len(result.owners))
        result_owner = result.owners[0]
        self.assertEqual(result_owner.email, self.test_user_1.email)

    def test_get_table_description(self):
        self.test_table.description = "This is a test description"
        self._create_test_table(self.test_table)
        result = self.proxy.get_table_description(table_uri=self.table_id)
        self.assertEqual(result, "This is a test description")

    def test_updating_table_description(self):
        self.test_table.description = "This is a test description"
        self._create_test_table(self.test_table)
        self.proxy.put_table_description(table_uri=self.table_id, description="test2")
        result = self.proxy.get_table_description(table_uri=self.table_id)
        self.assertEqual(result, "test2")

    def test_adding_a_tag_to_table(self):
        self._create_test_table(self.test_table)
        self.proxy.add_tag(id=self.table_id, tag='test', tag_type='default')
        result = self.proxy.get_table(table_uri=self.table_id)

        self.assertEqual(1, len(result.tags))
        result_tag = result.tags[0]
        self.assertEqual(result_tag.tag_type, 'default')
        self.assertEqual(result_tag.tag_name, 'test')

    def test_adding_a_tag_to_table_with_existing_tag(self):
        test_tag = Tag(
            tag_type="default",
            tag_name="test_tag"
        )
        self.test_table.tags = [
            test_tag
        ]
        self._create_test_table(self.test_table)
        self.proxy.add_tag(id=self.table_id, tag='test', tag_type='default')
        result = self.proxy.get_table(table_uri=self.table_id)

        self.assertEqual(2, len(result.tags))

    def test_deleting_a_tag(self):
        test_tag = Tag(
            tag_type="default",
            tag_name="test_tag"
        )
        self.test_table.tags = [
            test_tag
        ]
        self._create_test_table(self.test_table)
        self.proxy.delete_tag(id=self.table_id, tag='test_tag', tag_type='default')
        result = self.proxy.get_table(table_uri=self.table_id)
        self.assertEqual(0, len(result.tags))

    def test_get_column_description(self):
        self._create_test_table(self.test_table)
        result = self.proxy.get_column_description(table_uri=self.table_id, column_name=self.test_column_2.name)
        self.assertEqual(result, self.test_column_2.description)

    def test_put_column_description(self):
        self._create_test_table(self.test_table)
        self.proxy.put_column_description(
            table_uri=self.table_id,
            column_name=self.test_column_2.name,
            description="update"
        )
        result = self.proxy.get_column_description(table_uri=self.table_id, column_name=self.test_column_2.name)
        self.assertEqual(result, "update")
        self.proxy.put_column_description(
            table_uri=self.table_id,
            column_name=self.test_column_2.name,
            description="update_2"
        )
        result = self.proxy.get_column_description(table_uri=self.table_id, column_name=self.test_column_2.name)
        self.assertEqual(result, "update_2")

    def test_get_popular_tables(self):
        self.test_table.table_readers = [
            Reader(
                user=self.test_user_1,
                read_count=5
            ),
            Reader(
                user=self.test_user_2,
                read_count=5
            ),
            Reader(
                user=User(
                    user_id="test_user_3@gmail.com",
                    email="test_user_3@gmail.com"
                ),
                read_count=1
            ),
            Reader(
                user=User(
                    user_id="test_user_4@gmail.com",
                    email="test_user_4@gmail.com"
                ),
                read_count=2
            ),
            Reader(
                user=User(
                    user_id="test_user_5@gmail.com",
                    email="test_user_5@gmail.com"
                ),
                read_count=1
            ),
        ]
        self._create_test_table(self.test_table)
        test_table_2 = copy.deepcopy(self.test_table)
        test_table_2.name = 'test_table_2'
        test_table_2.table_readers = [
            Reader(
                user=self.test_user_1,
                read_count=30
            ),
            Reader(
                user=self.test_user_2,
                read_count=10
            ),
            Reader(
                user=User(
                    user_id="test_user_3@gmail.com",
                    email="test_user_3@gmail.com"
                ),
                read_count=5
            ),
            Reader(
                user=User(
                    user_id="test_user_4@gmail.com",
                    email="test_user_4@gmail.com"
                ),
                read_count=8
            ),
            Reader(
                user=User(
                    user_id="test_user_5@gmail.com",
                    email="test_user_5@gmail.com"
                ),
                read_count=7
            ),
        ]
        self._create_test_table(test_table_2)
        result = self.proxy.get_popular_tables(num_entries=5)
        self.assertEqual(len(result), 2)
        first_item = result[0]
        self.assertEqual(first_item.name, 'test_table_2')
        second_item = result[1]
        self.assertEqual(second_item.name, self.test_table.name)

    def test_get_tags(self):
        test_tag = Tag(
            tag_type="default",
            tag_name="test_tag"
        )
        self.test_table.tags = [
            test_tag
        ]
        self._create_test_table(self.test_table)
        result = self.proxy.get_tags()
        self.assertEqual(len(result), 1)

    def test_get_tag_with_no_tables(self):
        self.proxy.upsert_node(
            node_id="test_tag_1",
            node_label="Tag",
            node_properties={
                'tag_type': "default"
            }
        )
        self.proxy.upsert_node(
            node_id="test_tag_2",
            node_label="Tag",
            node_properties={
                'tag_type': "default"
            }
        )
        result = self.proxy.get_tags()
        self.assertEqual(len(result), 2)

    def test_get_table_by_user_relation_follow(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.upsert_edge(
            start_node_id=self.test_user_1.email,
            end_node_id=self.table_id,
            edge_label="FOLLOW",
            edge_properties={}
        )
        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.follow
        )
        self.assertEqual(len(result['table']), 1)
        followed_table = result['table'][0]
        self.assertEqual(self.test_table.name, followed_table.name)

    def test_get_table_by_user_relation_owned(self):
        self.test_table.owners = [
            self.test_user_1
        ]
        self._create_test_table(self.test_table)
        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.own
        )
        self.assertEqual(len(result['table']), 1)
        owned_table = result['table'][0]
        self.assertEqual(self.test_table.name, owned_table.name)

    def test_get_table_by_user_relation_read(self):
        self.test_table.table_readers = [
            Reader(
                user=self.test_user_1,
                read_count=30
            )
        ]
        self._create_test_table(self.test_table)
        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.read
        )
        self.assertEqual(len(result['table']), 1)
        read_table = result['table'][0]
        self.assertEqual(self.test_table.name, read_table.name)

    def test_get_frequently_used_tables(self):
        test_table_2 = copy.deepcopy(self.test_table)
        self.test_table.table_readers = [
            Reader(
                user=self.test_user_1,
                read_count=5
            )
        ]
        self._create_test_table(self.test_table)

        test_table_2.name = 'test_table_2'
        test_table_2.table_readers = [
            Reader(
                user=self.test_user_1,
                read_count=10
            )
        ]
        self._create_test_table(test_table_2)
        result = self.proxy.get_frequently_used_tables(user_email=self.test_user_1.email)
        table_result = result['table']
        self.assertEqual(len(table_result), 2)
        first_result = table_result[0]
        self.assertEqual(first_result.name, test_table_2.name)
        second_result = table_result[1]
        self.assertEqual(second_result.name, self.test_table.name)

    def test_add_resource_relation_by_user_read_table(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.add_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.read,
            resource_type=ResourceType.Table
        )

        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.read
        )
        table_result = result['table']
        self.assertEqual(len(table_result), 1)
        table_result = table_result[0]
        self.assertEqual(table_result.name, self.test_table.name)

    def test_add_resource_relation_by_user_follow_table(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.add_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.follow,
            resource_type=ResourceType.Table
        )

        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.follow
        )
        table_result = result['table']
        self.assertEqual(len(table_result), 1)
        table_result = table_result[0]
        self.assertEqual(table_result.name, self.test_table.name)

    def test_add_resource_relation_by_user_own_table(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.add_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.own,
            resource_type=ResourceType.Table
        )

        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.own
        )
        table_result = result['table']
        self.assertEqual(len(table_result), 1)
        table_result = table_result[0]
        self.assertEqual(table_result.name, self.test_table.name)

    def test_delete_resource_relation_by_user_own_table(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.add_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.own,
            resource_type=ResourceType.Table
        )
        self.proxy.delete_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.own,
            resource_type=ResourceType.Table
        )
        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.own
        )
        self.assertEqual(len(result['table']), 0)

    def test_delete_resource_relation_by_user_read_table(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.add_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.read,
            resource_type=ResourceType.Table
        )
        self.proxy.delete_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.read,
            resource_type=ResourceType.Table
        )
        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.read
        )
        self.assertEqual(len(result['table']), 0)

    def test_delete_resource_relation_by_user_follow_table(self):
        self._create_test_user(self.test_user_1)
        self._create_test_table(self.test_table)
        self.proxy.add_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.follow,
            resource_type=ResourceType.Table
        )
        self.proxy.delete_resource_relation_by_user(
            id=self.table_id,
            user_id=self.test_user_1.email,
            relation_type=UserResourceRel.follow,
            resource_type=ResourceType.Table
        )
        result = self.proxy.get_table_by_user_relation(
            user_email=self.test_user_1.email,
            relation_type=UserResourceRel.follow
        )
        self.assertEqual(len(result['table']), 0)

    def test_get_latest_updated_ts(self):
        now = datetime(year=2020, month=10, day=10, hour=1, minute=1)
        self.proxy.upsert_node(
            node_id='amundsen_updated_timestamp',
            node_label='Updatedtimestamp',
            node_properties={
                'datetime': now
            }
        )
        result = self.proxy.get_latest_updated_ts()
        self.assertAlmostEqual(result, int(now.timestamp()))

    def test_get_lastest_updated_ts_no_node(self):
        result = self.proxy.get_latest_updated_ts()
        self.assertIsNone(result)


if __name__ == '__main__':
    unittest.main()
