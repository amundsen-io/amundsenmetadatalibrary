import json
import logging
from datetime import datetime
from random import randint
from typing import Any, Dict, List, Mapping, Optional, Union

import gremlin_python
from gremlin_python.process.traversal import T, Order, gt, Cardinality, within, endingWith
from gremlin_python.process.graph_traversal import __
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import Table, Column, Reader, Tag, Watermark
from amundsen_common.models.user import User as UserEntity
from amundsen_common.models.dashboard import DashboardSummary
from gremlin_python.driver.driver_remote_connection import \
    DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from metadata_service.exception import NotFoundException
from metadata_service.entity.tag_detail import TagDetail

from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options

from metadata_service.entity.dashboard_detail import DashboardDetail as DashboardDetailEntity
from metadata_service.entity.description import Description
from metadata_service.entity.resource_type import ResourceType
from metadata_service.proxy import BaseProxy
from metadata_service.util import UserResourceRel
from metadata_service.entity.dashboard_detail import DashboardDetail as DashboardDetailEntity
from metadata_service.entity.dashboard_query import DashboardQuery as DashboardQueryEntity

__all__ = ['AbstractGremlinProxy', 'GenericGremlinProxy']

LOGGER = logging.getLogger(__name__)


_CACHE = CacheManager(**parse_cache_config_options({'cache.type': 'memory'}))

# Expire cache every 11 hours + jitter
_GET_POPULAR_TABLE_CACHE_EXPIRY_SEC = 11 * 60 * 60 + randint(0, 3600)


def _parse_gremlin_server_error(exception: Exception) -> Dict[str, Any]:
    if not isinstance(exception, gremlin_python.driver.protocol.GremlinServerError) or len(exception.args) != 1:
        return {}
    # this is like '444: {...json object...}'
    return json.loads(exception.args[0][exception.args[0].index(': ') + 1:])


class DashboardTraversal:
    def __init__(self, g: GraphTraversalSource, dashboard_uri: str, key_property_name: str):
        self.g = g
        self.dashboard_uri = dashboard_uri
        self.key_property_name = key_property_name

    def get_dashboard(self):
        ds_traversal = self.g.V().hasKey(self.key_property_name, self.dashboard_uri)
        ds_traversal = ds_traversal.hasLabel("Dashboard")
        ds_traversal = ds_traversal.project(
            'cluster_name',
            'uri',
            'url',
            'name',
            'created_timestamp',
            'description',
            'group',
            'last_successful_run_timestamp',
            'last_run',
            'updated_timestamp',
            'owners',
            'tags',
            'recent_view_count',
            'queries',
            'charts',
            'tables'
        )
        ds_traversal = ds_traversal.by(
            __.out('DASHBOARD_OF').out('DASHBOARD_GROUP_OF').values('name')
        )  # cluster_name
        ds_traversal = ds_traversal.by(
            self.key_property_name
        )  # uri
        ds_traversal = ds_traversal.by(
            'dashboard_url'
        )  # url
        ds_traversal = ds_traversal.by(
            'name'
        )  # name
        ds_traversal = ds_traversal.by(
            'created_timestamp'
        )  # created_timestamp
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_description_traversal()
        )  # description
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_dashboard_group_traversal()
        )  # group
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_last_successful_execution_traversal(self.key_property_name)
        )  # last_successful_run_timestamp
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_last_execution_traversal(self.key_property_name)
        )  # last_run
        ds_traversal = ds_traversal.by(
            __.out('LAST_UPDATED_AT').values('timestamp')
        )  # updated_timestamp
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_owners_traversal(self.key_property_name)
        )  # owners
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_tags_traversal()
        )  # tags
        ds_traversal = ds_traversal.by(
            __.outE('READ_BY').values('read_count').fold().sum()
        )  # recent_view_count
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_dashboard_query_traversal()
        )  # queries
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_dashboard_chart_traversal()
        )  # charts
        ds_traversal = ds_traversal.by(
            DashboardTraversal._create_dashboard_tables_traversal()
        )  # tables

        dashboard_result = ds_traversal.next()
        owners = []
        for owner in dashboard_result['owners']:
            owners.append(UserEntity(
                user_id=owner['user_id'],
                email=owner['email']
            ))
        tags = [Tag(tag_type=tag['tag_type'], tag_name=tag['key']) for tag in dashboard_result['tags']]
        chart_names = dashboard_result['charts']
        query_names = [query['name'] for query in dashboard_result['queries'] if 'name' in query and query['name']]
        queries = [
            DashboardQueryEntity(
                name=query['name'],
                url=query['url'],
                query_text=query['query_text'],
            )
            for query in dashboard_result['queries']
            if query.get('name') or query.get('url') or query.get('text')
        ]
        tables = [
            PopularTable(
                database=table['database_name'],
                cluster=table['cluster_name'],
                schema=table['schema_name'],
                name=table['table_name'],
                description=table['table_description']
            )
            for table in dashboard_result['tables']
            if 'name' in table and table['name']
        ]
        product = dashboard_result['uri'].split('_')[0]
        return DashboardDetailEntity(
            uri=dashboard_result['uri'],
            cluster=dashboard_result['cluster_name'],
            url=dashboard_result['url'],
            name=dashboard_result['name'],
            product=product,
            created_timestamp=dashboard_result['created_timestamp'],
            description=dashboard_result.get('description'),
            group_name=dashboard_result.get('group').get('name'),
            group_url=dashboard_result.get('group').get('url'),
            last_successful_run_timestamp=dashboard_result.get('last_successful_run_timestamp'),
            last_run_timestamp=dashboard_result.get('last_run').get('timestamp'),
            last_run_state=dashboard_result.get('last_run').get('state'),
            updated_timestamp=dashboard_result.get('updated_timestamp'),
            owners=owners,
            tags=tags,
            recent_view_count=dashboard_result['recent_view_count'],
            chart_names=chart_names,
            query_names=query_names,
            queries=queries,
            tables=tables
        )

    @staticmethod
    def _create_dashboard_group_traversal():
        dashboard_group_traversal = __.out('DASHBOARD_OF').project(
            'name',
            'url'
        )
        dashboard_group_traversal = dashboard_group_traversal.by(
            'name',
        )  # name
        dashboard_group_traversal = dashboard_group_traversal.by(
            'name',
        )  # url
        return dashboard_group_traversal

    @staticmethod
    def _create_description_traversal():
        description_traversal = __.out('DESCRIPTION')
        description_traversal = description_traversal.coalesce(
            __.values('description'),
            __.constant('')
        )
        return description_traversal

    @staticmethod
    def _create_last_execution_traversal(key_property_name: str):
        last_execution_traversal = __.out('EXECUTED')
        last_execution_traversal = last_execution_traversal.filter(
            __.has(key_property_name, endingWith('_last_execution'))
        )
        last_execution_traversal = last_execution_traversal.project('timestamp', 'state')
        last_execution_traversal = last_execution_traversal.by('timestamp')
        last_execution_traversal = last_execution_traversal.by('state')
        return last_execution_traversal.next()

    @staticmethod
    def _create_last_successful_execution_traversal(key_property_name: str):
        last_successful_execution_traversal = __.out('EXECUTED')
        last_successful_execution_traversal = last_successful_execution_traversal.filter(
            __.has(key_property_name, endingWith('_last_successful_execution'))
        )
        return last_successful_execution_traversal.values('timestamp')

    @staticmethod
    def _create_owners_traversal(key_property_name: str):
        owners_traversal = __.out('OWNER')
        owners_traversal = owners_traversal.project('id', 'email')
        owners_traversal = owners_traversal.by(key_property_name)
        owners_traversal = owners_traversal.by('email')
        return owners_traversal.fold()

    @staticmethod
    def _create_tags_traversal():
        tags_traversal = __.out('TAGGED_BY')
        return tags_traversal.fold()

    @staticmethod
    def _create_dashboard_query_traversal():
        dash_query_traversal = __.out("HAS_QUERY")
        dash_query_traversal = dash_query_traversal.project(
            'name',
            'url',
            'query_text'
        )
        dash_query_traversal = dash_query_traversal.by('name')
        dash_query_traversal = dash_query_traversal.by('url')
        dash_query_traversal = dash_query_traversal.by('query_text')
        return dash_query_traversal.fold()

    @staticmethod
    def _create_dashboard_chart_traversal():
        dash_query_traversal = __.out("HAS_QUERY").out("HAS_CHART")
        return dash_query_traversal.values('name').fold()

    @staticmethod
    def _create_dashboard_tables_traversal():
        dash_board_tables_traversal = __.out('DASHBOARD_WITH_TABLE').hasLabel('Table')
        dash_board_tables_traversal = dash_board_tables_traversal.project(
            'table_name', 'schema_name', 'cluster_name', 'database_name', 'table_description'
        )
        dash_board_tables_traversal = dash_board_tables_traversal.by(
            'name'
        )  # table_name
        dash_board_tables_traversal = dash_board_tables_traversal.by(
            __.out('TABLE_OF').values('name')
        )  # schema_name
        dash_board_tables_traversal = dash_board_tables_traversal.by(
            __.out('TABLE_OF').out('SCHEMA_OF').values('name')
        )  # cluster_name
        dash_board_tables_traversal = dash_board_tables_traversal.by(
            __.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')
        )  # database_name
        dash_board_tables_traversal = dash_board_tables_traversal.by(
            __.coalesce(
                __.out('DESCRIPTION').values('description'),
                __.constant('')
            )
        )  # table_description
        return dash_board_tables_traversal.fold()


class AbstractGremlinProxy(BaseProxy):
    """
    Gremlin Proxy client for the amundsen metadata
    """
    def __init__(self, *, key_property_name: str, remote_connection: DriverRemoteConnection) -> None:
        # these might vary from datastore type to another, but if you change these while talking to the same instance
        # without migration, it will go poorly
        self.key_property_name: str = key_property_name

        # safe this for use in _submit
        self.remote_connection: DriverRemoteConnection = remote_connection

        self._g: GraphTraversalSource = traversal().withRemote(self.remote_connection)

    def close_driver(self):
        self.remote_connection.close()

    @property
    def g(self) -> GraphTraversalSource:
        """
        might not actually refer to g, but usually is so let's call it that here.
        no setter so we don't accidentally self.g = somewhere
        """
        return self._g

    @classmethod
    def _is_retryable_exception(cls, *, method_name: str, exception: Exception) -> bool:
        """
        overridde this if you want to retry the exception for the given method_name
        """
        return False

    def _submit(self, *, command: str, bindings: Any = None) -> Any:
        """
        Do not use this.

        ...except if you are doing graph management or other things not supported
        by Gremlin.  For example, with JanusGraph, you might:

        >>> self._submit('''
        graph.tx().rollback()
        mgmt = graph.openManagement()
        keyProperty = mgmt.getPropertyKey('_key')
        vertexLabel = mgmt.getVertexLabel('Table')
        mgmt.buildIndex('TableByKeyUnique', Vertex.class).addKey(keyProperty).indexOnly(vertexLabel).unique().buildCompositeIndex()
        mgmt.commit()
        ''')

        >>> self._submit('''
        graph.openManagement().getGraphIndex('TableByKey')
        ''')

        >>> self._submit('''
        graph.openManagement().getGraphIndexes(Vertex.class)
        ''')

        >>> self._submit('''
        graph.openManagement().getGraphIndexes(Edge.class)
        ''')
        """  # noqa: E501
        return self.remote_connection._client.submit(message=command, bindings=bindings).all().result()

    def get_user(self, *, id: str) -> Union[UserEntity, None]:
        result = self.g.V().hasLabel('User').has(self.key_property_name, id).project('id', 'email').\
            by(self.key_property_name).\
            by('email').\
            next()
        user = UserEntity(
            user_id=result.get('id'),
            email=result.get('email')
        )

        return user

    def get_users(self) -> List[UserEntity]:
        users_result = self.g.V().hasLabel('User').project('id', 'email'). \
            by(self.key_property_name). \
            by('email').\
            toList()
        users = []
        for user_result in users_result:
            user = UserEntity(
                user_id=user_result.get('id'),
                email=user_result.get('email')
            )
            users.append(user)
        return users

    def get_table(self, *, table_uri: str) -> Table:
        result = self.g.V().has(self.key_property_name, table_uri). \
            project(
                'database',
                'cluster',
                'schema',
                'schema_description',
                'name',
                'is_view',
                'key',
                'description',
                'columns',
                'tags',
                'owners',
                'water_marks'
            ). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
            by(__.out('TABLE_OF').values('name')). \
            by(__.coalesce(__.out('TABLE_OF').out('DESCRIPTION').values('description'), __.constant(''))). \
            by('name'). \
            by('is_view'). \
            by(self.key_property_name). \
            by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))). \
            by(__.out('COLUMN').project('column_name', 'column_descriptions', 'column_type', 'sort_order').\
               by('name').\
               by(__.out('DESCRIPTION').project('text', 'description_type').\
                    by('description').\
                    by(__.label()).fold()).\
               by('type'). \
               by('sort_order').fold()). \
            by(__.inE('TAG').outV().project('tag_id', 'tag_type').by(self.key_property_name).by(__.values('tag_type')).fold()).\
            by(__.inE('OWNER').outV().values('email').fold()).\
            by(__.out("WATERMARK").project('key', 'partition_key', 'partition_value', 'create_time').\
               by(self.key_property_name).\
               by('partition_key').\
               by('partition_value').\
               by('create_time').fold()
            ).next()

        column_nodes = result['columns']
        tag_nodes = result['tags']
        owner_nodes = result['owners']
        water_mark_nodes = result['water_marks']
        readers = self._get_table_users(table_uri=table_uri)
        columns = []
        for column_node in column_nodes:
            programmatic_descriptions = [description.get('text') for description in column_node.get('column_descriptions') if description.get('description_type') == "Programmatic_Description"]
            default_descriptions = [description.get('text') for description in column_node.get('column_descriptions') if description.get('description_type') == "Description"]
            column_description = ''
            if len(default_descriptions) > 0:
                column_description = default_descriptions[0]
            elif len(programmatic_descriptions) > 0:
                column_description = programmatic_descriptions[0]
            # TODO column stats
            column = Column(
                name=column_node.get('column_name'),
                description=column_description,
                col_type=column_node.get('column_type'),
                sort_order=column_node.get('sort_order')
            )
            columns.append(column)
        tags = []
        for tag_node in tag_nodes:
            tags.append(
                Tag(
                    tag_type=tag_node['tag_type'],
                    tag_name=tag_node['tag_id']
                )
            )

        owners = []
        for owner in owner_nodes:
            owners.append(
                UserEntity(
                    email=owner
                )
            )
        water_marks = []
        for water_mark in water_mark_nodes:
            watermark_type = water_mark['key'].split('/')[-2]
            water_marks.append(
                Watermark(
                    watermark_type=watermark_type,
                    partition_key=water_mark['partition_key'],
                    partition_value=water_mark['partition_value'],
                    create_time=water_mark['create_time']
                )
            )

        table = Table(
            schema=result.get('schema'),
            database=result.get('database'),
            cluster=result.get('cluster'),
            description=result.get('description'),
            table_readers=readers,
            name=result.get('name'),
            columns=columns,
            is_view=result.get('is_view'),
            tags=tags,
            owners=owners,
            watermarks=water_marks
        )
        return table

    def _get_table_users(self, *, table_uri):
        records = self.g.V().has(self.key_property_name, table_uri). \
            out('READ_BY'). \
            project('user_id', 'email', 'read_count'). \
            by(self.key_property_name). \
            by('email'). \
            by(__.coalesce(__.inE('READ_BY').values('read_count'), __.constant(0))). \
            order().by(__.select('read_count'), Order.desc). \
            limit(5).toList()

        readers = []  # type: List[Reader]
        for record in records:
            reader = Reader(user=UserEntity(email=record['email'], user_id=record['user_id']),
                            read_count=record['read_count'])
            readers.append(reader)

        return readers

    def delete_owner(self, *, table_uri: str, owner: str) -> None:
        forward_key = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_vertex_id=owner,
            to_vertex_id=table_uri,
            label="OWNER"
        )
        reverse_key = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_vertex_id=table_uri,
            to_vertex_id=owner,
            label="OWNER_OF"
        )
        self.g.E().or_(
            __.has(self.key_property_name, reverse_key),
            __.has(self.key_property_name, forward_key)
        ).drop().iterate()

    def add_owner(self, *, table_uri: str, owner: str) -> None:
        user = self.get_user(id=owner)
        self.upsert_edge(
            start_node_id=user.user_id,
            end_node_id=table_uri,
            edge_label="OWNER",
            edge_properties={}
        )

    def get_table_description(self, *,
                              table_uri: str) -> Union[str, None]:
        result = self.g.V().has(self.key_property_name, table_uri).out('DESCRIPTION').values('description').next()
        return result

    def put_table_description(self, *,
                              table_uri: str,
                              description: str) -> None:
        self._put_resource_description(
            uri=table_uri,
            description=description
        )

    def _put_resource_description(self, *,
                                  uri: str,
                                  description: str) -> None:

        desc_key = uri + '/_description'
        node_properties = {
            'description': description
        }
        tx = self.g
        tx = self.upsert_node_as_tx(
            tx=tx,
            node_id=desc_key,
            node_label="Description",
            node_properties=node_properties
        )
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=uri,
            end_node_id=desc_key,
            edge_label="DESCRIPTION",
            edge_properties={}
        )
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=desc_key,
            end_node_id=uri,
            edge_label="DESCRIPTION_OF",
            edge_properties={}
        )
        tx.next()

    def add_tag(self, *, id: str, tag: str, tag_type: str, resource_type: ResourceType = ResourceType.Table) -> None:
        # id is the table id.
        node_properties = {
            'tag_type': tag_type
        }
        tx = self.g
        tx = self.upsert_node_as_tx(
            tx=tx,
            node_id=tag,
            node_label="Tag",
            node_properties=node_properties
        )
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=tag,
            end_node_id=id,
            edge_label="TAG",
            edge_properties={}
        )
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=id,
            end_node_id=tag,
            edge_label="TAGGED_BY",
            edge_properties={}
        )
        tx.next()

    def delete_tag(self, *, id: str, tag: str, tag_type: str,
                   resource_type: ResourceType = ResourceType.Table) -> None:

        forward_edge_id = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_vertex_id=tag,
            to_vertex_id=id,
            label="TAG"
        )
        reverse_edge_id = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_vertex_id=tag,
            to_vertex_id=id,
            label="TAGGED_BY"
        )
        tx = self.g
        tx = tx.E().or_(
            __.has(self.key_property_name, forward_edge_id),
            __.has(self.key_property_name, reverse_edge_id)
        ).drop()
        tx.iterate()

    def put_column_description(self, *,
                               table_uri: str,
                               column_name: str,
                               description: str) -> None:
        column_uri = table_uri + '/' + column_name  # type: str
        desc_key = column_uri + '/_description'
        node_properties = {
            'description': description
        }
        tx = self.g
        tx = self.upsert_node_as_tx(
            tx=tx,
            node_id=desc_key,
            node_label="Description",
            node_properties=node_properties
        )
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=column_uri,
            end_node_id=desc_key,
            edge_label="DESCRIPTION",
            edge_properties={}
        )
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=desc_key,
            end_node_id=column_uri,
            edge_label="DESCRIPTION_OF",
            edge_properties={}
        )
        tx.next()

    def get_column_description(self, *,
                               table_uri: str,
                               column_name: str) -> Union[str, None]:
        column_uri = table_uri + '/' + column_name + '/_description'  # type: str
        return self.g.V().has(self.key_property_name, column_uri).values('description').next()

    def get_popular_tables(self, *, num_entries: int) -> List[PopularTable]:
        table_uris = self._get_popular_tables(num_entries)
        if not table_uris:
            return []

        records = self.g.V().has(self.key_property_name, within(table_uris)). \
            project('key', 'table_name', 'schema_name', 'cluster_name', 'database_name', 'table_description'). \
            by(self.key_property_name). \
            by('name'). \
            by(__.out('TABLE_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
            by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))). \
            toList()

        popular_tables = []
        for table_uri in table_uris:
            record = [record for record in records if record['key'] == table_uri][0]
            popular_table = PopularTable(
                database=record['database_name'],
                cluster=record['cluster_name'],
                schema=record['schema_name'],
                name=record['table_name'],
                description=record['table_description']
            )
            popular_tables.append(popular_table)

        return popular_tables

    @_CACHE.cache('_get_popular_tables_uris', _GET_POPULAR_TABLE_CACHE_EXPIRY_SEC)
    def _get_popular_tables(self, num_entries: int):
        results = self.g.V().hasLabel('Table'). \
            where(__.outE('READ_BY').count().is_(gt(0))). \
            project('table_key', 'count', 'score'). \
            by(self.key_property_name).\
            by(__.outE('READ_BY').count()). \
            by(__.project('readers', 'total_reads'). \
               by(__.outE('READ_BY').count()).\
               by(__.coalesce(__.outE('READ_BY').values('read_count'), __.constant(0)).sum()).\
               math('readers * log(total_reads)')). \
            order().by(__.select('score'), Order.desc). \
            limit(num_entries). \
            toList()
        return [result['table_key'] for result in results]

    def get_latest_updated_ts(self) -> Optional[int]:
        """
        API method to fetch last updated / index timestamp for neo4j, es

        :return:
        """
        updated_traversal = self.g.V().has(self.key_property_name, 'amundsen_updated_timestamp')
        updated_traversal = updated_traversal.hasLabel('Updatedtimestamp').values('datetime')
        if updated_traversal.hasNext():
            result = updated_traversal.next()
            if isinstance(result, datetime):
                return int(result.timestamp())
        return None

    def get_tags(self) -> List:
        records = self.g.V().hasLabel('Tag').project('tag_name', 'tag_count').\
            by(self.key_property_name).\
            by(__.outE("TAG").count()).toList()

        results = []
        for record in records:
            results.append(TagDetail(
                tag_name=record['tag_name'],
                tag_count=record['tag_count']
            ))
        return results

    def get_dashboard_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) \
            -> Dict[str, List[DashboardSummary]]:
        # TODO
        pass

    def get_table_by_user_relation(self, *,
                                   user_email: str,
                                   relation_type: UserResourceRel) -> Dict[str, Any]:
        if relation_type == UserResourceRel.follow:
            relation_label = "FOLLOW"
        elif relation_type == UserResourceRel.own:
            relation_label = "OWNER"
        elif relation_type == UserResourceRel.read:
            relation_label = "READ"
        else:
            raise NotFoundException("Relation type {} not found".format(repr(relation_type)))
        table_records = self.g.V().has(self.key_property_name, user_email).outE(relation_label).inV().hasLabel("Table")\
            .project('table_name', 'schema_name', 'cluster_name', 'database_name', 'table_description').\
            by('name'). \
            by(__.out('TABLE_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
            by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))).toList()

        results = []
        for record in table_records:
            results.append(PopularTable(
                database=record['database_name'],
                cluster=record['cluster_name'],
                schema=record['schema_name'],
                name=record['table_name'],
                description=record['table_description']
            ))
        return {ResourceType.Table.name.lower(): results}

    def get_frequently_used_tables(self, *, user_email: str) -> Dict[str, Any]:
        """
        MATCH (user:User {key: $query_key})-[r:READ]->(tbl:Table)
        WHERE EXISTS(r.published_tag) AND r.published_tag IS NOT NULL
        WITH user, r, tbl ORDER BY r.published_tag DESC, r.read_count DESC LIMIT 50
        MATCH (tbl:Table)<-[:TABLE]-(schema:Schema)<-[:SCHEMA]-(clstr:Cluster)<-[:CLUSTER]-(db:Database)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(tbl_dscrpt:Description)
        RETURN db, clstr, schema, tbl, tbl_dscrpt
        :param user_email:
        :return:
        """

        frequent_traversal = self.g.V().has(self.key_property_name, user_email).outE('READ')
        frequent_traversal = frequent_traversal.project(
            'db',
            'cluster',
            'schema',
            'table_name',
            'table_description',
            'read_count'
        )
        frequent_traversal = frequent_traversal.by(
            __.inV().out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')
        )  # db
        frequent_traversal = frequent_traversal.by(
            __.inV().out('TABLE_OF').out('SCHEMA_OF').values('name')
        )  # cluster
        frequent_traversal = frequent_traversal.by(
            __.inV().out('TABLE_OF').values('name')
        )  # schema
        frequent_traversal = frequent_traversal.by(
            __.inV().values('name')
        )  # table_name
        frequent_traversal = frequent_traversal.by(
            __.inV().coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))
        )  # table_description
        frequent_traversal = frequent_traversal.by(
            'read_count'
        )  # read_count
        frequent_traversal = frequent_traversal.order().by(__.select('read_count'), Order.desc)
        frequent_traversal = frequent_traversal.limit(50)
        table_records = frequent_traversal.toList()
        if not table_records:
            raise NotFoundException('User {user_id} does not READ any resources'.format(user_id=user_email))

        results = []
        for record in table_records:
            results.append(PopularTable(
                database=record['db'],
                cluster=record['cluster'],
                schema=record['schema'],
                name=record['table_name'],
                description=record['table_description']
            ))
        return {'table': results}

    def add_resource_relation_by_user(self, *,
                                      id: str,
                                      user_id: str,
                                      relation_type: UserResourceRel,
                                      resource_type: ResourceType) -> None:
        """
        Update table user informations.
        1. Do a upsert of the user node.
        2. Do a upsert of the relation/reverse-relation edge.

        :param table_uri:
        :param user_id:
        :param relation_type:
        :return:
        """
        tx = self.g
        tx = self.upsert_node_as_tx(
            tx=tx,
            node_id=user_id,
            node_label="User",
            node_properties={
                'email': user_id
            }
        )
        if relation_type == UserResourceRel.follow:
            relation_label = "FOLLOW"
            reverse_relation_label = None
        elif relation_type == UserResourceRel.own:
            relation_label = "OWNER"
            reverse_relation_label = "OWNER_OF"
        elif relation_type == UserResourceRel.read:
            relation_label = "READ"
            reverse_relation_label = "READ_BY"
        else:
            raise NotFoundException("Relation type {} not found".format(repr(relation_type)))

        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=user_id,
            end_node_id=id,
            edge_label=relation_label,
            edge_properties={}
        )
        if reverse_relation_label:
            tx = self.upsert_edge_as_tx(
                tx=tx,
                start_node_id=id,
                end_node_id=user_id,
                edge_label=reverse_relation_label,
                edge_properties={}
            )

        tx.next()

    def delete_resource_relation_by_user(self, *,
                                         id: str,
                                         user_id: str,
                                         relation_type: UserResourceRel,
                                         resource_type: ResourceType) -> None:
        if relation_type == UserResourceRel.follow:
            relation_label = "FOLLOW"
            reverse_relation_label = None
        elif relation_type == UserResourceRel.own:
            relation_label = "OWNER"
            reverse_relation_label = "OWNER_OF"
        elif relation_type == UserResourceRel.read:
            relation_label = "READ"
            reverse_relation_label = "READ_BY"
        else:
            raise NotFoundException("Relation type {} not found".format(repr(relation_type)))
        edge_ids = [
            "{from_vertex_id}_{to_vertex_id}_{label}".format(
                from_vertex_id=user_id,
                to_vertex_id=id,
                label=relation_label
            )
        ]
        if reverse_relation_label:
            edge_ids.append("{from_vertex_id}_{to_vertex_id}_{label}".format(
                from_vertex_id=id,
                to_vertex_id=user_id,
                label=reverse_relation_label
            ))

        self.g.E().has(self.key_property_name, within(edge_ids)).drop().iterate()

    def get_dashboard(self,
                      dashboard_uri: str,
                      ) -> DashboardDetailEntity:
        ds_traversal = self.g.V().hasKey(self.key_property_name, dashboard_uri)
        ds_traversal = ds_traversal.project(
            'cluster_name',
            'uri',
            'name',
            'product',
            'created_timestamp',
            'description',
            'group',
            'group_name',
            'group_url',
            'last_successful_run_timestamp',
            'last_run_timestamp',
            'last_run_state',
            'updated_timestamp',
            'owners',
            'tags',
            'recent_view_count',
            'queries',
            'charts',
            'tables'
        )

        dashboard_group_traversal = __.out('DASHBOARD_OF').project(
            'name',
            'url'
        )
        dashboard_group_traversal = dashboard_group_traversal.by(
            'name',
        )  # name
        dashboard_group_traversal = dashboard_group_traversal.by(
            'name',
        )  # url
        ds_traversal = ds_traversal.by(
            __.out('DASHBOARD_OF').out('DASHBOARD_GROUP_OF').values('name')
        )  # cluster name

    def get_dashboard_description(self, *,
                                  id: str) -> Description:
        pass

    def put_dashboard_description(self, *,
                                  id: str,
                                  description: str) -> None:
        pass

    def get_resources_using_table(self, *,
                                  id: str,
                                  resource_type: ResourceType) -> Dict[str, List[DashboardSummary]]:
        return {}

    def upsert_node(self, *,
                    node_id,
                    node_label,
                    node_properties
                    ):
        tx = self.g
        tx = self.upsert_node_as_tx(
            tx=tx,
            node_id=node_id,
            node_label=node_label,
            node_properties=node_properties
        )
        tx.next()

    def upsert_node_as_tx(self, *,
                          tx,
                          node_id,
                          node_label,
                          node_properties
                          ):
        create_traversal = __.addV(node_label).property(self.key_property_name, node_id)
        tx = tx.V().has(self.key_property_name, node_id). \
            fold(). \
            coalesce(__.unfold(), create_traversal)
        for key, value in node_properties.items():
            if value is None:
                continue
            tx = tx.property(Cardinality.single, key, value)

        return tx

    def upsert_edge(self, *,
                    start_node_id,
                    end_node_id,
                    edge_label,
                    edge_properties: Dict[str, Any]):
        tx = self.g
        tx = self.upsert_edge_as_tx(
            tx=tx,
            start_node_id=start_node_id,
            end_node_id=end_node_id,
            edge_label=edge_label,
            edge_properties=edge_properties
        )

        tx.next()

    def upsert_edge_as_tx(self, *,
                          tx,
                          start_node_id,
                          end_node_id,
                          edge_label,
                          edge_properties: Dict[str, Any]):
        edge_id = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_vertex_id=start_node_id,
            to_vertex_id=end_node_id,
            label=edge_label
        )
        create_traversal = __.V().has(self.key_property_name, start_node_id).addE(edge_label).to(__.V().has(self.key_property_name, end_node_id)).property(self.key_property_name, edge_id)
        tx = tx.V().has(self.key_property_name, start_node_id).outE(edge_label).has(self.key_property_name, edge_id). \
            fold(). \
            coalesce(__.unfold(), create_traversal)
        for key, value in edge_properties.items():
            tx = tx.property(key, value)
        return tx



class GenericGremlinProxy(AbstractGremlinProxy):
    """
    A generic Gremlin proxy
    :param host: a websockets URL
    :param port: None (put it in the URL passed in host)
    :param user: (as optional as your server allows) username
    :param password: (as optional as your server allows) password
    :param driver_remote_connection_options: passed to DriverRemoteConnection's constructor.
    """
    def __init__(self, *, host: str, port: Optional[int] = None, user: Optional[str] = None,
                 password: Optional[str] = None, traversal_source: 'str' = 'g', key_property_name: str = 'key',
                 driver_remote_connection_options: Mapping[str, Any] = {}) -> None:
        driver_remote_connection_options = dict(driver_remote_connection_options)
        # as others, we repurpose host a url
        driver_remote_connection_options.update(url=host)
        # port should be part of that url
        if port is not None:
            raise NotImplementedError(f'port is not allowed! port={port}')

        if user is not None:
            driver_remote_connection_options.update(username=user)
        if password is not None:
            driver_remote_connection_options.update(password=password)

        driver_remote_connection_options.update(traversal_source=traversal_source)

        super().__init__(key_property_name=key_property_name,
                         remote_connection=DriverRemoteConnection(**driver_remote_connection_options))


