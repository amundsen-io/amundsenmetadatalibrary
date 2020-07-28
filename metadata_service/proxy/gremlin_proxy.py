import json
import logging
from random import randint
from typing import Any, Dict, List, Mapping, Optional, Union

import gremlin_python
from gremlin_python.process.traversal import T, Order, gt
from gremlin_python.process.graph_traversal import __
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import Table, Column, Reader
from amundsen_common.models.user import User as UserEntity
from amundsen_common.models.dashboard import DashboardSummary
from gremlin_python.driver.driver_remote_connection import \
    DriverRemoteConnection
from gremlin_python.process.anonymous_traversal import traversal
from gremlin_python.process.graph_traversal import GraphTraversalSource
from metadata_service.exception import NotFoundException

from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options

from metadata_service.entity.dashboard_detail import DashboardDetail as DashboardDetailEntity
from metadata_service.entity.description import Description
from metadata_service.entity.resource_type import ResourceType
from metadata_service.proxy import BaseProxy
from metadata_service.util import UserResourceRel

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
        user_result = self.g.V().hasLabel('User').has('email', id).valueMap(True).fold().next()
        user = UserEntity(
            id=user_result[T.id],
            email=user_result['email'][0]
        )
        return user

    def get_users(self) -> List[UserEntity]:
        users_result = self.g.V().hasLabel('User').valueMap(True).toList()
        users = []
        for user_result in users_result:
            user = UserEntity(
                id=user_result[T.id],
                email=user_result['email'][0]
            )
            users.append(user)
        return users

    def get_table(self, *, table_uri: str) -> Table:
        table_query = self.g.V().hasId(table_uri).as_('table')
        table_query = table_query.union(__.select('table').valueMap(True),
                                        __.select('table').out().valueMap(True),
                                        __.select('table').out().hasLabel('Schema').out().hasLabel('Cluster').valueMap(True),
                                        __.select('table').out().hasLabel('Schema').out().hasLabel('Cluster').out().hasLabel('Database').valueMap(True))
        table_results = table_query.fold().next()
        database_node = [table_result for table_result in table_results if table_result[T.label] == 'Database'][0]
        schema_node = [table_result for table_result in table_results if table_result[T.label] == 'Schema'][0]
        cluster_node = [table_result for table_result in table_results if table_result[T.label] == 'Cluster'][0]
        table_node = [table_result for table_result in table_results if table_result[T.label] == 'Table'][0]
        column_nodes = [table_result for table_result in table_results if table_result[T.label] == 'Column']
        columns = []
        readers = self._get_table_users(table_uri=table_uri)
        for column_node in column_nodes:
            # TODO column descriptions and column stats
            column = Column(
                name=column_node['name'][0],
                description='',
                col_type=column_node['type'][0],
                sort_order=column_node['sort_order'][0]
            )
            columns.append(column)
        table = Table(
            schema=schema_node['name'][0],
            database=database_node['name'][0],
            cluster=cluster_node['name'][0],
            table_readers=readers,
            name=table_node['name'][0],
            columns=columns,
            is_view=table_node['is_view'][0]
        )
        return table

    def _get_table_users(self, *, table_uri):
        records = self.g.V(table_uri). \
            out('READ_BY'). \
            project('email', 'read_count'). \
            by('email'). \
            by(__.coalesce(__.inE('READ_BY').values('read_count'), __.constant(0))). \
            order().by(__.select('read_count'), Order.desc). \
            limit(5).toList()

        readers = []  # type: List[Reader]
        for record in records:
            reader = Reader(user=UserEntity(email=record['email']),
                            read_count=record['read_count'])
            readers.append(reader)

        return readers


    def delete_owner(self, *, table_uri: str, owner: str) -> None:
        forward_key = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_from_vertex_id=owner,
            to_vertex_id=table_uri,
            label="OWNER"
        )
        self.g.E().hasId(forward_key).drop()

    def add_owner(self, *, table_uri: str, owner: str) -> None:
        user = self.get_user(owner)
        if user is None:
            self.g.addV(T.id, owner, T.label, "User").property('email', owner).property('is_active', True)
        forward_key = "{from_vertex_id}_{to_vertex_id}_{label}".format(
            from_vertex_id=owner,
            to_vertex_id=table_uri,
            label="OWNER"
        )
        self.g.addE(T.id, forward_key, T.label, "OWNER")

    def get_table_description(self, *,
                              table_uri: str) -> Union[str, None]:
        result = self.g.V().hasId(table_uri).value('description').next()
        return result

    def put_table_description(self, *,
                              table_uri: str,
                              description: str) -> None:
        pass

    def add_tag(self, *, id: str, tag: str, tag_type: str,
                resource_type: ResourceType = ResourceType.Table) -> None:
        pass

    def delete_tag(self, *, id: str, tag: str, tag_type: str,
                   resource_type: ResourceType = ResourceType.Table) -> None:
        pass

    def put_column_description(self, *,
                               table_uri: str,
                               column_name: str,
                               description: str) -> None:
        pass

    def get_column_description(self, *,
                               table_uri: str,
                               column_name: str) -> Union[str, None]:
        pass

    def get_popular_tables(self, *, num_entries: int) -> List[PopularTable]:
        table_uris = self._get_popular_tables(num_entries)
        if not table_uris:
            return []

        records = self.g.V(table_uris). \
            project('table_name', 'schema_name', 'cluster_name', 'database_name', 'table_description'). \
            by('name'). \
            by(__.out('TABLE_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
            by(__.coalesce(__.out('DESCRIPTION_OF').values('description'), __.constant(''))). \
            toList()

        popular_tables = []
        for record in records:
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
            project('table_key', 'score'). \
            by(T.id).by(__.outE('READ_BY').count()). \
            by(__.project('readers', 'total_reads').\
               by(__.outE('READ_BY').count()).\
               by(__.coalesce(__.outE('READ_BY').values('read_count'), __.constant(0)).sum()).\
               math('readers * log(total_reads)')). \
            order().by(__.select('score'), Order.desc). \
            limit(num_entries). \
            toList()
        return [result['table_key'] for result in results]

    def get_latest_updated_ts(self) -> int:
        pass

    def get_tags(self) -> List:
        pass

    def get_dashboard_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) \
            -> Dict[str, List[DashboardSummary]]:
        pass

    def get_table_by_user_relation(self, *, user_email: str,
                                   relation_type: UserResourceRel) -> Dict[str, Any]:
        raise NotFoundException('User {user_id} does not {relation} any resources'.format(user_id=user_email,
                                                                                          relation=relation_type))

    def get_frequently_used_tables(self, *, user_email: str) -> Dict[str, Any]:
        pass

    def add_resource_relation_by_user(self, *,
                                      id: str,
                                      user_id: str,
                                      relation_type: UserResourceRel,
                                      resource_type: ResourceType) -> None:
        pass

    def delete_resource_relation_by_user(self, *,
                                         id: str,
                                         user_id: str,
                                         relation_type: UserResourceRel,
                                         resource_type: ResourceType) -> None:
        pass

    def get_dashboard(self,
                      dashboard_uri: str,
                      ) -> DashboardDetailEntity:
        pass

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
