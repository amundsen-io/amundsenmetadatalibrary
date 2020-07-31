import json
import logging
from random import randint
from typing import Any, Dict, List, Mapping, Optional, Union

import gremlin_python
from gremlin_python.process.traversal import T, Order, gt, Cardinality
from gremlin_python.process.graph_traversal import __
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.table import Table, Column, Reader, Tag
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
        result = self.g.V(id).project('id', 'email').\
            by(__.id()).\
            by('email').\
            next()
        user = UserEntity(
            user_id=result.get('id'),
            email=result.get('email')
        )

        return user

    def get_users(self) -> List[UserEntity]:
        users_result = self.g.V().hasLabel('User').project('id', 'email'). \
            by(__.id()). \
            by('email').\
            toList()
        users = []
        for user_result in users_result:
            user = UserEntity(
                id=user_result.get('id'),
                email=user_result.get('email')
            )
            users.append(user)
        return users

    def get_table(self, *, table_uri: str) -> Table:
        result = self.g.V().hasId(table_uri). \
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
                'owners'
            ). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').out('CLUSTER_OF').values('name')). \
            by(__.out('TABLE_OF').out('SCHEMA_OF').values('name')). \
            by(__.out('TABLE_OF').values('name')). \
            by(__.coalesce(__.out('TABLE_OF').out('DESCRIPTION').values('description'), __.constant(''))). \
            by('name'). \
            by('is_view'). \
            by(T.id). \
            by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))). \
            by(__.out('COLUMN').project('column_name', 'column_description', 'column_type', 'sort_order').\
               by('name').\
               by(__.coalesce(__.out('DESCRIPTION').values('description'), __.constant(''))).\
               by('type'). \
               by('sort_order').fold()). \
            by(__.inE('TAG').outV().project('tag_id', 'tag_type').by(__.id()).by(__.values('tag_type')).fold()).\
            by(__.inE('OWNER').outV().values('email').fold()).\
            next()

        column_nodes = result['columns']
        tag_nodes = result['tags']
        owner_nodes = result['owners']
        readers = self._get_table_users(table_uri=table_uri)
        columns = []
        for column_node in column_nodes:
            # TODO column stats
            column = Column(
                name=column_node.get('column_name'),
                description=column_node.get('column_description'),
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
            owners=owners
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
            from_vertex_id=owner,
            to_vertex_id=table_uri,
            label="OWNER"
        )
        self.g.E().hasId(forward_key).drop().iterate()

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
        result = self.g.V(table_uri).out('DESCRIPTION').values('description').next()
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
        tx = tx.E([ forward_edge_id,reverse_edge_id]).drop()
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
        tx.next()

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
        records = self.g.V().hasLabel('Tag').project('tag_name', 'tag_count').\
            by(__.id()).\
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

    def upsert_node(self, *,
                    node_id,
                    node_label,
                    node_properties
                    ):
        tx = self.g
        tx = self.upsert_node_as_tx(
            tx,
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
        create_traversal = __.addV(node_label).property(T.id, node_id)
        tx = tx.V().hasId(node_id). \
            fold(). \
            coalesce(__.unfold(), create_traversal)
        for key, value in node_properties.items():
            if not value:
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
        create_traversal = __.V(start_node_id).addE(edge_label).to(__.V(end_node_id)).property(T.id, edge_id)
        tx = tx.V(start_node_id).outE(edge_label).hasId(edge_id). \
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



