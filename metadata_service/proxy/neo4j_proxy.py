import logging
import textwrap
from random import randint
from typing import Dict, Any, no_type_check, List, Tuple, Union, Optional  # noqa: F401

import time
from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
from neo4j.v1 import BoltStatementResult
from neo4j.v1 import GraphDatabase, Driver  # noqa: F401

from metadata_service.entity.popular_table import PopularTable
from metadata_service.entity.table_detail import Application, Column, Reader, Source, \
    Statistics, Table, Tag, User, Watermark
from metadata_service.entity.tag_detail import TagDetail
from metadata_service.entity.user_detail import User as UserEntity
from metadata_service.exception import NotFoundException
from metadata_service.proxy.base_proxy import BaseProxy
from metadata_service.proxy.statsd_utilities import timer_with_counter
from metadata_service.util import UserResourceRel

_CACHE = CacheManager(**parse_cache_config_options({'cache.type': 'memory'}))


# Expire cache every 11 hours + jitter
_GET_POPULAR_TABLE_CACHE_EXPIRY_SEC = 11 * 60 * 60 + randint(0, 3600)

LOGGER = logging.getLogger(__name__)


class Neo4jProxy(BaseProxy):
    """
    A proxy to Neo4j (Gateway to Neo4j)
    """

    def __init__(self, *,
                 host: str,
                 port: int,
                 user: str ='neo4j',
                 password: str ='',
                 num_conns: int =50,
                 max_connection_lifetime_sec: int =100) -> None:
        """
        There's currently no request timeout from client side where server
        side can be enforced via "dbms.transaction.timeout"
        By default, it will set max number of connections to 50 and connection time out to 10 seconds.
        :param endpoint: neo4j endpoint
        :param num_conns: number of connections
        :param max_connection_lifetime_sec: max life time the connection can have when it comes to reuse. In other
        words, connection life time longer than this value won't be reused and closed on garbage collection. This
        value needs to be smaller than surrounding network environment's timeout.
        """
        endpoint = f'{host}:{port}'
        self._driver = GraphDatabase.driver(endpoint, max_connection_pool_size=num_conns,
                                            connection_timeout=10,
                                            max_connection_lifetime=max_connection_lifetime_sec,
                                            auth=(user, password))  # type: Driver

    @timer_with_counter
    def get_table(self, *, table_key: str) -> Table:
        """
        :param table_key: Table URI
        :return:  A Table object
        """

        cols, last_neo4j_record = self._exec_col_query(table_key)

        readers = self._exec_usage_query(table_key)

        wmk_results, table_writer, timestamp_value, owners, tags, source = self._exec_table_query(table_key)

        table = Table(database=last_neo4j_record['db']['name'],
                      cluster=last_neo4j_record['clstr']['name'],
                      schema=last_neo4j_record['schema']['name'],
                      name=last_neo4j_record['tbl']['name'],
                      tags=tags,
                      description=self._safe_get(last_neo4j_record, 'tbl_dscrpt', 'description'),
                      columns=cols,
                      owners=owners,
                      table_readers=readers,
                      watermarks=wmk_results,
                      table_writer=table_writer,
                      last_updated_timestamp=timestamp_value,
                      source=source,
                      is_view=self._safe_get(last_neo4j_record, 'tbl', 'is_view'))

        return table

    @timer_with_counter
    def _exec_col_query(self, table_key: str) -> Tuple:
        # Return Value: (Columns, Last Processed Record)

        column_level_query = textwrap.dedent("""
        MATCH (db:Database)<-[:CLUSTER_OF]-(clstr:Cluster)<-[:SCHEMA_OF]-(schema:Schema)
        <-[:TABLE_OF]-(tbl:Table {table_key: $tbl_table_key})-[:COLUMN]->(col:Column)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(tbl_dscrpt:Description)
        OPTIONAL MATCH (col:Column)-[:DESCRIPTION]->(col_dscrpt:Description)
        OPTIONAL MATCH (col:Column)-[:STAT]->(stat:Stat)
        RETURN db, clstr, schema, tbl, tbl_dscrpt, col, col_dscrpt, collect(distinct stat) as col_stats
        ORDER BY col.sort_order;""")

        tbl_col_neo4j_records = self._execute_cypher_query(
            statement=column_level_query, param_dict={'tbl_table_key': table_key})
        cols = []
        for tbl_col_neo4j_record in tbl_col_neo4j_records:
            # Getting last record from this for loop as Neo4j's result's random access is O(n) operation.
            col_stats = []
            for stat in tbl_col_neo4j_record['col_stats']:
                col_stat = Statistics(
                    stat_type=stat['stat_name'],
                    stat_val=stat['stat_val'],
                    start_epoch=int(float(stat['start_epoch'])),
                    end_epoch=int(float(stat['end_epoch']))
                )
                col_stats.append(col_stat)

            last_neo4j_record = tbl_col_neo4j_record
            col = Column(name=tbl_col_neo4j_record['col']['name'],
                         description=self._safe_get(tbl_col_neo4j_record, 'col_dscrpt', 'description'),
                         col_type=tbl_col_neo4j_record['col']['type'],
                         sort_order=int(tbl_col_neo4j_record['col']['sort_order']),
                         stats=col_stats)

            cols.append(col)

        if not cols:
            raise NotFoundException('Table URI( {table_key} ) does not exist'.format(table_key=table_key))

        return (cols, last_neo4j_record)

    @timer_with_counter
    def _exec_usage_query(self, table_key: str) -> List[Reader]:
        # Return Value: List[Reader]

        usage_query = textwrap.dedent("""\
        MATCH (user:User)-[read:READ]->(table:Table {table_key: $tbl_table_key})
        RETURN user.email as email, read.read_count as read_count, table.name as table_name
        ORDER BY read.read_count DESC LIMIT 5;
        """)

        usage_neo4j_records = self._execute_cypher_query(statement=usage_query,
                                                         param_dict={'tbl_table_key': table_key})
        readers = []  # type: List[Reader]
        for usage_neo4j_record in usage_neo4j_records:
            reader = Reader(user=User(email=usage_neo4j_record['email']),
                            read_count=usage_neo4j_record['read_count'])
            readers.append(reader)

        return readers

    @timer_with_counter
    def _exec_table_query(self, table_key: str) -> Tuple:
        """
        Queries one Cypher record with watermark list, Application,
        ,timestamp, owner records and tag records.
        """

        # Return Value: (Watermark Results, Table Writer, Last Updated Timestamp, owner records, tag records)

        table_level_query = textwrap.dedent("""\
        MATCH (tbl:Table {table_key: $tbl_table_key})
        OPTIONAL MATCH (wmk:Watermark)-[:BELONG_TO_TABLE]->(tbl)
        OPTIONAL MATCH (application:Application)-[:GENERATES]->(tbl)
        OPTIONAL MATCH (tbl)-[:LAST_UPDATED_AT]->(t:Timestamp)
        OPTIONAL MATCH (owner:User)-[:OWNER_OF]->(tbl)
        OPTIONAL MATCH (tbl)-[:TAGGED_BY]->(tag:Tag)
        OPTIONAL MATCH (tbl)-[:SOURCE]->(src:Source)
        RETURN collect(distinct wmk) as wmk_records,
        application,
        t.last_updated_timestamp as last_updated_timestamp,
        collect(distinct owner) as owner_records,
        collect(distinct tag) as tag_records,
        src
        """)

        table_records = self._execute_cypher_query(statement=table_level_query,
                                                   param_dict={'tbl_table_key': table_key})

        table_records = table_records.single()

        wmk_results = []
        table_writer = None

        wmk_records = table_records['wmk_records']

        for record in wmk_records:
            if record['table_key'] is not None:
                watermark_type = record['table_key'].split('/')[-2]
                wmk_result = Watermark(watermark_type=watermark_type,
                                       partition_key=record['partition_table_key'],
                                       partition_value=record['partition_value'],
                                       create_time=record['create_time'])
                wmk_results.append(wmk_result)

        tags = []
        if table_records.get('tag_records'):
            tag_records = table_records['tag_records']
            for record in tag_records:
                tag_result = Tag(tag_name=record['table_key'],
                                 tag_type=record['tag_type'])
                tags.append(tag_result)

        application_record = table_records['application']
        if application_record is not None:
            table_writer = Application(
                application_url=application_record['application_url'],
                description=application_record['description'],
                name=application_record['name'],
                id=application_record.get('id', '')
            )

        timestamp_value = table_records['last_updated_timestamp']

        owner_record = []

        for owner in table_records.get('owner_records', []):
            owner_record.append(User(email=owner['email']))

        src = None

        if table_records['src']:
            src = Source(source_type=table_records['src']['source_type'],
                         source=table_records['src']['source'])

        return wmk_results, table_writer, timestamp_value, owner_record, tags, src

    @no_type_check
    def _safe_get(self, dct, *table_keys):
        """
        Helper method for getting value from nested dict. This also works either table_key does not exist or value is None.
        :param dct:
        :param table_keys:
        :return:
        """
        for table_key in table_keys:
            dct = dct.get(table_key)
            if dct is None:
                return None
        return dct

    @timer_with_counter
    def _execute_cypher_query(self, *,
                              statement: str,
                              param_dict: Dict[str, Any]) -> BoltStatementResult:
        if LOGGER.isEnabledFor(logging.DEBUG):
            LOGGER.debug('Executing Cypher query: {statement} with params {params}: '.format(statement=statement,
                                                                                             params=param_dict))
        start = time.time()
        try:
            with self._driver.session() as session:
                return session.run(statement, **param_dict)

        finally:
            # TODO: Add support on statsd
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug('Cypher query execution elapsed for {} seconds'.format(time.time() - start))

    @timer_with_counter
    def get_table_description(self, *,
                              table_key: str) -> Union[str, None]:
        """
        Get the table description based on table uri. Any exception will propagate back to api server.

        :param table_key:
        :return:
        """

        table_description_query = textwrap.dedent("""
        MATCH (tbl:Table {table_key: $tbl_table_key})-[:DESCRIPTION]->(d:Description)
        RETURN d.description AS description;
        """)

        result = self._execute_cypher_query(statement=table_description_query,
                                            param_dict={'tbl_table_key': table_key})

        table_descrpt = result.single()

        table_description = table_descrpt['description'] if table_descrpt else None

        return table_description

    @timer_with_counter
    def put_table_description(self, *,
                              table_key: str,
                              description: str) -> None:
        """
        Update table description with one from user
        :param table_key: Table uri (table_key in Neo4j)
        :param description: new value for table description
        """
        # start neo4j transaction
        desc_table_key = table_key + '/_description'

        upsert_desc_query = textwrap.dedent("""
            MERGE (u:Description {table_key: $desc_table_key})
            on CREATE SET u={description: $description, table_key: $desc_table_key}
            on MATCH SET u={description: $description, table_key: $desc_table_key}
            """)

        upsert_desc_tab_relation_query = textwrap.dedent("""
            MATCH (n1:Description {table_key: $desc_table_key}), (n2:Table {table_key: $tbl_table_key})
            MERGE (n1)-[r1:DESCRIPTION_OF]->(n2)-[r2:DESCRIPTION]->(n1)
            RETURN n1.table_key, n2.table_key
            """)

        start = time.time()

        try:
            tx = self._driver.session().begin_transaction()

            tx.run(upsert_desc_query, {'description': description,
                                       'desc_table_key': desc_table_key})

            result = tx.run(upsert_desc_tab_relation_query, {'desc_table_key': desc_table_key,
                                                             'tbl_table_key': table_key})

            if not result.single():
                raise RuntimeError('Failed to update the table {tbl} description'.format(tbl=table_key))

            # end neo4j transaction
            tx.commit()

        except Exception as e:

            LOGGER.exception('Failed to execute update process')

            if not tx.closed():
                tx.rollback()

            # propagate exception back to api
            raise e

        finally:

            tx.close()

            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug('Update process elapsed for {} seconds'.format(time.time() - start))

    @timer_with_counter
    def get_column_description(self, *,
                               table_key: str,
                               column_name: str) -> Union[str, None]:
        """
        Get the column description based on table uri. Any exception will propagate back to api server.

        :param table_key:
        :param column_name:
        :return:
        """
        column_description_query = textwrap.dedent("""
        MATCH (tbl:Table {table_key: $tbl_table_key})-[:COLUMN]->(c:Column {name: $column_name})-[:DESCRIPTION]->(d:Description)
        RETURN d.description AS description;
        """)

        result = self._execute_cypher_query(statement=column_description_query,
                                            param_dict={'tbl_table_key': table_key, 'column_name': column_name})

        column_descrpt = result.single()

        column_description = column_descrpt['description'] if column_descrpt else None

        return column_description

    @timer_with_counter
    def put_column_description(self, *,
                               table_key: str,
                               column_name: str,
                               description: str) -> None:
        """
        Update column description with input from user
        :param table_key:
        :param column_name:
        :param description:
        :return:
        """

        column_uri = table_key + '/' + column_name  # type: str
        desc_table_key = column_uri + '/_description'

        upsert_desc_query = textwrap.dedent("""
            MERGE (u:Description {table_key: $desc_table_key})
            on CREATE SET u={description: $description, table_key: $desc_table_key}
            on MATCH SET u={description: $description, table_key: $desc_table_key}
            """)

        upsert_desc_col_relation_query = textwrap.dedent("""
            MATCH (n1:Description {table_key: $desc_table_key}), (n2:Column {table_key: $column_table_key})
            MERGE (n1)-[r1:DESCRIPTION_OF]->(n2)-[r2:DESCRIPTION]->(n1)
            RETURN n1.table_key, n2.table_key
            """)

        start = time.time()

        try:
            tx = self._driver.session().begin_transaction()

            tx.run(upsert_desc_query, {'description': description,
                                       'desc_table_key': desc_table_key})

            result = tx.run(upsert_desc_col_relation_query, {'desc_table_key': desc_table_key,
                                                             'column_table_key': column_uri})

            if not result.single():
                raise RuntimeError('Failed to update the table {tbl} '
                                   'column {col} description'.format(tbl=table_key,
                                                                     col=column_uri))

            # end neo4j transaction
            tx.commit()

        except Exception as e:

            LOGGER.exception('Failed to execute update process')

            if not tx.closed():
                tx.rollback()

            # propagate error to api
            raise e

        finally:

            tx.close()

            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug('Update process elapsed for {} seconds'.format(time.time() - start))

    @timer_with_counter
    def add_owner(self, *,
                  table_key: str,
                  owner: str) -> None:
        """
        Update table owner informations.
        1. Do a create if not exists query of the owner(user) node.
        2. Do a upsert of the owner/owned_by relation.

        :param table_key:
        :param owner:
        :return:
        """
        create_owner_query = textwrap.dedent("""
        MERGE (u:User {key: $user_email})
        on CREATE SET u={email: $user_email, key: $user_email}
        """)

        upsert_owner_relation_query = textwrap.dedent("""
        MATCH (n1:User {table_key: $user_email}), (n2:Table {table_key: $tbl_table_key})
        MERGE (n1)-[r1:OWNER_OF]->(n2)-[r2:OWNER]->(n1)
        RETURN n1.table_key, n2.table_key
        """)

        try:
            tx = self._driver.session().begin_transaction()
            # upsert the node
            tx.run(create_owner_query, {'user_email': owner})
            result = tx.run(upsert_owner_relation_query, {'user_email': owner,
                                                          'tbl_table_key': table_key})

            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'owner {owner} and table {tbl}'.format(owner=owner,
                                                                          tbl=table_key))
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e
        finally:
            tx.commit()
            tx.close()

    @timer_with_counter
    def delete_owner(self, *,
                     table_key: str,
                     owner: str) -> None:
        """
        Delete the owner / owned_by relationship.

        :param table_key:
        :param owner:
        :return:
        """
        delete_query = textwrap.dedent("""
        MATCH (n1:User{table_key: $user_email})-[r1:OWNER_OF]->(n2:Table {table_key: $tbl_table_key})-[r2:OWNER]->(n1) DELETE r1,r2
        """)

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {'user_email': owner,
                                  'tbl_table_key': table_key})
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e
        finally:
            tx.commit()
            tx.close()

    @timer_with_counter
    def add_tag(self, *,
                table_key: str,
                tag: str) -> None:
        """
        Add new tag
        1. Create the node with type Tag if the node doesn't exist.
        2. Create the relation between tag and table if the relation doesn't exist.

        :param table_key:
        :param tag:
        :return: None
        """
        LOGGER.info('New tag {} for table_key {}'.format(tag, table_key))

        table_validation_query = 'MATCH (t:Table {table_key: $tbl_table_key}) return t'

        upsert_tag_query = textwrap.dedent("""
        MERGE (u:Tag {table_key: $tag})
        on CREATE SET u={tag_type: $tag_type, table_key: $tag}
        on MATCH SET u={tag_type: $tag_type, table_key: $tag}
        """)

        upsert_tag_relation_query = textwrap.dedent("""
        MATCH (n1:Tag {table_key: $tag}), (n2:Table {table_key: $tbl_table_key})
        MERGE (n1)-[r1:TAG]->(n2)-[r2:TAGGED_BY]->(n1)
        RETURN n1.table_key, n2.table_key
        """)

        try:
            tx = self._driver.session().begin_transaction()
            tbl_result = tx.run(table_validation_query, {'tbl_table_key': table_key})
            if not tbl_result.single():
                raise NotFoundException('table_key {} does not exist'.format(table_key))

            # upsert the node. Currently the type for all the tags is default. We could change it later per UI.
            tx.run(upsert_tag_query, {'tag': tag,
                                      'tag_type': 'default'})
            result = tx.run(upsert_tag_relation_query, {'tag': tag,
                                                        'tbl_table_key': table_key})
            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'tag {tag} and table {tbl}'.format(tag=tag,
                                                                      tbl=table_key))
            tx.commit()
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e
        finally:
            if not tx.closed():
                tx.close()

    @timer_with_counter
    def delete_tag(self, *, table_key: str,
                   tag: str) -> None:
        """
        Deletes tag
        1. Delete the relation between table and the tag
        2. todo(Tao): need to think about whether we should delete the tag if it is an orphan tag.

        :param table_key:
        :param tag:
        :return:
        """

        LOGGER.info('Delete tag {} for table_key {}'.format(tag, table_key))
        delete_query = textwrap.dedent("""
        MATCH (n1:Tag{table_key: $tag})-[r1:TAG]->(n2:Table {table_key: $tbl_table_key})-[r2:TAGGED_BY]->(n1) DELETE r1,r2
        """)

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {'tag': tag,
                                  'tbl_table_key': table_key})
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e
        finally:
            tx.commit()
            tx.close()

    @timer_with_counter
    def get_tags(self) -> List:
        """
        Get all existing tags from neo4j

        :return:
        """
        LOGGER.info('Get all the tags')
        query = textwrap.dedent("""
        MATCH (t:Tag)
        OPTIONAL MATCH (tbl:Table)-[:TAGGED_BY]->(t)
        RETURN t as tag_name, count(distinct tbl.table_key) as tag_count
        """)

        records = self._execute_cypher_query(statement=query,
                                             param_dict={})
        results = []
        for record in records:
            results.append(TagDetail(tag_name=record['tag_name']['table_key'],
                                     tag_count=record['tag_count']))
        return results

    @timer_with_counter
    def get_latest_updated_ts(self) -> Optional[int]:
        """
        API method to fetch last updated / index timestamp for neo4j, es

        :return:
        """
        query = textwrap.dedent("""
        MATCH (n:Updatedtimestamp{table_key: 'amundsen_updated_timestamp'}) RETURN n as ts
        """)
        record = self._execute_cypher_query(statement=query,
                                            param_dict={})
        # None means we don't have record for neo4j, es last updated / index ts
        record = record.single()
        if record:
            return record.get('ts', {}).get('latest_timestmap', 0)
        else:
            return None

    @timer_with_counter
    @_CACHE.cache('_get_popular_tables_uris', _GET_POPULAR_TABLE_CACHE_EXPIRY_SEC)
    def _get_popular_tables_uris(self, num_entries: int) -> List[str]:
        """
        Retrieve popular table uris. Will provide tables with top x popularity score.
        Popularity score = number of distinct readers * log(total number of reads)
        The result of this method will be cached based on the table_key (num_entries), and the cache will be expired based on
        _GET_POPULAR_TABLE_CACHE_EXPIRY_SEC

        For score computation, it uses logarithm on total number of reads so that score won't be affected by small
        number of users reading a lot of times.
        :return: Iterable of table uri
        """
        query = textwrap.dedent("""
        MATCH (tbl:Table)-[r:READ_BY]->(u:User)
        WITH tbl.table_key as table_table_key, count(distinct u) as readers, sum(r.read_count) as total_reads
        WHERE readers > 10
        RETURN table_table_key, readers, total_reads, (readers * log(total_reads)) as score
        ORDER BY score DESC LIMIT $num_entries;
        """)

        LOGGER.info('Querying popular tables URIs')
        records = self._execute_cypher_query(statement=query,
                                             param_dict={'num_entries': num_entries})

        return [record['table_key'] for record in records]

    @timer_with_counter
    def get_popular_tables(self, *, num_entries: int =10) -> List[PopularTable]:
        """
        Retrieve popular tables. As popular table computation requires full scan of table and user relationship,
        it will utilize cached method _get_popular_tables_uris.

        :param num_entries:
        :return: Iterable of PopularTable
        """

        table_keys = self._get_popular_tables_uris(num_entries)
        if not table_keys:
            return []

        query = textwrap.dedent("""
        MATCH (db:Database)<-[:CLUSTER_OF]-(clstr:Cluster)<-[:SCHEMA_OF]-(schema:Schema)<-[:TABLE_OF]-(tbl:Table)
        WHERE tbl.table_key IN $table_keys
        WITH db.name as database_name, clstr.name as cluster_name, schema.name as schema_name, tbl
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(dscrpt:Description)
        RETURN database_name, cluster_name, schema_name, tbl.name as table_name,
        dscrpt.description as table_description;
        """)

        records = self._execute_cypher_query(statement=query,
                                             param_dict={'table_keys': table_keys})

        popular_tables = []
        for record in records:
            popular_table = PopularTable(database=record['database_name'],
                                         cluster=record['cluster_name'],
                                         schema=record['schema_name'],
                                         name=record['table_name'],
                                         key='{0}://{1}.{2}/{3}'.format(record['database_name'], record['cluster_name'],
                                                                        record['schema_name'], record['table_name']),
                                         entity_type='table',
                                         description=self._safe_get(record, 'table_description'))
            popular_tables.append(popular_table)
        return popular_tables

    @timer_with_counter
    def get_user_detail(self, *, user_id: str) -> Union[UserEntity, None]:
        """
        Retrieve user detail based on user_id(email).

        :param user_id: the email for the given user
        :return:
        """

        query = textwrap.dedent("""
        MATCH (user:User {key: $user_id})
        OPTIONAL MATCH (user)-[:MANAGE_BY]->(manager:User)
        RETURN user as user_record, manager as manager_record
        """)

        record = self._execute_cypher_query(statement=query,
                                            param_dict={'user_id': user_id})
        single_result = record.single()

        if not single_result:
            raise NotFoundException('User {user_id} '
                                    'not found in the graph'.format(user_id=user_id))

        record = single_result.get('user_record', {})
        manager_record = single_result.get('manager_record', {})
        if manager_record:
            manager_name = manager_record.get('full_name', '')
        else:
            manager_name = ''
        result = UserEntity(email=record['email'],
                            first_name=record.get('first_name'),
                            last_name=record.get('last_name'),
                            full_name=record.get('full_name'),
                            is_active=record.get('is_active'),
                            github_username=record.get('github_username'),
                            team_name=record.get('team_name'),
                            slack_id=record.get('slack_id'),
                            employee_type=record.get('employee_type'),
                            manager_fullname=manager_name)
        return result

    @staticmethod
    def _get_relation_by_type(relation_type: UserResourceRel) -> Tuple:

        if relation_type == UserResourceRel.follow:
            relation, reverse_relation = 'FOLLOW', 'FOLLOWED_BY'
        elif relation_type == UserResourceRel.own:
            relation, reverse_relation = 'OWNER_OF', 'OWNER'
        elif relation_type == UserResourceRel.read:
            relation, reverse_relation = 'READ', 'READ_BY'
        else:
            raise NotImplementedError('The relation type {} is not defined!'.format(relation_type))
        return relation, reverse_relation

    @timer_with_counter
    def get_table_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) -> Dict[str, Any]:
        """
        Retrive all follow the resources per user based on the relation.
        We start with table resources only, then add dashboard.

        :param user_email: the email of the user
        :param relation_type: the relation between the user and the resource
        :return:
        """
        relation, _ = self._get_relation_by_type(relation_type)
        # relationship can't be parameterized
        query_table_key = 'table_key: "{user_id}"'.format(user_id=user_email)

        query = textwrap.dedent("""
        MATCH (user:User {{{table_key}}})-[:{relation}]->(tbl:Table)
        RETURN COLLECT(DISTINCT tbl) as table_records
        """).format(table_key=query_table_key,
                    relation=relation)

        record = self._execute_cypher_query(statement=query,
                                            param_dict={})

        if not record:
            raise NotFoundException('User {user_id} does not {relation} '
                                    'any resources'.format(user_id=user_email,
                                                           relation=relation))
        results = []
        table_records = record.single().get('table_records', [])

        for record in table_records:
            _, last_neo4j_record = self._exec_col_query(record['key'])
            results.append(PopularTable(
                database=last_neo4j_record['db']['name'],
                cluster=last_neo4j_record['clstr']['name'],
                schema=last_neo4j_record['schema']['name'],
                name=last_neo4j_record['tbl']['name'],
                key='{0}://{1}.{2}/{3}'.format(last_neo4j_record['db']['name'],
                                               last_neo4j_record['clstr']['name'],
                                               last_neo4j_record['schema']['name'],
                                               last_neo4j_record['tbl']['name']),
                entity_type='table',
                description=self._safe_get(last_neo4j_record, 'tbl_dscrpt', 'description')))
        return {'table': results}

    @timer_with_counter
    def add_table_relation_by_user(self, *,
                                   table_key: str,
                                   user_email: str,
                                   relation_type: UserResourceRel) -> None:
        """
        Update table user informations.
        1. Do a upsert of the user node.
        2. Do a upsert of the relation/reverse-relation edge.

        :param table_key:
        :param user_email:
        :param relation_type:
        :return:
        """
        relation, reverse_relation = self._get_relation_by_type(relation_type)

        upsert_user_query = textwrap.dedent("""
        MERGE (u:User {key: $user_email})
        on CREATE SET u={email: $user_email, key: $user_email}
        on MATCH SET u={email: $user_email, key: $user_email}
        """)

        user_email = 'table_key: "{user_email}"'.format(user_email=user_email)
        tbl_table_key = 'table_key: "{tbl_table_key}"'.format(tbl_table_key=table_key)

        upsert_user_relation_query = textwrap.dedent("""
        MATCH (n1:User {{{user_email}}}), (n2:Table {{{tbl_table_key}}})
        MERGE (n1)-[r1:{relation}]->(n2)-[r2:{reverse_relation}]->(n1)
        RETURN n1.table_key, n2.table_key
        """).format(user_email=user_email,
                    tbl_table_key=tbl_table_key,
                    relation=relation,
                    reverse_relation=reverse_relation)

        try:
            tx = self._driver.session().begin_transaction()
            # upsert the node
            tx.run(upsert_user_query, {'user_email': user_email})
            result = tx.run(upsert_user_relation_query, {})

            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'user {user} and table {tbl}'.format(user=user_email,
                                                                        tbl=table_key))
            tx.commit()
        except Exception as e:
            if not tx.closed():
                tx.rollback()
            # propagate the exception back to api
            raise e
        finally:
            tx.close()

    @timer_with_counter
    def delete_table_relation_by_user(self, *,
                                      table_key: str,
                                      user_email: str,
                                      relation_type: UserResourceRel) -> None:
        """
        Delete the relationship between user and resources.

        :param table_key:
        :param user_email:
        :param relation_type:
        :return:
        """
        relation, reverse_relation = self._get_relation_by_type(relation_type)

        user_email = 'table_key: "{user_email}"'.format(user_email=user_email)
        tbl_table_key = 'table_key: "{tbl_table_key}"'.format(tbl_table_key=table_key)

        delete_query = textwrap.dedent("""
        MATCH (n1:User {{{user_email}}})-[r1:{relation}]->
        (n2:Table {{{tbl_table_key}}})-[r2:{reverse_relation}]->(n1) DELETE r1,r2
        """).format(user_email=user_email,
                    tbl_table_key=tbl_table_key,
                    relation=relation,
                    reverse_relation=reverse_relation)

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {})
            tx.commit()
        except Exception as e:
            # propagate the exception back to api
            if not tx.closed():
                tx.rollback()
            raise e
        finally:
            tx.close()
