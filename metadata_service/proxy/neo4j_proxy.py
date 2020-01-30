import logging
import textwrap
import time
from enum import Enum
from random import randint
from typing import (Any, Dict, Generator, List, Optional, Tuple, Union,  # noqa: F401
                    no_type_check)

from amundsen_common.models.table import (Application, Column, Reader, Source,
                                          Statistics, Table, Tag, User,
                                          Watermark, ProgrammaticDescription)
from amundsen_common.models.user import User as UserEntity
from beaker.cache import CacheManager
from beaker.util import parse_cache_config_options
from neo4j.v1 import (WRITE_ACCESS, BoltStatementResult, Driver, GraphDatabase,
                      Session, StatementResult, Transaction)  # noqa: F401

from metadata_service.entity.popular_table import PopularTable
from metadata_service.entity.tag_detail import TagDetail
from metadata_service.exception import NotFoundException
from metadata_service.proxy.base_proxy import BaseProxy
from metadata_service.proxy.statsd_utilities import timer_with_counter
from metadata_service.util import UserResourceRel

_CACHE = CacheManager(**parse_cache_config_options({'cache.type': 'memory'}))

# Expire cache every 11 hours + jitter
_GET_POPULAR_TABLE_CACHE_EXPIRY_SEC = 11 * 60 * 60 + randint(0, 3600)

LOGGER = logging.getLogger(__name__)
PUBLISH_TAG_TIME_FORMAT: str = "%Y-%m-%d %H:%M"
QUERY_TIMEOUT_LIMIT: int = 5


def batch(*, iterable: Any, n: int = 1) -> Generator:
    """
    Helper method for running batched code. It can be used like:

    for batched_subsection_of_data in batch(iterable=data, n=100):
        do_something(with=batched_subsection_of_data)

    Read more about this implementation here:
    https://stackoverflow.com/questions/8290397/how-to-split-an-iterable-in-constant-size-chunks

    :param iterable:
    :param n:
    :return:
    """
    length = len(iterable)
    for ndx in range(0, length, n):
        yield iterable[ndx:min(ndx + n, length)]


class Direction(Enum):
    """
    Helper Enum class for node to node relationship direction (i.e. from node one
    to node two -->, or from node two to node one <--)
    """
    ONE_TO_TWO = 1
    TWO_TO_ONE = 2


class Query:
    def __init__(self, *, statement: str, params: Optional[Dict[str, Any]] = None) -> None:
        self.statement: str = statement
        self.params: Dict[str, Any] = params or {}


class LocalTransaction:
    """
    LocalTransaction is a nice wrapper to make it easier to remember proper commit and rollback semantics when
    performing neo4j queries, by wrapping it in a context. To understand better the pattern here,
    see http://effbot.org/zone/python-with-statement.htm and
    https://docs.python.org/3/reference/datamodel.html#object.__exit__

    This also encourages us to avoid situations where we tangle in unnecessary python code into a neo4j execution
    context.

    """

    def __init__(self, *, driver: Driver, commit_every_run: bool = True) -> None:
        """

        :param driver: GraphDriver i.e. Neo4J driver
        :param commit_every_run: true if you want to commit after every local query, false if commit only at the end
        of the context
        """
        self.commit_every_run: bool = commit_every_run
        # TODO possible slowdowns exist from reusing sessions after many many queries
        # see https://gist.github.com/friendtocephalopods/14c5b59f3ace8772ca2d502c928bb641#file-stresstest-py-L37
        self.session: Session = driver.session(access_mode=WRITE_ACCESS)

    def __enter__(self) -> 'LocalTransaction':
        self.start: float = time.time()
        self.tx: Transaction = self.session.begin_transaction()
        self.query: Query = Query(statement='')
        return self

    # exc_val and traceback are required arguments in this method, even if we never use them
    def __exit__(self, exc_type: Any, exc_val: Any, traceback: Any) -> bool:
        if not self.commit_every_run and exc_type is None:
            try:
                self.tx.commit()
            except Exception as e:
                exc_type = e
        if exc_val is not None:
            self.rollback(message=f'Encountered {exc_val.__class__.__name__} while executing {self.query.statement}')

        if not self.tx.closed():
            self.tx.close()
        if not self.session.closed():
            self.session.close()
        total = time.time() - self.start
        LOGGER.debug(f'Update process elapsed for {total} seconds')
        if total > QUERY_TIMEOUT_LIMIT:
            LOGGER.warning(f'Transaction took {total} > {QUERY_TIMEOUT_LIMIT} seconds '
                           f'to execute; most recent query is {self.query.statement}')
        # If there was an exception, returning anything BUT true should bubble it up
        return exc_type is None or exc_type is Warning

    def rollback(self, *, message: str) -> None:
        LOGGER.exception(message)
        if not self.tx.closed():
            self.tx.rollback()

    def run(self, *queries: Query) -> List[List[Any]]:
        if len(queries) == 0:
            raise RuntimeWarning('zero queries specified; is this intentional?')
        # We create a list of lists of records that map 1-1 to the queries being sent in to execute
        records_list: List[List[Any]] = []
        for self.query in queries:
            result: StatementResult = self.tx.run(self.query.statement, self.query.params)
            records: List[Any] = result.data()
            records_list.append(records)
        if self.commit_every_run:
            self.tx.commit()
        return records_list

    @staticmethod
    def _get_node_as_dict(*, node: Any, node_type: Optional[str] = None, key: Optional[str] = None,
                          exclude: Optional[List[str]] = None) -> Tuple[str, Dict[str, Any]]:
        if type(node) is dict and node_type is None and not hasattr(node, '__dict__'):
            raise RuntimeError('Unable to determine node type to upsert')

        if node_type is None:
            node_type = node.__class__.__name__

        params: Dict[str, Any] = node.__dict__ if type(node) is not dict else node
        if 'key' not in params and key is None:
            raise RuntimeError(f'No key specified in node type {node_type}')
        elif key is not None:
            if key not in params:
                raise RuntimeError(f'Key {key} is missing in node type {node_type}')
            params['key'] = params[key]

        if exclude is not None:
            params = {element: params[element] for element in params if element not in exclude}
        params['published_tag'] = time.strftime(PUBLISH_TAG_TIME_FORMAT)

        return node_type, params

    def upsert(self, *, node: Any, node_type: Optional[str] = None, key: Optional[str] = None,
               exclude: Optional[List[str]] = None) -> None:
        node_type, params = self._get_node_as_dict(node=node, node_type=node_type, key=key, exclude=exclude)

        param_str = f'{{{", ".join([f"{param}: ${param}" for param in params.keys()])}}}'

        statement: str = f'''MERGE (n:{node_type} {{key: $key}})
                            on CREATE SET n={param_str}
                            on MATCH SET n={param_str}
                            '''

        self.run(Query(statement=statement, params=params))

    def _get_direction_str(self, *, direction: Direction, relation_name: str, params: Dict[str, Any] = {}) -> str:
        param_str = ", ".join([f"{pair[0]}: '{pair[1]}'" for pair in params.items()])
        if direction == Direction.ONE_TO_TWO:
            return f'-[rel:{relation_name} {{{param_str}}}]->'
        else:
            return f'<-[rel:{relation_name} {{{param_str}}}]-'

    def link(self, *,
             node1: Any = None,
             node_type1: Optional[str] = None,
             key1: Optional[str] = None,
             node_query1: Optional[str] = None,
             node2: Any = None,
             node_type2: Optional[str] = None,
             key2: Optional[str] = None,
             node_query2: Optional[str] = None,
             params: Optional[Dict[str, str]] = None,
             relation_name: str,
             direction: Direction = Direction.ONE_TO_TWO) -> None:
        if node_query1 is None:
            params1: Optional[Dict[str, Any]]
            node_type1, params1 = self._get_node_as_dict(node=node1, node_type=node_type1, key=key1)
            node_query1 = f'(n1:{node_type1} {{key: $key1}})'
        else:
            params1 = None
        if node_query2 is None:
            params2: Optional[Dict[str, Any]]
            node_type2, params2 = self._get_node_as_dict(node=node2, node_type=node_type2, key=key2)
            node_query2 = f'(n2:{node_type2} {{key: $key2}})'
        else:
            params2 = None

        if params is None:
            params = {}
        if params1 is not None:
            params['key1'] = params1['key']
        if params2 is not None:
            params['key2'] = params2['key']

        direction_str = self._get_direction_str(direction=direction,
                                                relation_name=relation_name,
                                                params={})
        link_statement: str = f'''MATCH {node_query1}
            MATCH {node_query2}
            MERGE (n1){direction_str}(n2)
            RETURN n1.key, n2.key'''
        results = self.run(Query(statement=link_statement, params=params))

        # if no results were returned, then there was no match, which means the nodes don't exist
        check = results[0]
        if check is None or len(check) == 0:
            message: str = f'Unable to create link {link_statement}'
            # we raise a NotFoundException which should bubble up
            raise NotFoundException(message)

    def upsert_batch(self, *, nodes: List[Any], node_type: Optional[str] = None, key: Optional[str] = None,
                     exclude: Optional[List[str]] = None) -> None:
        if len(nodes) == 0:
            raise RuntimeWarning('zero nodes specified; is this intentional?')
        tuples = [self._get_node_as_dict(node=node, node_type=node_type, key=key, exclude=exclude) for node in nodes]
        node_types = [t[0] for t in tuples]
        node_type = node_types[0]
        for possible_node_type in node_types:
            if possible_node_type != node_type:
                raise RuntimeError(f'mismatch in node type! {node_type} != {possible_node_type}; either set it '
                                   f'explicitly or make all nodes the same class')
        params = [t[1] for t in tuples]
        param_str = f'{{{", ".join([f"{param}: row.{param}" for param in params[0].keys()])}}}'

        statement: str = f'''UNWIND $batch as row
            MERGE (n:{node_type} {{ key: row.key }})
            on CREATE SET n={param_str}
            on MATCH SET n={param_str}
            RETURN n.key'''

        self.run(Query(statement=statement, params={'batch': params}))


class Neo4jProxy(BaseProxy):
    """
    A proxy to Neo4j (Gateway to Neo4j)
    """

    def __init__(self, *,
                 host: str,
                 port: int,
                 user: str = 'neo4j',
                 password: str = '',
                 num_conns: int = 50,
                 max_connection_lifetime_sec: int = 100) -> None:
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
    def get_table(self, *, table_uri: str) -> Table:
        """
        :param table_uri: Table URI
        :return:  A Table object
        """

        cols, last_neo4j_record = self._exec_col_query(table_uri)

        readers = self._exec_usage_query(table_uri)

        wmk_results, table_writer, timestamp_value, owners, tags, source, badges, prog_descs = \
            self._exec_table_query(table_uri)

        table = Table(database=last_neo4j_record['db']['name'],
                      cluster=last_neo4j_record['clstr']['name'],
                      schema=last_neo4j_record['schema']['name'],
                      name=last_neo4j_record['tbl']['name'],
                      tags=tags,
                      badges=badges,
                      description=self._safe_get(last_neo4j_record, 'tbl_dscrpt', 'description'),
                      columns=cols,
                      owners=owners,
                      table_readers=readers,
                      watermarks=wmk_results,
                      table_writer=table_writer,
                      last_updated_timestamp=timestamp_value,
                      source=source,
                      is_view=self._safe_get(last_neo4j_record, 'tbl', 'is_view'),
                      programmatic_descriptions=prog_descs
                      )

        return table

    @timer_with_counter
    def _exec_col_query(self, table_uri: str) -> Tuple:
        # Return Value: (Columns, Last Processed Record)

        column_level_query = textwrap.dedent("""
        MATCH (db:Database)-[:CLUSTER]->(clstr:Cluster)-[:SCHEMA]->(schema:Schema)
        -[:TABLE]->(tbl:Table {key: $tbl_key})-[:COLUMN]->(col:Column)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(tbl_dscrpt:Description)
        OPTIONAL MATCH (col:Column)-[:DESCRIPTION]->(col_dscrpt:Description)
        OPTIONAL MATCH (col:Column)-[:STAT]->(stat:Stat)
        RETURN db, clstr, schema, tbl, tbl_dscrpt, col, col_dscrpt, collect(distinct stat) as col_stats
        ORDER BY col.sort_order;""")

        tbl_col_neo4j_records = self._execute_cypher_query(
            statement=column_level_query, param_dict={'tbl_key': table_uri})
        cols = []
        last_neo4j_record = None
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
            raise NotFoundException('Table URI( {table_uri} ) does not exist'.format(table_uri=table_uri))

        return sorted(cols, key=lambda item: item.sort_order), last_neo4j_record

    @timer_with_counter
    def _exec_usage_query(self, table_uri: str) -> List[Reader]:
        # Return Value: List[Reader]

        usage_query = textwrap.dedent("""\
        MATCH (user:User)-[read:READ]->(table:Table {key: $tbl_key})
        RETURN user.email as email, read.read_count as read_count, table.name as table_name
        ORDER BY read.read_count DESC LIMIT 5;
        """)

        usage_neo4j_records = self._execute_cypher_query(statement=usage_query,
                                                         param_dict={'tbl_key': table_uri})
        readers = []  # type: List[Reader]
        for usage_neo4j_record in usage_neo4j_records:
            reader = Reader(user=User(email=usage_neo4j_record['email']),
                            read_count=usage_neo4j_record['read_count'])
            readers.append(reader)

        return readers

    @timer_with_counter
    def _exec_table_query(self, table_uri: str) -> Tuple:
        """
        Queries one Cypher record with watermark list, Application,
        ,timestamp, owner records and tag records.
        """

        # Return Value: (Watermark Results, Table Writer, Last Updated Timestamp, owner records, tag records)

        table_level_query = textwrap.dedent("""\
        MATCH (tbl:Table {key: $tbl_key})
        OPTIONAL MATCH (wmk:Watermark)-[:BELONG_TO_TABLE]->(tbl)
        OPTIONAL MATCH (application:Application)-[:GENERATES]->(tbl)
        OPTIONAL MATCH (tbl)-[:LAST_UPDATED_AT]->(t:Timestamp)
        OPTIONAL MATCH (owner:User)<-[:OWNER]-(tbl)
        OPTIONAL MATCH (tbl)-[:TAGGED_BY]->(tag:Tag{tag_type: $tag_normal_type})
        OPTIONAL MATCH (tbl)-[:TAGGED_BY]->(badge:Tag{tag_type: $tag_badge_type})
        OPTIONAL MATCH (tbl)-[:SOURCE]->(src:Source)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(prog_descriptions:Programmatic_Description)
        RETURN collect(distinct wmk) as wmk_records,
        application,
        t.last_updated_timestamp as last_updated_timestamp,
        collect(distinct owner) as owner_records,
        collect(distinct tag) as tag_records,
        collect(distinct badge) as badge_records,
        src,
        collect(distinct prog_descriptions) as prog_descriptions
        """)

        table_records = self._execute_cypher_query(statement=table_level_query,
                                                   param_dict={'tbl_key': table_uri,
                                                               'tag_normal_type': 'default',
                                                               'tag_badge_type': 'badge'})

        table_records = table_records.single()

        wmk_results = []
        table_writer = None

        wmk_records = table_records['wmk_records']

        for record in wmk_records:
            if record['key'] is not None:
                watermark_type = record['key'].split('/')[-2]
                wmk_result = Watermark(watermark_type=watermark_type,
                                       partition_key=record['partition_key'],
                                       partition_value=record['partition_value'],
                                       create_time=record['create_time'])
                wmk_results.append(wmk_result)

        tags = []
        if table_records.get('tag_records'):
            tag_records = table_records['tag_records']
            for record in tag_records:
                tag_result = Tag(tag_name=record['key'],
                                 tag_type=record['tag_type'])
                tags.append(tag_result)

        badges = []
        if table_records.get('badge_records'):
            badge_records = table_records['badge_records']
            for record in badge_records:
                badge_result = Tag(tag_name=record['key'],
                                   tag_type=record['tag_type'])
                badges.append(badge_result)

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

        prog_descriptions = self._extract_programmatic_descriptions_from_query(
            table_records.get('prog_descriptions', [])
        )

        return wmk_results, table_writer, timestamp_value, owner_record, tags, src, badges, prog_descriptions

    def _extract_programmatic_descriptions_from_query(self, raw_prog_descriptions: dict) -> list:
        prog_descriptions = []
        for prog_description in raw_prog_descriptions:
            source = prog_description['description_source']
            if source is None:
                LOGGER.error("A programmatic description with no source was found... skipping.")
            else:
                prog_descriptions.append(ProgrammaticDescription(source=source, text=prog_description['description']))
        prog_descriptions.sort(key=lambda x: x.source)
        return prog_descriptions

    @no_type_check
    def _safe_get(self, dct, *keys):
        """
        Helper method for getting value from nested dict. This also works either key does not exist or value is None.
        :param dct:
        :param keys:
        :return:
        """
        for key in keys:
            dct = dct.get(key)
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
                              table_uri: str) -> Union[str, None]:
        """
        Get the table description based on table uri. Any exception will propagate back to api server.

        :param table_uri:
        :return:
        """

        table_description_query = textwrap.dedent("""
        MATCH (tbl:Table {key: $tbl_key})-[:DESCRIPTION]->(d:Description)
        RETURN d.description AS description;
        """)

        result = self._execute_cypher_query(statement=table_description_query,
                                            param_dict={'tbl_key': table_uri})

        table_descrpt = result.single()

        table_description = table_descrpt['description'] if table_descrpt else None

        return table_description

    @timer_with_counter
    def put_table_description(self, *,
                              table_uri: str,
                              description: str) -> None:
        """
        Update table description with one from user
        :param table_uri: Table uri (key in Neo4j)
        :param description: new value for table description
        """
        # start neo4j transaction
        desc_key = f'{table_uri}/_description'

        with LocalTransaction(driver=self._driver, commit_every_run=False) as ltx:
            ltx.upsert(node={'description': description, 'key': desc_key},
                       node_type='Description')
            ltx.link(node1={'key': desc_key}, node_type1='Description',
                     node2={'key': table_uri}, node_type2='Table', direction=Direction.TWO_TO_ONE,
                     relation_name='DESCRIPTION')

    @timer_with_counter
    def get_column_description(self, *,
                               table_uri: str,
                               column_name: str) -> Union[str, None]:
        """
        Get the column description based on table uri. Any exception will propagate back to api server.

        :param table_uri:
        :param column_name:
        :return:
        """
        column_description_query = textwrap.dedent("""
        MATCH (tbl:Table {key: $tbl_key})-[:COLUMN]->(c:Column {name: $column_name})-[:DESCRIPTION]->(d:Description)
        RETURN d.description AS description;
        """)

        result = self._execute_cypher_query(statement=column_description_query,
                                            param_dict={'tbl_key': table_uri, 'column_name': column_name})

        column_descrpt = result.single()
        column_description = column_descrpt['description'] if column_descrpt else None
        return column_description

    @timer_with_counter
    def put_column_description(self, *,
                               table_uri: str,
                               column_name: str,
                               description: str) -> None:
        """
        Update column description with input from user
        :param table_uri:
        :param column_name:
        :param description:
        :return:
        """

        column_uri = table_uri + '/' + column_name  # type: str
        desc_key = column_uri + '/_description'

        with LocalTransaction(driver=self._driver, commit_every_run=False) as ltx:
            ltx.upsert(node={'description': description, 'key': desc_key},
                       node_type='Description')
            ltx.link(node1={'key': column_uri}, node_type1='Column',
                     node2={'key': desc_key}, node_type2='Description', direction=Direction.ONE_TO_TWO,
                     relation_name='DESCRIPTION')

    @timer_with_counter
    def add_owner(self, *,
                  table_uri: str,
                  owner: str) -> None:
        """
        Update table owner informations.
        1. Do a create if not exists query of the owner(user) node.
        2. Do a upsert of the owner/owned_by relation.

        :param table_uri:
        :param owner:
        :return:
        """

        with LocalTransaction(driver=self._driver, commit_every_run=False) as ltx:
            ltx.link(node1={'key': owner}, node_type1='User',
                     node2={'key': table_uri}, node_type2='Table', relation_name='OWNER',
                     direction=Direction.TWO_TO_ONE)

    @timer_with_counter
    def delete_owner(self, *,
                     table_uri: str,
                     owner: str) -> None:
        """
        Delete the owner / owned_by relationship.

        :param table_uri:
        :param owner:
        :return:
        """
        delete_query = textwrap.dedent("""
        MATCH (n1:User{key: $user_email})<-[r1:OWNER]-(n2:Table {key: $tbl_key}) DELETE r1
        """)

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {'user_email': owner,
                                  'tbl_key': table_uri})
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
                table_uri: str,
                tag: str,
                tag_type: str = 'default') -> None:
        """
        Add new tag
        1. Create the node with type Tag if the node doesn't exist.
        2. Create the relation between tag and table if the relation doesn't exist.

        :param table_uri:
        :param tag:
        :param tag_type
        :return: None
        """
        LOGGER.info('New tag {} for table_uri {} with type {}'.format(tag, table_uri, tag_type))

        with LocalTransaction(driver=self._driver, commit_every_run=False) as ltx:
            ltx.upsert(node={'tag_type': 'default',
                             'key': tag},
                       node_type='Tag')
            ltx.link(node1={'key': tag}, node_type1='Tag',
                     node2={'key': table_uri}, node_type2='Table', relation_name='TAG',
                     direction=Direction.ONE_TO_TWO)

    @timer_with_counter
    def delete_tag(self, *, table_uri: str,
                   tag: str,
                   tag_type: str = 'default') -> None:
        """
        Deletes tag
        1. Delete the relation between table and the tag
        2. todo(Tao): need to think about whether we should delete the tag if it is an orphan tag.

        :param table_uri:
        :param tag:
        :param tag_type: {default-> normal tag, badge->non writable tag from UI}
        :return:
        """

        LOGGER.info('Delete tag {} for table_uri {} with type {}'.format(tag, table_uri, tag_type))
        delete_query = textwrap.dedent("""
        MATCH (n1:Tag{key: $tag, tag_type: $tag_type})-
        [r1:TAG]->(n2:Table {key: $tbl_key})-[r2:TAGGED_BY]->(n1) DELETE r1,r2
        """)

        try:
            tx = self._driver.session().begin_transaction()
            tx.run(delete_query, {'tag': tag,
                                  'tbl_key': table_uri,
                                  'tag_type': tag_type})
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
        # todo: Currently all the tags are default type, we could open it up if we want to include badge
        query = textwrap.dedent("""
        MATCH (t:Tag{tag_type: 'default'})
        OPTIONAL MATCH (tbl:Table)-[:TAGGED_BY]->(t)
        RETURN t as tag_name, count(distinct tbl.key) as tag_count
        """)

        records = self._execute_cypher_query(statement=query,
                                             param_dict={})
        results = []
        for record in records:
            results.append(TagDetail(tag_name=record['tag_name']['key'],
                                     tag_count=record['tag_count']))
        return results

    @timer_with_counter
    def get_latest_updated_ts(self) -> Optional[int]:
        """
        API method to fetch last updated / index timestamp for neo4j, es

        :return:
        """
        query = textwrap.dedent("""
        MATCH (n:Updatedtimestamp{key: 'amundsen_updated_timestamp'}) RETURN n as ts
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
        The result of this method will be cached based on the key (num_entries), and the cache will be expired based on
        _GET_POPULAR_TABLE_CACHE_EXPIRY_SEC

        For score computation, it uses logarithm on total number of reads so that score won't be affected by small
        number of users reading a lot of times.
        :return: Iterable of table uri
        """
        query = textwrap.dedent("""
        MATCH (tbl:Table)-[r:READ_BY]->(u:User)
        WITH tbl.key as table_key, count(distinct u) as readers, sum(r.read_count) as total_reads
        WHERE readers > 10
        RETURN table_key, readers, total_reads, (readers * log(total_reads)) as score
        ORDER BY score DESC LIMIT $num_entries;
        """)

        LOGGER.info('Querying popular tables URIs')
        records = self._execute_cypher_query(statement=query,
                                             param_dict={'num_entries': num_entries})

        return [record['table_key'] for record in records]

    @timer_with_counter
    def get_popular_tables(self, *, num_entries: int) -> List[PopularTable]:
        """
        Retrieve popular tables. As popular table computation requires full scan of table and user relationship,
        it will utilize cached method _get_popular_tables_uris.

        :param num_entries:
        :return: Iterable of PopularTable
        """

        table_uris = self._get_popular_tables_uris(num_entries)
        if not table_uris:
            return []

        query = textwrap.dedent("""
        MATCH (db:Database)-[:CLUSTER]->(clstr:Cluster)-[:SCHEMA]->(schema:Schema)-[:TABLE]->(tbl:Table)
        WHERE tbl.key IN $table_uris
        WITH db.name as database_name, clstr.name as cluster_name, schema.name as schema_name, tbl
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(dscrpt:Description)
        RETURN database_name, cluster_name, schema_name, tbl.name as table_name,
        dscrpt.description as table_description;
        """)

        records = self._execute_cypher_query(statement=query,
                                             param_dict={'table_uris': table_uris})

        popular_tables = []
        for record in records:
            popular_table = PopularTable(database=record['database_name'],
                                         cluster=record['cluster_name'],
                                         schema=record['schema_name'],
                                         name=record['table_name'],
                                         description=self._safe_get(record, 'table_description'))
            popular_tables.append(popular_table)
        return popular_tables

    @timer_with_counter
    def get_user(self, *, id: str) -> Union[UserEntity, None]:
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
                                            param_dict={'user_id': id})
        single_result = record.single()

        if not single_result:
            raise NotFoundException('User {user_id} '
                                    'not found in the graph'.format(user_id=id))

        record = single_result.get('user_record', {})
        manager_record = single_result.get('manager_record', {})
        if manager_record:
            manager_name = manager_record.get('full_name', '')
        else:
            manager_name = ''

        return self._build_user_from_record(record=record, manager_name=manager_name)

    def get_users(self) -> List[UserEntity]:
        statement = "MATCH (usr:User) WHERE usr.is_active = true RETURN collect(usr) as users"

        record = self._execute_cypher_query(statement=statement, param_dict={})
        result = record.single()
        if not result or not result.get('users'):
            raise NotFoundException('Error getting users')

        return [self._build_user_from_record(record=rec) for rec in result['users']]

    @staticmethod
    def _build_user_from_record(record: dict, manager_name: str = '') -> UserEntity:
        return UserEntity(email=record['email'],
                          first_name=record.get('first_name'),
                          last_name=record.get('last_name'),
                          full_name=record.get('full_name'),
                          is_active=record.get('is_active', False),
                          github_username=record.get('github_username'),
                          team_name=record.get('team_name'),
                          slack_id=record.get('slack_id'),
                          employee_type=record.get('employee_type'),
                          manager_fullname=manager_name)

    @timer_with_counter
    def put_user(self, *, data: UserEntity) -> None:
        """
        Update user with supplied data.
        :param data: new user to be added
        """
        with LocalTransaction(driver=self._driver) as ltx:
            ltx.upsert(node=data, key='email')

    @timer_with_counter
    def post_users(self, *, data: List[UserEntity]) -> None:
        """
        Add or update users with supplied data.
        :param data: users to be added or updated
        """
        for batched_users in batch(iterable=data, n=100):
            with LocalTransaction(driver=self._driver) as ltx:
                ltx.upsert_batch(nodes=batched_users, key='email')

    @staticmethod
    def _get_user_table_relationship_clause(relation_type: UserResourceRel, tbl_key: str = None,
                                            user_key: str = None) -> str:
        """
        Returns the relationship clause of a cypher query between users and tables
        The User node is 'usr', the table node is 'tbl', and the relationship is 'rel'
        e.g. (usr:User)-[rel:READ]->(tbl:Table), (usr)-[rel:READ]->(tbl)
        """
        tbl_matcher: str = ''
        user_matcher: str = ''

        if tbl_key is not None:
            tbl_matcher += ':Table'
            if tbl_key != '':
                tbl_matcher += f' {{key: "{tbl_key}"}}'

        if user_key is not None:
            user_matcher += ':User'
            if user_key != '':
                user_matcher += f' {{key: "{user_key}"}}'

        if relation_type == UserResourceRel.follow:
            relation = f'(usr{user_matcher})-[rel:FOLLOW]->(tbl{tbl_matcher})'
        elif relation_type == UserResourceRel.own:
            relation = f'(usr{user_matcher})<-[rel:OWNER]-(tbl{tbl_matcher})'
        elif relation_type == UserResourceRel.read:
            relation = f'(usr{user_matcher})-[rel:READ]->(tbl{tbl_matcher})'
        else:
            raise NotImplementedError(f'The relation type {relation_type} is not defined!')
        return relation

    @timer_with_counter
    def get_table_by_user_relation(self, *, user_email: str, relation_type: UserResourceRel) -> Dict[str, Any]:
        """
        Retrive all follow the resources per user based on the relation.
        We start with table resources only, then add dashboard.

        :param user_email: the email of the user
        :param relation_type: the relation between the user and the resource
        :return:
        """
        rel_clause: str = self._get_user_table_relationship_clause(relation_type=relation_type,
                                                                   tbl_key='',
                                                                   user_key=user_email)
        query = textwrap.dedent(f"""
        MATCH {rel_clause}<-[:TABLE]-(schema:Schema)<-[:SCHEMA]-(clstr:Cluster)<-[:CLUSTER]-(db:Database)
        WITH db, clstr, schema, tbl
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(tbl_dscrpt:Description)
        RETURN db, clstr, schema, tbl, tbl_dscrpt""")

        table_records = self._execute_cypher_query(statement=query, param_dict={'query_key': user_email})

        if not table_records:
            raise NotFoundException('User {user_id} does not {relation} any resources'.format(user_id=user_email,
                                                                                              relation=relation_type))
        results = []
        for record in table_records:
            results.append(PopularTable(
                database=record['db']['name'],
                cluster=record['clstr']['name'],
                schema=record['schema']['name'],
                name=record['tbl']['name'],
                description=self._safe_get(record, 'tbl_dscrpt', 'description')))
        return {'table': results}

    @timer_with_counter
    def get_frequently_used_tables(self, *, user_email: str) -> Dict[str, Any]:
        """
        Retrieves all Table the resources per user on READ relation.

        :param user_email: the email of the user
        :return:
        """

        query = textwrap.dedent("""
        MATCH (user:User {key: $query_key})-[r:READ]->(tbl:Table)
        WHERE EXISTS(r.published_tag) AND r.published_tag IS NOT NULL
        WITH user, r, tbl ORDER BY r.published_tag DESC, r.read_count DESC LIMIT 50
        MATCH (tbl:Table)<-[:TABLE]-(schema:Schema)<-[:SCHEMA]-(clstr:Cluster)<-[:CLUSTER]-(db:Database)
        OPTIONAL MATCH (tbl)-[:DESCRIPTION]->(tbl_dscrpt:Description)
        RETURN db, clstr, schema, tbl, tbl_dscrpt
        """)

        table_records = self._execute_cypher_query(statement=query, param_dict={'query_key': user_email})

        if not table_records:
            raise NotFoundException('User {user_id} does not READ any resources'.format(user_id=user_email))
        results = []

        for record in table_records:
            results.append(PopularTable(
                database=record['db']['name'],
                cluster=record['clstr']['name'],
                schema=record['schema']['name'],
                name=record['tbl']['name'],
                description=self._safe_get(record, 'tbl_dscrpt', 'description')))
        return {'table': results}

    @timer_with_counter
    def add_table_relation_by_user(self, *,
                                   table_uri: str,
                                   user_email: str,
                                   relation_type: UserResourceRel) -> None:
        """
        Update table user informations.
        1. Do a upsert of the user node.
        2. Do a upsert of the relation/reverse-relation edge.

        :param table_uri:
        :param user_email:
        :param relation_type:
        :return:
        """

        upsert_user_query = textwrap.dedent("""
        MERGE (u:User {key: $user_email})
        on CREATE SET u={email: $user_email, key: $user_email}
        """)

        user_email_clause = f'key: "{user_email}"'
        tbl_key = f'key: "{table_uri}"'

        rel_clause: str = self._get_user_table_relationship_clause(relation_type=relation_type)
        upsert_user_relation_query = textwrap.dedent(f"""
        MATCH (usr:User {{{user_email_clause}}}), (tbl:Table {{{tbl_key}}})
        MERGE {rel_clause}
        RETURN usr.key, tbl.key
        """)

        try:
            tx = self._driver.session().begin_transaction()
            # upsert the node
            tx.run(upsert_user_query, {'user_email': user_email})
            result = tx.run(upsert_user_relation_query, {})

            if not result.single():
                raise RuntimeError('Failed to create relation between '
                                   'user {user} and table {tbl}'.format(user=user_email,
                                                                        tbl=table_uri))
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
                                      table_uri: str,
                                      user_email: str,
                                      relation_type: UserResourceRel) -> None:
        """
        Delete the relationship between user and resources.

        :param table_uri:
        :param user_email:
        :param relation_type:
        :return:
        """
        rel_clause: str = self._get_user_table_relationship_clause(relation_type=relation_type,
                                                                   user_key=user_email,
                                                                   tbl_key=table_uri)

        delete_query = textwrap.dedent(f"""
        MATCH {rel_clause}
        DELETE rel
        """)

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
