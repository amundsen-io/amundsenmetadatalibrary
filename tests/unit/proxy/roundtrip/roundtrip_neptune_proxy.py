# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0import json


import logging
from typing import List
from amundsen_common.models.table import Table, Application
from amundsen_common.models.user import User
from amundsen_gremlin.neptune_bulk_loader.gremlin_model_converter import (
    GetGraph
)
from overrides import overrides
from .roundtrip_gremlin_proxy import RoundtripGremlinProxy
from metadata_service.proxy.neptune_proxy import NeptuneGremlinProxy

LOGGER = logging.getLogger(__name__)


class RoundtripNeptuneGremlinProxy(NeptuneGremlinProxy, RoundtripGremlinProxy):
    @overrides
    def post_users(self, *, data: List[User]) -> None:
        """
        Post - process to all users.

        Args:
            self: (todo): write your description
            data: (str): write your description
        """
        entities = GetGraph.user_entities(user_data=data, g=self.neptune_graph_traversal_source_factory())
        self.neptune_bulk_loader_api.bulk_load_entities(entities=entities)

    @overrides
    def put_user(self, *, data: User) -> None:
        """
        Https : py : class.

        Args:
            self: (todo): write your description
            data: (str): write your description
        """
        self.post_users(data=[data])

    @overrides
    def put_app(self, *, data: Application) -> None:
        """
        Write a new application to the app.

        Args:
            self: (todo): write your description
            data: (todo): write your description
        """
        self.post_apps(data=[data])

    @overrides
    def post_apps(self, *, data: List[Application]) -> None:
        """
        Post - process all of the sources.

        Args:
            self: (todo): write your description
            data: (array): write your description
        """
        entities = GetGraph.app_entities(app_data=data, g=self.neptune_graph_traversal_source_factory())
        self.neptune_bulk_loader_api.bulk_load_entities(entities=entities)

    @overrides
    def put_table(self, *, table: Table) -> None:
        """
        Sends a table.

        Args:
            self: (todo): write your description
            table: (str): write your description
        """
        self.post_tables(tables=[table])

    @overrides
    def post_tables(self, *, tables: List[Table]) -> None:
        """
        Post - tables / tables.

        Args:
            self: (todo): write your description
            tables: (list): write your description
        """
        entities = GetGraph.table_entities(table_data=tables, g=self.neptune_graph_traversal_source_factory())
        self.neptune_bulk_loader_api.bulk_load_entities(entities=entities)
