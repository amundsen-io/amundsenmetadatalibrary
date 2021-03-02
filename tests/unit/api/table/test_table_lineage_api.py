# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import unittest
from http import HTTPStatus
from unittest.mock import Mock, patch

from amundsen_common.models.table import Badge
from amundsen_common.models.lineage import Lineage, LineageItem
from tests.unit.test_basics import BasicTestCase
from metadata_service.exception import NotFoundException

import logging

LOGGER = logging.getLogger(__name__)

TABLE_URI = "db://cluster.schema/test_table_1"
API_RESPONSE = {
    "key": "db://cluster.schema/test_table_1",
    "direction": "both",
    "depth": 1,
    "upstream_entities": [
    {
        "level": 1,
        "badges": [
            
        ],
        "source": "db",
        "usage": 257,
        "key": "db://cluster.schema/up_table_1"
    },
    {
        "level": 1,
        "badges": [
            
        ],
        "source": "hive",
        "usage": 164,
        "key": "hive://cluster.schema/up_table_2"
    },
    {
        "level": 1,
        "badges": [
            
        ],
        "source": "hive",
        "usage": 94,
        "key": "hive://cluster.schema/up_table_3"
    },
  ],
    "downstream_entities": [
    {
        "level": 1,
        "badges": [

        ],
        "source": "db",
        "usage": 567,
        "key": "db://cluster.schema/down_table_1"
    },
    {
        "level": 1,
        "badges": [

        ],
        "source": "hive",
        "usage": 54,
        "key": "hive://cluster.schema/down_table_2"
    },
    {
        "level": 2,
        "badges": [
        ],
        "source": "hive",
        "usage": 17,
        "key": "hive://cluster.schema/down_table_3"
    },
    
  ]
}
UPSTREAM = [
    LineageItem(key='db://cluster.schema/up_table_1',
                level=1,
                source='db',
                badges=[],
                usage=257),
    LineageItem(key='hive://cluster.schema/up_table_2',
                level=1,
                source='hive',
                badges=[],
                usage=164),
    LineageItem(key='hive://cluster.schema/up_table_3',
                level=1,
                source='hive',
                badges=[],
                usage=94),
    ]

DOWNSTREAM = [
    LineageItem(key='db://cluster.schema/down_table_1',
                level=1,
                source='db',
                badges=[],
                usage=567),
    LineageItem(key='hive://cluster.schema/down_table_2',
                level=1,
                source='hive',
                badges=[],
                usage=54),
    LineageItem(key='hive://cluster.schema/down_table_3',
                level=2,
                source='hive',
                badges=[],
                usage=17),
    ]

LINEAGE_RESPONSE = Lineage(
    key='db://cluster.schema/test_table_1',
    direction='both',
    depth=0,
    upstream_entities=UPSTREAM,
    downstream_entities=DOWNSTREAM
)

class TestTableLineageAPI(BasicTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.mock_client = patch('metadata_service.api.column.get_proxy_client')
        self.mock_proxy = self.mock_client.start().return_value = Mock()

    def tearDown(self) -> None:
        super().tearDown()

        self.mock_client.stop()

    def test_should_return_response(self) -> None:
        self.mock_proxy.get_lineage.return_value = LINEAGE_RESPONSE
        response = self.app.test_client().get(f'/table/{TABLE_URI}/lineage')
        self.assertEqual(response.json, API_RESPONSE)
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.get_lineage.assert_called_with(table_uri=TABLE_URI)

    def test_should_fail_when_table_doesnt_exist(self) -> None:

        self.mock_proxy.get_lineage.side_effect = NotFoundException(message='table not found')

        response = self.app.test_client().get(f'/table/{TABLE_URI}/lineage')

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
