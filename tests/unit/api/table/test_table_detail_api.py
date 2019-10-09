from http import HTTPStatus

from metadata_service.exception import NotFoundException
from tests.unit.api.table.table_test_case import TableTestCase

TABLE_URI = 'wizards'

QUERY_RESPONSE = {
    'database': 'postgres',
    'cluster': 'postgres',
    'schema': 'hogwarts',
    'name': 'wizards',
    'description': 'all wizards at hogwarts',
    'tags': [{
        'tag_type': 'table',
        'tag_name': 'wizards'
    }],
    'table_readers': [{
        'user': {
            'email': 'ssnape@hogwarts.com',
            'first_name': 'severus',
            'last_name': 'snape'
        },
        'read_count': 10
    }],
    'owners': [{
        'email': 'mmcgonagall@hogwarts.com',
        'first_name': 'minerva',
        'last_name': 'mcgonagall'
    }],
    'columns': [{
        'name': 'wizard_name',
        'description': 'full name of wizard',
        'col_type': 'String',
        'sort_order': 0,
        'stats': [{
            'stat_type': 'requests',
            'stat_val': '10',
            'start_epoch': 1570581861,
            'end_epoch': 1570581861

        }]
    }],
    'watermarks': [{
        'watermark_type': 'type',
        'partition_key': 'key',
        'partition_value': 'value',
        'create_time': '1570581861'
    }],
    'table_writer': [{
        'application_url': 'table_writer_rul',
        'name': 'table_writer_name',
        'id': 'table_writer_id',
        'description': 'table_writer_description'
    }],
    'last_updated_timestamp': 1570581861,
    'source': {
        'source_type': 'type',
        'source': 'source'
    },
    'is_view': True
}

API_RESPONSE = {
    'database': 'postgres',
    'cluster': 'postgres',
    'schema': 'hogwarts',
    'table_name': 'wizards',
    'table_description': 'all wizards at hogwarts',
    'tags': [{
        'tag_type': 'table',
        'tag_name': 'wizards'
    }],
    'table_readers': [{
        'reader': {
            'email': 'ssnape@hogwarts.com',
            'first_name': 'severus',
            'last_name': 'snape'
        },
        'read_count': 10
    }],
    'owners': [{
        'email': 'mmcgonagall@hogwarts.com',
        'first_name': 'minerva',
        'last_name': 'mcgonagall'
    }],
    'columns': [{
        'name': 'wizard_name',
        'description': 'full name of wizard',
        'type': 'String',
        'sort_order': 0,
        'stats': [{
            'stat_type': 'requests',
            'stat_val': '10',
            'start_epoch': 1570581861,
            'end_epoch': 1570581861

        }]
    }],
    'watermarks': [{
        'watermark_type': 'type',
        'partition_key': 'key',
        'partition_value': 'value',
        'create_time': '1570581861'
    }],
    'table_writer': [{
        'application_url': 'table_writer_rul',
        'name': 'table_writer_name',
        'id': 'table_writer_id',
        'description': 'table_writer_description'
    }],
    'last_updated_timestamp': 1570581861,
    'source': {
        'source_type': 'type',
        'source': 'source'
    },
    'is_view': True
}


class TestTableDetailAPI(TableTestCase):

    def test_should_get_column_details(self) -> None:
        self.mock_proxy.get_table.return_value = QUERY_RESPONSE

        response = self.app.test_client().get(f'/table/{TABLE_URI}')

        self.assertEqual(response.json, API_RESPONSE)
        self.assertEqual(response.status_code, HTTPStatus.OK)
        self.mock_proxy.get_table.assert_called_with(table_uri=TABLE_URI)

    def test_should_fail_to_get_column_details_when_table_not_foubd(self) -> None:
        self.mock_proxy.get_table.side_effect = NotFoundException(message='table not found')

        response = self.app.test_client().get(f'/table/{TABLE_URI}')

        self.assertEqual(response.status_code, HTTPStatus.NOT_FOUND)
