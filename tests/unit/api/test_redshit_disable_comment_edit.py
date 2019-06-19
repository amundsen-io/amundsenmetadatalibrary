import unittest
from http import HTTPStatus

from mock import patch
from metadata_service.api.table import TableDescriptionAPI
from metadata_service.api.column import ColumnDescriptionAPI


class RedshiftCommentEditDisableTest(unittest.TestCase):
    def test_table_comment_edit(self) -> None:
        with patch('metadata_service.api.table.get_proxy_client'):
            tbl_dscrpt_api = TableDescriptionAPI()

            key = '8cfc0513-9a6b-4cdb-a4cc-f6549ed087cf'
            response = tbl_dscrpt_api.put(key=key, description_val='test')
            self.assertEqual(list(response)[1], HTTPStatus.OK)

    def test_column_comment_edit(self) -> None:
        with patch('metadata_service.api.column.get_proxy_client'):
            col_dscrpt_api = ColumnDescriptionAPI()

            key = '5717362e-19a2-4bac-be49-0e4c5851300e'
            response = col_dscrpt_api.put(key=key, column_name='foo', description_val='test')
            self.assertEqual(list(response)[1], HTTPStatus.OK)


if __name__ == '__main__':
    unittest.main()
