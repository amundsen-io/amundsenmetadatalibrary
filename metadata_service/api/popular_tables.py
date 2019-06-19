from http import HTTPStatus
from typing import Iterable, Union, Mapping

from flask_restful import Resource, fields, marshal

from metadata_service.proxy import get_proxy_client

popular_table_fields = {
    'cluster': fields.String,
    'database': fields.String,
    'description': fields.String,  # Optional
    'key': fields.String,
    'name': fields.String,
    'schema_name': fields.String,
    'type': fields.String,
    'last_updated_epoch': fields.String,  # Optional
}

popular_tables_fields = {
    'popular_tables': fields.List(fields.Nested(popular_table_fields))
}


class PopularTablesAPI(Resource):
    """
    PopularTables API
    """
    def __init__(self) -> None:
        self.client = get_proxy_client()

    def get(self) -> Iterable[Union[Mapping, int, None]]:
        popular_tables = self.client.get_popular_tables()
        return marshal({'popular_tables': popular_tables}, popular_tables_fields), HTTPStatus.OK
