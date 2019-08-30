import json
from flask import current_app
from http import HTTPStatus
from typing import Any, Iterable, Mapping, Union

from amundsen_common.models.table import TableSchema
from flasgger import swag_from
from flask import current_app as app
from flask import request
from flask_restful import Resource, reqparse

from metadata_service.exception import NotFoundException
from metadata_service.proxy import get_proxy_client
from metadata_service.proxy.search_proxy import SearchProxy


class TableDetailAPI(Resource):
    """
    TableDetail API
    """

    def __init__(self) -> None:
        self.client = get_proxy_client()

    @swag_from('swagger_doc/table/detail_get.yml')
    def get(self, table_uri: str) -> Iterable[Union[Mapping, int, None]]:
        try:
            table = self.client.get_table(table_uri=table_uri)
            schema = TableSchema(strict=True)
            return schema.dump(table).data, HTTPStatus.OK

        except NotFoundException:
            return {'message': 'table_uri {} does not exist'.format(table_uri)}, HTTPStatus.NOT_FOUND


class TableOwnerAPI(Resource):
    """
    TableOwner API to add / delete owner info
    """

    def __init__(self) -> None:
        self.client = get_proxy_client()

    @swag_from('swagger_doc/table/owner_put.yml')
    def put(self, table_uri: str, owner: str) -> Iterable[Union[Mapping, int, None]]:
        try:
            self.client.add_owner(table_uri=table_uri, owner=owner)
            return {'message': 'The owner {} for table_uri {} '
                               'is added successfully'.format(owner,
                                                              table_uri)}, HTTPStatus.OK
        except Exception:
            return {'message': 'The owner {} for table_uri {} '
                               'is not added successfully'.format(owner,
                                                                  table_uri)}, HTTPStatus.INTERNAL_SERVER_ERROR

    @swag_from('swagger_doc/table/owner_delete.yml')
    def delete(self, table_uri: str, owner: str) -> Iterable[Union[Mapping, int, None]]:
        try:
            self.client.delete_owner(table_uri=table_uri, owner=owner)
            return {'message': 'The owner {} for table_uri {} '
                               'is deleted successfully'.format(owner,
                                                                table_uri)}, HTTPStatus.OK
        except Exception:
            return {'message': 'The owner {} for table_uri {} '
                               'is not deleted successfully'.format(owner,
                                                                    table_uri)}, HTTPStatus.INTERNAL_SERVER_ERROR


class TableDescriptionAPI(Resource):
    """
    TableDescriptionAPI supports PUT and GET operation to upsert table description
    """
    def __init__(self) -> None:
        self.client = get_proxy_client()
        super(TableDescriptionAPI, self).__init__()

    @swag_from('swagger_doc/table/description_get.yml')
    def get(self, table_uri: str) -> Iterable[Any]:
        """
        Returns description in Neo4j endpoint
        """
        try:
            description = self.client.get_table_description(table_uri=table_uri)
            return {'description': description}, HTTPStatus.OK

        except NotFoundException:
            return {'message': 'table_uri {} does not exist'.format(table_uri)}, HTTPStatus.NOT_FOUND

        except Exception:
            return {'message': 'Internal server error!'}, HTTPStatus.INTERNAL_SERVER_ERROR

    @swag_from('swagger_doc/table/description_put.yml')
    def put(self, table_uri: str) -> Iterable[Any]:
        """
        Updates table description (passed as a request body)
        :param table_uri:
        :return:
        """
        try:
            description = json.loads(request.data).get('description')
            self.client.put_table_description(table_uri=table_uri, description=description)
            return None, HTTPStatus.OK

        except NotFoundException:
            return {'message': 'table_uri {} does not exist'.format(table_uri)}, HTTPStatus.NOT_FOUND


class TableTagAPI(Resource):
    """
    TableTagAPI that supports GET, PUT and DELETE operation to add or delete tag
    on table
    """

    def __init__(self) -> None:
        self.client = get_proxy_client()
        self.search_client = SearchProxy(config=current_app.config)
        self.parser = reqparse.RequestParser()
        self.parser.add_argument('tag_type', type=str, required=False, default='default')
        super(TableTagAPI, self).__init__()

    @swag_from('swagger_doc/table/tag_put.yml')
    def put(self, table_uri: str, tag: str) -> Iterable[Union[Mapping, int, None]]:
        """
        API to add a tag to existing table uri.

        :param table_uri:
        :param tag:
        :return:
        """
        args = self.parser.parse_args()
        # use tag_type to distinguish between tag and badge
        tag_type = args.get('tag_type', 'default')

        whitelist_badges = app.config.get('WHITELIST_BADGES', [])
        if tag_type == 'badge':
            # need to check whether the badge is part of the whitelist:
            if tag not in whitelist_badges:
                return \
                    {'message': 'The tag {} for table_uri {} with type {} '
                                'is not added successfully as badge '
                                'is not part of the whitelist'.format(tag,
                                                                      table_uri,
                                                                      tag_type)}, \
                    HTTPStatus.NOT_FOUND
        else:
            if tag in whitelist_badges:
                return \
                    {'message': 'The tag {} for table_uri {} with type {} '
                                'is not added successfully as tag '
                                'for it is reserved for badge'.format(tag,
                                                                      table_uri,
                                                                      tag_type)}, \
                    HTTPStatus.CONFLICT

        try:
            self.client.add_tag(table_uri=table_uri, tag=tag, tag_type=tag_type)
            if app.config.get('SHOULD_UPDATE_ELASTIC'):
                search_document = self.client.get_table_search_document(table_uri=table_uri)
                self.search_client.update_elastic(table_uri=table_uri, data=search_document)
            return {'message': 'The tag {} for table_uri {} with type {} '
                               'is added successfully'.format(tag,
                                                              table_uri,
                                                              tag_type)}, HTTPStatus.OK
        except NotFoundException:
            return \
                {'message': 'The tag {} for table_uri {} with type {} '
                            'is not added successfully'.format(tag,
                                                               table_uri,
                                                               tag_type)}, \
                HTTPStatus.NOT_FOUND

    @swag_from('swagger_doc/table/tag_delete.yml')
    def delete(self, table_uri: str, tag: str) -> Iterable[Union[Mapping, int, None]]:
        """
        API to remove a association between a given tag and a table.

        :param table_uri:
        :param tag:
        :return:
        """
        args = self.parser.parse_args()
        tag_type = args.get('tag_type', 'default')

        try:
            self.client.delete_tag(table_uri=table_uri,
                                   tag=tag,
                                   tag_type=tag_type)
            return {'message': 'The tag {} for table_uri {} with type {} '
                               'is deleted successfully'.format(tag,
                                                                table_uri,
                                                                tag_type)}, HTTPStatus.OK
        except NotFoundException:
            return \
                {'message': 'The tag {} for table_uri {} with type {} '
                            'is not deleted successfully'.format(tag,
                                                                 table_uri,
                                                                 tag_type)}, \
                HTTPStatus.NOT_FOUND
