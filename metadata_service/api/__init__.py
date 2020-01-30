import logging
from http import HTTPStatus
from typing import Iterable, Union, Mapping, Any, Optional, List

from flask import request
from flask_restful import Resource

from metadata_service.exception import NotFoundException
from metadata_service.proxy import BaseProxy
from marshmallow import UnmarshalResult, ValidationError

LOGGER = logging.getLogger(__name__)


def produce_not_found_response(slug: str, exception: Exception) -> tuple:
    """
    Returns Not Found status with message containing slug that was not found.
    Logs exception that prompted this response.

    :param exception:
    :param slug:
    :return: Tuple with message containing slug that was not found and NOT_FOUND status
    """
    msg = f'slug {slug} does not exist'
    LOGGER.exception(f'Not Found error caused by: {exception}')
    return {'message': msg}, HTTPStatus.NOT_FOUND


def produce_internal_server_error_response(exception: Exception) -> tuple:
    """
    Returns Internal Server Error status and logs exception that caused the
    error

    :param exception: Any exception can be bubbled up here
    :return: Tuple with opaque error message and INTERNAL_SERVER_ERROR status
    """
    LOGGER.exception(f'Internal server error caused by: {exception}')
    return {'message': 'Internal server error!'}, HTTPStatus.INTERNAL_SERVER_ERROR


class BaseAPI(Resource):
    def __init__(self, schema: Any, str_type: str, client: BaseProxy) -> None:
        self.schema = schema
        self.client = client
        self.str_type = str_type
        self.allow_empty_upload = False

    def post(self) -> Iterable[Union[List, Mapping, int, None]]:
        post_objects = getattr(self.client, f'post_{self.str_type}s')
        """
        Inserts or updates objects
        """
        try:
            json_data = request.get_json()
            data = None
            if json_data is None:
                if not self.allow_empty_upload:
                    return {'message': 'No input data provided'}, HTTPStatus.BAD_REQUEST
            else:
                # Validate and deserialize input
                try:
                    schema = self.schema(many=True, strict=True)
                    result: UnmarshalResult = schema.load(json_data)
                    data = result.data
                except ValidationError as err:
                    return {'messages': err.messages}, HTTPStatus.UNPROCESSABLE_ENTITY
            post_result: Any = post_objects(data=data)

            return post_result, HTTPStatus.OK

        except Exception as e:
            return produce_internal_server_error_response(e)

    # TODO: refactor to actually use this id in PUTs
    def put(self, *, id: str = '') -> Iterable[Union[Mapping, int, None]]:
        put_object = getattr(self.client, f'put_{self.str_type}')
        """
        Inserts or updates a single object
        """
        try:
            json_data = request.get_json()
            data = None
            if json_data is None:
                if not self.allow_empty_upload:
                    return {'message': 'No input data provided'}, HTTPStatus.BAD_REQUEST
            else:
                # Validate and deserialize input
                try:
                    schema = self.schema(strict=True)
                    result: UnmarshalResult = schema.load(json_data)
                    data = result.data
                except ValidationError as err:
                    return {'messages': err.messages}, HTTPStatus.UNPROCESSABLE_ENTITY
            put_object(data=data)

            return None, HTTPStatus.OK

        except NotFoundException as e:
            return produce_not_found_response(id, e)

        except Exception as e:
            return produce_internal_server_error_response(e)

    def get(self, *, id: Optional[str] = None) -> Iterable[Union[Mapping, int, None]]:
        """
        Gets a single or multiple objects
        """
        if id is not None:
            get_object = getattr(self.client, f'get_{self.str_type}')
            try:
                actual_id: Union[str, int] = int(id) if id.isdigit() else id
                object = get_object(id=actual_id)
                if object is not None:
                    return self.schema().dump(object).data, HTTPStatus.OK
                return None, HTTPStatus.NOT_FOUND
            except ValueError as e:
                return {'message': f'exception:{e}'}, HTTPStatus.BAD_REQUEST
        else:
            get_objects = getattr(self.client, f'get_{self.str_type}s')
            objects: List[Any] = get_objects()
            return self.schema(many=True).dump(objects).data, HTTPStatus.OK
