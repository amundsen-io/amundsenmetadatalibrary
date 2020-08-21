# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from flask_restful import Resource, fields, marshal

from http import HTTPStatus
from typing import Iterable, Union, Mapping, Tuple, Any

from flasgger import swag_from
from flask import current_app as app

from metadata_service.entity.resource_type import ResourceType
from metadata_service.proxy import get_proxy_client

badge_fields = {
    'badge_name': fields.String,
    'sentiment': fields.String,
    'category': fields.String
}

badges_fields = {
    'badges': fields.List(fields.Nested(badge_fields))
}

class BadgeAPI(Resource):
    def __init__(self) -> None:
        self.client = get_proxy_client()
        super(BadgeAPI, self).__init__()

    @swag_from('swagger_doc/badge/badge_get.yml')
    def get(self) -> Iterable[Union[Mapping, int, None]]:
        """
        API to get all existing badges
        """
        badges = self.client.get_tags()
        return marshal({'badges': badges}, badges_fields), HTTPStatus.OK