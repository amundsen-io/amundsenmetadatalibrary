# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from flask_restful import Resource, fields, marshal

from http import HTTPStatus
from typing import Iterable, Union, Mapping, Tuple, Any

from flasgger import swag_from
from flask import current_app as app

from metadata_service.entity.resource_type import ResourceType
from metadata_service.exception import NotFoundException
from metadata_service.proxy import get_proxy_client
from metadata_service.proxy.base_proxy import BaseProxy

badge_fields = {
    'badge_name': fields.String,
    'sentiment': fields.String,
    'badge_type': fields.String,
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
        badges = self.client.get_badges()
        return marshal({'badges': badges}, badges_fields), HTTPStatus.OK

class BadgeCommon:
    def __init__(self, client: BaseProxy) -> None:
        self.client = client
    
    def put(self, id:str, resource_type: ResourceType,
            badge_name: str,
            category: str = '',
            badge_type: str = '') -> Tuple[Any, HTTPStatus]:
        whitelist_badges = app.config.get('WHITELIST_BADGES', [])
        if sentiment == '' or category == '':
            return \
            {'message': 'The badge {} for id {} is not added successfully because '
                        'category `{}` and badge_type {} parameters are required '
                        'for badges'.format(tag, id, category, badge_type)}

        # need to check whether the badge is part of the whitelist:
        if badge_name not in whitelist_badges:
            return \
                {'message': 'The badge {} for id {} with badge_type {} and resource_type {} '
                            'is not added successfully as badge '
                            'is not part of the whitelist'.format(tag,
                                                                    id,
                                                                    tag_type,
                                                                    resource_type.name)}, \
                HTTPStatus.NOT_FOUND
        
        try:
            self.client.add_badge(id=id,
                                badge_name=badge_name,
                                category=category,
                                badge_type=badge_type,
                                resource_type=resource_type)
            return {'message': 'The badge {} for id {} with category {} '
                    'and type {} was added successfully'.format(
                        badge_name,
                        id,
                        category,
                        badge_type)}, HTTPStatus.OK
        except NotFoundException:
            return {'message': 'The badge {} for id {} with category {} '
                    'and type {} failed to be added'.format(
                        badge_name,
                        id,
                        category,
                        badge_type)}, HTTPStatus.NOT_FOUND