# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from flask_restful import Resource, fields, marshal

from http import HTTPStatus
from typing import Iterable, Union, Mapping, Tuple, Any

from flasgger import swag_from
from flask import current_app as app

from metadata_service.entity.resource_type import ResourceType
from metadata_service.entity.badge import Badge
from metadata_service.exception import NotFoundException
from metadata_service.proxy import get_proxy_client
from metadata_service.proxy.base_proxy import BaseProxy

badge_fields = {
    'badge_name': fields.String,
    'category': fields.String,
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
    def put(self, id: str, resource_type: ResourceType,
            badge_name: str,
            category: str = '',
            badge_type: str = '') -> Tuple[Any, HTTPStatus]:

        if badge_type == '' or category == '':
            return \
                {'message': 'The badge {} for resource id {} is not added successfully because '
                            'category `{}` and badge_type `{}` parameters are required '
                            'for badges'.format(badge_name, id, category, badge_type)}, \
                HTTPStatus.NOT_FOUND

        # TODO check resource type is column when adding a badge of category column after
        # implementing column level badges
        whitelist_badges = app.config.get('WHITELIST_BADGES', [])
        incomimg_badge = Badge(badge_name=badge_name,
                               category=category,
                               badge_type=badge_type)
        # need to check whether the badge combination is part of the whitelist:
        for badge in whitelist_badges:
            if not incomimg_badge.equals(badge):
                return \
                    {'message': 'The badge {} with category {} badge_type {} for resource id {} '
                                'and resource_type {} is not added successfully because this combination '
                                'of values is not part of the whitelist'.format(badge_name,
                                                                                category,
                                                                                badge_type,
                                                                                id,
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
        except Exception as e:
            return {'message': 'The badge {} with category {}, badge type {} for resource id {} '
                               'and resource_type {} failed to be added'.format(badge_name,
                                                                                category,
                                                                                badge_type,
                                                                                id,
                                                                                resource_type.name)}, \
                HTTPStatus.NOT_FOUND

    def delete(self, id: str, badge_name: str,
               category: str,
               badge_type: str,
               resource_type: ResourceType) -> Tuple[Any, HTTPStatus]:
        try:
            self.client.delete_badge(id=id,
                                     resource_type=resource_type,
                                     badge_name=badge_name,
                                     category=category,
                                     badge_type=badge_type)
            return \
                {'message': 'The badge {} with category {}, badge type {} for resource id {} '
                            'and resource_type {} was deleted successfully'.format(badge_name,
                                                                                   category,
                                                                                   badge_type,
                                                                                   id,
                                                                                   resource_type.name)}, \
                HTTPStatus.OK
        except NotFoundException:
            return \
                {'message': 'The badge {} with category {}, badge type {} for resource id {} '
                            'and resource_type {} was not deleted successfully'.format(badge_name,
                                                                                       category,
                                                                                       badge_type,
                                                                                       id,
                                                                                       resource_type.name)}, \
                HTTPStatus.NOT_FOUND
