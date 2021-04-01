# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from http import HTTPStatus
from typing import Any, Tuple

from amundsen_common.models.data_quality_checks import TableQualityCheckSchema
from flasgger import swag_from
from flask_restful import Resource

from metadata_service.proxy import get_proxy_client


class TableQualityChecksAPI(Resource):
    def __init__(self) -> None:
        self.client = get_proxy_client()
        super(TableQualityChecksAPI, self).__init__()

    @swag_from('swagger_doc/data_quality_checks/table_quality_checks_get.yml')
    def get(self, table_uri: str) -> Tuple[Any, HTTPStatus]:
        try:
            table_quality_checks = self.client.get_table_quality_checks(table_uri=table_uri)
            schema = TableQualityCheckSchema()
            return {'table_quality_checks': [schema.dump(x) for x in table_quality_checks]}, HTTPStatus.OK
        except Exception as e:
            return {'message': f'Exception raised when getting table quality checks: {e}'}, HTTPStatus.NOT_FOUND
