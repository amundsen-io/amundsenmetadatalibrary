import json
import time

from typing import Any, Dict

from metadata_service.utils.request_utils import request_search


class SearchProxy:
    def __init__(self, *, config: Dict[str, Any]) -> None:
        self.config = config
        self.searchservice_base = config['SEARCHSERVICE_BASE']

    def update_elastic(self, *, table_uri: str, data: Dict[str, Any]) -> None:
        URL = f'{self.searchservice_base}/document_table'
        json_data = json.dumps([dict(data, last_updated_epoch=int(time.time()))])
        request_search(url=URL, method='PUT', config=self.config, data=json.dumps({'data': json_data}))
