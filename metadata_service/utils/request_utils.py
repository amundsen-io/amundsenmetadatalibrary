import requests
from flask import current_app as app
from typing import Any, Dict

BATCH_TABLE_UPLOAD_TIMEOUT_SEC: int = 15
REQUEST_SESSION_TIMEOUT_SEC: int = 3
CONTENT_TYPE: str = 'application/json'


def request_search(*,
                   url: str,
                   method: str = 'GET',
                   timeout_sec: int = REQUEST_SESSION_TIMEOUT_SEC,
                   data: Any = '',
                   config: Dict[str, Any] = {}) -> Any:
    """
    Helper function to make a request to search service.
    Sets the client and header information based on the configuration
    :param method: DELETE | GET | POST | PUT
    :param url: The request URL
    :param timeout_sec: Number of seconds before timeout is triggered.
    :return:
    """
    if app:
        config = app.config

    if config['REQUEST_HEADERS_METHOD']:
        headers = config['REQUEST_HEADERS_METHOD'](app)
    else:
        headers = config['SEARCHSERVICE_REQUEST_HEADERS']

    if headers is None:
        headers = {}

    return request_wrapper(method=method,
                           url=url,
                           client=config['SEARCHSERVICE_REQUEST_CLIENT'],
                           headers=headers,
                           timeout_sec=timeout_sec,
                           data=data)


def request_wrapper(method: str, url: str, client: Any, headers: dict, timeout_sec: int, data: Any) -> Any:
    """
    Wraps a request to use specified client and headers, if available
    :param method: DELETE | GET | POST | PUT
    :param url: The request URL
    :param client: Optional client
    :param headers: Optional request headers
    :param timeout_sec: Number of seconds before timeout is triggered
    :return:
    """
    headers['Content-Type'] = CONTENT_TYPE
    if client is not None:
        if method == 'DELETE':
            return client.delete(url, headers=headers, raw_response=True)
        elif method == 'GET':
            return client.get(url, headers=headers, raw_response=True)
        elif method == 'POST':
            return client.post(url, headers=headers, raw_response=True, data=data)
        elif method == 'PUT':
            return client.put(url, headers=headers, raw_response=True, data=data)
        else:
            raise Exception('Method not allowed: {}'.format(method))
    else:
        with requests.Session() as s:
            if method == 'DELETE':
                return s.delete(url, headers=headers, timeout=timeout_sec)
            elif method == 'GET':
                return s.get(url, headers=headers, timeout=timeout_sec)
            elif method == 'POST':
                return s.post(url, headers=headers, timeout=timeout_sec, data=data)
            elif method == 'PUT':
                return s.put(url, headers=headers, timeout=timeout_sec, data=data)
            else:
                raise Exception('Method not allowed: {}'.format(method))
