from threading import Lock

from flask import current_app
from werkzeug.utils import import_string

from metadata_service import config
from metadata_service.proxy.base_proxy import BaseProxy
import boto3

_proxy_client = None
_proxy_client_lock = Lock()


def get_proxy_client() -> BaseProxy:
    """
    Provides singleton proxy client based on the config
    :return: Proxy instance of any subclass of BaseProxy
    """
    global _proxy_client

    if _proxy_client:
        return _proxy_client

    with _proxy_client_lock:
        if _proxy_client:
            return _proxy_client
        else:
            # Gather all the configuration to create a Proxy Client
            host = current_app.config[config.PROXY_HOST]
            port = current_app.config[config.PROXY_PORT]
            user = current_app.config[config.PROXY_USER]
            password = current_app.config[config.PROXY_PASSWORD]
            encrypted = current_app.config[config.PROXY_ENCRYPTED]
            validate_ssl = current_app.config[config.PROXY_VALIDATE_SSL]

            client_init_params = {
                'host': host,
                'port': port,
                'user': user,
                'password': password,
                'encrypted': encrypted,
                'validate_ssl': validate_ssl
            }

            proxy_client_name = current_app.config[config.PROXY_CLIENT_NAME]
            if proxy_client_name == "NEPTUNE":
                aws_region = current_app.config[config.PROXY_AWS_REGION]
                session = boto3.Session()
                aws_creds = session.get_credentials()
                aws_access_key = aws_creds.access_key
                aws_access_secret = aws_creds.secret_key
                aws_token = aws_creds.token
                client_init_params['password'] = {
                    'aws_access_key_id': aws_access_key,
                    'aws_secret_access_key': aws_access_secret,
                    'service_region': aws_region
                }
                client_init_params['aws4auth_options'] = {
                    'session_token': aws_token
                }
                client_init_params.pop('encrypted')
                client_init_params.pop('validate_ssl')
                neptune_port = client_init_params.pop('port')
                neptune_endpoint = client_init_params.pop('host')
                client_init_params.pop('user')
                client_init_params['host'] = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)

            client = import_string(current_app.config[config.PROXY_CLIENT])
            _proxy_client = client(**client_init_params)

    return _proxy_client
