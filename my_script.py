import os
from metadata_service.proxy.neptune_proxy import NeptuneGremlinProxy
import boto3


def main():

    session = boto3.Session()
    aws_creds = session.get_credentials()
    aws_access_key = aws_creds.access_key
    aws_access_secret = aws_creds.secret_key
    aws_token = aws_creds.token
    aws_zone = os.getenv("AWS_REGION")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = int(os.getenv("NEPTUNE_PORT"))
    auth_dict = {
        'aws_access_key_id': aws_access_key,
        'aws_secret_access_key': aws_access_secret,
        'service_region': aws_zone
    }
    extra_options = {
        'session_token': aws_token
    }
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)
    neptune_proxy = NeptuneGremlinProxy(
        host=neptune_host,
        password=auth_dict,
        aws4auth_options=extra_options
    )
    table = neptune_proxy.get_table(table_uri='postgres://sganalytic.public/action_touchpoints')
    print(neptune_proxy)


if __name__ == '__main__':
    main()