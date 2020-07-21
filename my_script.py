import os
from metadata_service.proxy.neptune_proxy import NeptuneGremlinProxy


def main():
    access_key = os.getenv('AWS_KEY')
    access_secret = os.getenv('AWS_SECRET_KEY')
    aws_zone = os.getenv("AWS_ZONE")
    neptune_endpoint = os.getenv('NEPTUNE_ENDPOINT')
    neptune_port = int(os.getenv("NEPTUNE_PORT"))
    auth_dict = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': access_secret,
        'service_region': aws_zone
    }
    neptune_host = "wss://{}:{}/gremlin".format(neptune_endpoint, neptune_port)
    neptune_proxy = NeptuneGremlinProxy(
        host=neptune_host,
        password=auth_dict
    )
    table = neptune_proxy.get_table(table_uri='postgres://sganalytic.public/action_touchpoints')
    print(neptune_proxy)


if __name__ == '__main__':
    main()