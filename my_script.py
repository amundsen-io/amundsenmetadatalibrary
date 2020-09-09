import os
from metadata_service.proxy.neptune_proxy import NeptuneGremlinProxy
from metadata_service.proxy.gremlin_proxy import AbstractGremlinProxy
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from gremlin_python.structure.graph import Graph
import boto3
from gremlin_python.process.traversal import T, Order, gt
from gremlin_python.process.graph_traversal import __
from metadata_service.util import UserResourceRel
from metadata_service.entity.resource_type import to_resource_type, ResourceType


def main():

    #test_proxy = AbstractGremlinProxy(key_property_name='key', remote_connection=DriverRemoteConnection('ws://localhost:8182/gremlin', 'g'))

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
        aws4auth_options=extra_options,
        key_property_name=T.id
    )
    # neptune_proxy.add_resource_relation_by_user(
    #     id='postgres://sganalytic.public/user_purchases',
    #     user_id='aciambrone@seatgeek.com',
    #     relation_type=UserResourceRel.follow,
    #     resource_type=to_resource_type(label='Table')
    # )
    # neptune_proxy.get_table_by_user_relation(user_email='aciambrone@seatgeek.com', relation_type=UserResourceRel.follow)
    table = neptune_proxy.get_table(
        table_uri='postgres://sganalytic.public/event'
    )
    # neptune_proxy.add_owner(table_uri='postgres://sganalytic.public/customer_acquisition', owner='andrew_ciambrone')
    # neptune_proxy.put_column_description(table_uri='postgres://sganalytic.public/customer_acquisition', column_name='acquired_at', description="When the user signed up")
    table = neptune_proxy.get_table(table_uri='postgres://sganalytic.public/event')
    neptune_proxy.get_column_description(table_uri='postgres://sganalytic.public/customer_acquisition', column_name='acquired_at')
    # neptune_proxy.delete_owner(table_uri='postgres://sganalytic.public/user_purchases', owner='andrew_ciambrone')
    # d = neptune_proxy.get_table_description(table_uri='postgres://sganalytic.public/user_purchases')
    # neptune_proxy.put_table_description(table_uri='postgres://sganalytic.public/user_purchases', description="description")
    popular_tables = neptune_proxy.get_popular_tables(num_entries=10)

    tags = neptune_proxy.get_tags()
    print(neptune_proxy)


if __name__ == '__main__':
    main()