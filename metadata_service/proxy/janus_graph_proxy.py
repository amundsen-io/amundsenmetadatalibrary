# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Mapping, Optional, Type

from amundsen_gremlin.script_translator import ScriptTranslatorTargetJanusgraph
from overrides import overrides

from .gremlin_proxy import AbstractGremlinProxy


class JanusGraphGremlinProxy(AbstractGremlinProxy):
    """
    A proxy to a JanusGraph using the Gremlin protocol.

    TODO: HTTP proxy support.  This does *NOT* support HTTP proxies as-is. Why? The default transport factory in
    gremlin_python is tornado.websocket, which is hardcoded to use simple_httpclient (look at
    WebSocketClientConnection).  But, even if that could be made to use curl_httpclient, curl_httpclient requires pycurl
    which requires libcurl and other native libraries which is a pain to install.
    """
    def __init__(self, *, host: str, port: Optional[int] = None, user: Optional[str] = None,
                 password: Optional[str] = None, traversal_source: 'str' = 'g',
                 driver_remote_connection_options: Mapping[str, Any] = {}) -> None:
        """
        Establish a connection to a connection.

        Args:
            self: (todo): write your description
            host: (str): write your description
            port: (int): write your description
            user: (str): write your description
            password: (str): write your description
            traversal_source: (str): write your description
            driver_remote_connection_options: (todo): write your description
        """
        driver_remote_connection_options = dict(driver_remote_connection_options)

        # as others, we repurpose host a url, and url can be an HTTPRequest
        self.url = host

        # port should be part of that url
        if port is not None:
            raise NotImplementedError(f'port is not allowed! port={port}')

        if user is not None:
            driver_remote_connection_options.update(username=user)
        if password is not None:
            driver_remote_connection_options.update(password=password)

        driver_remote_connection_options.update(traversal_source=traversal_source)

        # use _key
        AbstractGremlinProxy.__init__(self, key_property_name='_key',
                                      driver_remote_connection_options=driver_remote_connection_options)

    @classmethod
    @overrides
    def script_translator(cls) -> Type[ScriptTranslatorTargetJanusgraph]:
        """
        Translator script for the given script.

        Args:
            cls: (todo): write your description
        """
        return ScriptTranslatorTargetJanusgraph

    @overrides
    def possibly_signed_ws_client_request_or_url(self) -> str:
        """
        Return the url or none if the request is signed.

        Args:
            self: (todo): write your description
        """
        return self.url
