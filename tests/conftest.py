# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import pytest

from _pytest.config import Config, Parser
from _pytest.nodes import Item
from typing import List

# This file configures the roundtrip pytest option and skips roundtrip tests without it


def pytest_addoption(parser: Parser) -> None:
    """
    Add a pytest. pytest.

    Args:
        parser: (todo): write your description
    """
    parser.addoption(
        "--roundtrip-neptune", action="store_true", default=False, help="Run roundtrip tests. These tests are slow and require \
        a configured neptune instance."
    )
    parser.addoption(
        "--roundtrip-janusgraph", action="store_true", default=False, help="Run roundtrip tests. These tests are slow and require \
        a configured janusgraph instance."
    )


def pytest_configure(config: Config) -> None:
    """
    Configure pytest_configure. py.

    Args:
        config: (todo): write your description
    """
    config.addinivalue_line("markers", "roundtrip: mark test as roundtrip")


def pytest_collection_modifyitems(config: Config, items: List[Item]) -> None:
    """
    Modify all the pytestus. py : meth : pytestus. py : param config : meth : pytest :

    Args:
        config: (todo): write your description
        items: (todo): write your description
    """
    roundtrip_neptune: bool = config.getoption("--roundtrip-neptune")
    roundtrip_janusgraph: bool = config.getoption("--roundtrip-janusgraph")
    skip_roundtrip = pytest.mark.skip(reason="need the approprirate --roundtrip-[neptune|janus] option to run")
    for item in items:
        if "NeptuneGremlinProxyTest" in item.keywords and not roundtrip_neptune:
            item.add_marker(skip_roundtrip)
        if "JanusGraphGremlinProxyTest" in item.keywords and not roundtrip_janusgraph:
            item.add_marker(skip_roundtrip)
