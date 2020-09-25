# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import pytest

from _pytest.config import Config, Parser
from _pytest.nodes import Item
from typing import List

# This file configures the roundtrip pytest option and skips roundtrip tests without it


def pytest_addoption(parser: Parser) -> None:
    parser.addoption(
        "--roundtrip", action="store_true", default=False, help="Run roundtrip tests. These tests are slow and require \
        a configured neptune instance."
    )


def pytest_configure(config: Config) -> None:
    config.addinivalue_line("markers", "roundtrip: mark test as roundtrip")


def pytest_collection_modifyitems(config: Config, items: List[Item]) -> None:
    if config.getoption("--roundtrip"):
        # --roundtrip given in cli: do not skip roundtrip tests
        return
    skip_roundtrip = pytest.mark.skip(reason="need --roundtrip option to run")
    for item in items:
        if "roundtrip" in item.keywords:
            item.add_marker(skip_roundtrip)
