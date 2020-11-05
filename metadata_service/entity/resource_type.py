# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

from enum import Enum, auto


class ResourceType(Enum):
    Table = auto()
    Dashboard = auto()
    User = auto()


def to_resource_type(*, label: str) -> ResourceType:
    """
    Convert a resource type to a resource type.

    Args:
        label: (todo): write your description
    """
    return ResourceType[label.title()]
