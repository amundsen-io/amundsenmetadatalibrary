from typing import List

import attr
from amundsen_common.models.popular_table import PopularTable
from amundsen_common.models.user import User
from marshmallow_annotations.ext.attrs import AttrsSchema

from amundsen_common.models.table import Tag


@attr.s(auto_attribs=True, kw_only=True)
class DashboardDetail:
    uri: str = attr.ib()
    cluster: str = attr.ib()
    group_name: str = attr.ib()
    group_url: str = attr.ib()
    name: str = attr.ib()
    url: str = attr.ib()
    description: str = attr.ib()
    created_timestamp: int = attr.ib()
    updated_timestamp: int = attr.ib()
    last_run_timestamp: int = attr.ib()
    last_run_state: str = attr.ib()
    owners: List[User] = attr.ib(factory=list)
    frequent_users: List[User] = attr.ib(factory=list)
    chart_names: List[str] = attr.ib(factory=list)
    query_names: List[str] = attr.ib(factory=list)
    tables: List[PopularTable] = attr.ib(factory=list)
    tags: List[Tag] = attr.ib(factory=list)


class DashboardSchema(AttrsSchema):
    class Meta:
        target = DashboardDetail
        register_as_scheme = True
