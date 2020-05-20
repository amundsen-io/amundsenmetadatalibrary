from typing import List

import attr
from amundsen_common.models.dashboard import DashboardSummary
from marshmallow_annotations.ext.attrs import AttrsSchema


@attr.s(auto_attribs=True, kw_only=True)
class DashboardSummaries:
    dashboards: List[DashboardSummary] = attr.ib(factory=list)


class DashboardSummariesSchema(AttrsSchema):
    class Meta:
        target = DashboardSummaries
        register_as_scheme = True
