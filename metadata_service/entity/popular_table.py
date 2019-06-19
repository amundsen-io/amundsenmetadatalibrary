from typing import Optional


class PopularTable:

    def __init__(self, *,
                 database: str,
                 cluster: str,
                 schema: str,
                 name: str,
                 key: str,
                 entity_type: str,
                 last_updated_epoch: Optional[str] = None,
                 description: Optional[str] = None) -> None:
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.name = name
        self.description = description
        self.key = key
        self.entity_type = entity_type
        self.last_updated_epoch = last_updated_epoch

    def __repr__(self) -> str:
        return """Table(cluster={!r}, database={!r}, description={!r}, key={!r}, name={!r}, schema_name={!r}, 
        type={!r},last_updated_epoch={!r})"""\
            .format(self.cluster, self.database, self.description,
                    self.key, self.name, self.schema, self.entity_type, self.last_updated_epoch)
