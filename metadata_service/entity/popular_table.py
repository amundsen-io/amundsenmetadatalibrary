from typing import Optional


class PopularTable:

    def __init__(self, *,
                 key: str,
                 database: str,
                 cluster: str,
                 schema: str,
                 name: str,
                 description: Optional[str] = None) -> None:
        self.key = key
        self.database = database
        self.cluster = cluster
        self.schema = schema
        self.name = name
        self.description = description

    def __repr__(self) -> str:
        return """Table(key={!r}, database={!r}, cluster={!r}, 
        schema={!r}, name={!r}, description={!r})""".format(
            self.key, self.database, self.cluster,
            self.schema, self.name, self.description)
