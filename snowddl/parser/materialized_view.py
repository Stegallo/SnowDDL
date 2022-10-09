from snowddl.blueprint import (
    Ident,
    MaterializedViewBlueprint,
    SchemaObjectIdent,
    ViewColumn,
)
from snowddl.parser.abc_parser import AbstractParser, ParsedFile

materialized_view_json_schema = {
    "type": "object",
    "properties": {
        "columns": {"type": "object", "additionalProperties": {"type": "string"}},
        "text": {"type": "string"},
        "cluster_by": {"type": "array", "items": {"type": "string"}, "minItems": 1},
        "is_secure": {"type": "boolean"},
        "comment": {"type": "string"},
    },
    "required": ["text"],
    "additionalProperties": False,
}


class MaterializedViewParser(AbstractParser):
    def load_blueprints(self):
        self.parse_schema_object_files(
            "materialized_view",
            materialized_view_json_schema,
            self.process_materialized_view,
        )

    def process_materialized_view(self, f: ParsedFile):
        column_blueprints = [
            ViewColumn(name=Ident(col_name), comment=col_comment or None)
            for col_name, col_comment in f.params.get("columns", {}).items()
        ]


        bp = MaterializedViewBlueprint(
            full_name=SchemaObjectIdent(
                self.env_prefix, f.database, f.schema, f.name
            ),
            text=f.params["text"],
            columns=column_blueprints or None,
            is_secure=f.params.get("is_secure", False),
            cluster_by=f.params.get("cluster_by"),
            comment=f.params.get("comment"),
        )


        self.config.add_blueprint(bp)
