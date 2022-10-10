from .database import DatabaseResolver
from .schema import SchemaResolver
from .table import TableResolver
from .view import ViewResolver

default_resolver_sequence = [
    DatabaseResolver,
    SchemaResolver,
    TableResolver,
    ViewResolver,
]

singledb_resolver_sequence = [
    SchemaResolver,
    TableResolver,
    ViewResolver,
]
