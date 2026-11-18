# PLANTED (census B2): an edge module, the one user of the product's engine_columns.
from .._planted_reader import engine_columns

ROUTES = {'/api/server/v1/currentUser': engine_columns}
