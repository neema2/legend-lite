"""THE PLANNER PROTOCOL, answered by legend-lite's native library (engine-client/src/legend/planner-protocol.ts, the one
protocol every app that plans in a tab sends: docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C2): each request kind, the native
function answering it, and that function's arguments from the request. Each
function delegates to the Boundary function the tab's TabExports export of that kind delegates to, so the tab's worker
and Python answer every request the same way. ``KINDS`` is that table, the one place it is written: DataCube's
headless engine answers by it (legend_lite._headless): DataCube's code in this process, its worker answered in process.
A page's planner asks Python's engine over HTTP instead, on legend-engine's own routes and the host's compute
(docs/DATACUBE_ONE_APP_DESIGN_2026_10_11.md §15). Python's standard library only."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from ._library import Library

KINDS: dict[str, tuple[str, Callable[[dict[str, Any]], tuple[str, ...]]]] = {
    'hold': ('lite_hold_model', lambda m: (m['model'],)),
    'release': ('lite_release_model', lambda m: (str(m['handle']),)),
    'plan': ('lite_plan_text_held', lambda m: (str(m['handle']), m['query'], m['runtime'])),
    'relationType': ('lite_relation_type_text_held', lambda m: (str(m['handle']), m['query'])),
    'tableModel': ('lite_table_model', lambda m: (m['table'],)),
    'catalogColumnsSql': ('lite_catalog_columns_sql', lambda m: (m['schema'], m['table'])),
    'planJson': ('lite_plan_json_held', lambda m: (str(m['handle']), m['lambda'], m['runtime'])),
    'planModel': ('lite_plan_json', lambda m: (m['model'], m['lambda'], m['runtime'])),
    'pureV1': ('lite_pure_v1', lambda m: (m['path'], m['query'], m['body'])),
    'executePlan': ('lite_execute_json_plan', lambda m: (m['body'], m['models'])),
    'executeAnswer': ('lite_execute_json_answer', lambda m: (m['plan'], m['wire'])),
    'testData': ('lite_test_data_sql', lambda m: (m['model'], m['database'], m['tables'])),
    'sessionSetup': ('lite_session_setup', lambda m: (m['databaseType'],)),
    'projectData': ('lite_project_data', lambda m: (m['model'], m['databaseType'])),
    'hostOperations': ('lite_host_operations', lambda m: (m['databaseType'], m['operations'])),
    'warm': ('lite_warm_model', lambda m: (m['model'],)),
}


def answer(lib: Library, request: dict[str, Any]) -> dict[str, Any]:
    """One planner request answered as planner-worker.ts answers it: the function's tagged answer (a refusal is one too),
    or, when the call itself fails, the worker's failure."""
    try:
        name, args = KINDS[request['kind']]
        return {'id': request.get('id'), 'ok': True, 'answer': lib.call(name, *args(request))}
    except Exception as e:  # noqa: BLE001 -- the worker answers its own failure, as planner-worker.ts's catch does
        return {'id': request.get('id'), 'ok': False, 'error': f'{type(e).__name__}: {e}'}
