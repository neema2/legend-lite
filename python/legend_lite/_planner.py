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
    'executionPlan': ('lite_execution_plan_json_held', lambda m: (str(m['handle']), m['lambda'], m['runtime'])),
    'planModel': ('lite_plan_json', lambda m: (m['model'], m['lambda'], m['runtime'])),
    'pureV1': ('lite_pure_v1', lambda m: (m['path'], m['query'], m['body'])),
    'executePlan': ('lite_execute_json_plan', lambda m: (m['body'], m['models'])),
    'executeAnswer': ('lite_execute_json_answer', lambda m: (m['plan'], m['rows'])),
    'fileLoad': ('lite_file_load', lambda m: (m['request'],)),
    'remoteMount': ('lite_remote_mount', lambda m: (m['request'],)),
    'snapStream': ('lite_snap_stream', lambda m: (m['request'],)),
    'snapCopy': ('lite_snap_copy', lambda m: (m['request'],)),
    'outputPlan': ('lite_output_plan', lambda m: (m['request'],)),
    'synthesize': ('lite_synthesize', lambda m: (m['request'],)),
    'ingestPlans': ('lite_ingest_plans', lambda m: (m['request'],)),
    'sessionSetup': ('lite_session_setup', lambda m: (m['databaseType'],)),
    'projectData': ('lite_project_data', lambda m: (m['model'], m['databaseType'])),
    'hostOperations': ('lite_host_operations', lambda m: (m['databaseType'], m['operations'])),
    'warm': ('lite_warm_model', lambda m: (m['model'],)),
    'deephaven': ('lite_deephaven', lambda m: (m['op'], m['args'])),
}

# a kind's form when its request names a library the model is compiled over (the protocol's ``hold``'s ``library``,
# docs/TS_V1_PROGRAM_2026_11_01.md A5): planner-answer.ts's ``holdModelOverOrError``, the library held as a layer
OVER: dict[str, tuple[str, Callable[[dict[str, Any]], tuple[str, ...]]]] = {
    'hold': ('lite_hold_model_over', lambda m: (m['model'], m['library'])),
}


def function(request: dict[str, Any]) -> tuple[str, Callable[[dict[str, Any]], tuple[str, ...]]]:
    """The native function answering ``request`` and its arguments: its kind's, or its kind's over a library."""
    kind = request['kind']
    return OVER[kind] if request.get('library') is not None and kind in OVER else KINDS[kind]


# THE KINDS THIS HOST REFUSES (2026-11-12, the orchestrator's ruling: the authored door -- a person's own statement
# carried as written -- is Studio's SQL playground's alone, and no headless host walked it; its KINDS row, its library
# arity and the native library's lite_authored export are deleted): the protocol still names it (the tab answers it), so
# this host refuses a request of it by name, the planner never asked
REFUSED: dict[str, str] = {
    'authored': "a person's own statement is planned for Studio's SQL playground alone: this host carries none",
}


def answer(lib: Library, request: dict[str, Any]) -> dict[str, Any]:
    """One planner request answered as planner-worker.ts answers it: the function's tagged answer (a refusal is one too),
    or, when the call itself fails, the worker's failure. A kind this host refuses (``REFUSED``) is refused by name."""
    if request.get('kind') in REFUSED:
        return {'id': request.get('id'), 'ok': False, 'error': f"NotImplemented: {REFUSED[request['kind']]}"}
    try:
        name, args = function(request)
        return {'id': request.get('id'), 'ok': True, 'answer': lib.call(name, *args(request))}
    except Exception as e:  # noqa: BLE001 -- the worker answers its own failure, as planner-worker.ts's catch does
        return {'id': request.get('id'), 'ok': False, 'error': f'{type(e).__name__}: {e}'}
