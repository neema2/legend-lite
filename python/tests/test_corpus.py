"""The planner differential corpus (wasm/corpus: every query, refusals included) through the NATIVE
library, answer for answer against the JVM's (//wasm:jvm_answers) -- which the WebAssembly build is
held to as well (//wasm:differential_test). Three builds of one source, one answer."""

import os
import re
import unittest
from pathlib import Path

from legend_lite import _json
from legend_lite._library import library


def parsed_then_planned(model, text, runtime, plan=None):
    """A text query as the tab and the JVM compose it (planner.JvmMain): lite's query/parse, then -- parsed -- its
    query/plan (the lite API v1, 2026-11-17), each answer as the native entry folds it."""
    parsed = library().call("lite_query_parse", _json.dumps({"text": text}))
    if not parsed.startswith("OK\n"):
        return parsed
    query = _json.loads(parsed[3:])["query"]
    if plan is not None:
        return plan(query)
    return library().call("lite_query_plan", _json.dumps({"model": model, "query": query, "runtime": runtime}))


class Corpus(unittest.TestCase):
    def test_every_answer_is_the_jvms(self):
        model = Path(os.environ["LEGEND_LITE_CORPUS_MODEL"]).read_text()
        queries = [
            l.split("\t", 1) for l in Path(os.environ["LEGEND_LITE_CORPUS_QUERIES"]).read_text().split("\n") if l
        ]
        jvm = dict(
            re.findall(
                r"<<<([^>]+)>>>\n([\s\S]*?)\n<<<END>>>\n", Path(os.environ["LEGEND_LITE_JVM_ANSWERS"]).read_text()
            )
        )
        self.assertGreater(len(queries), 50, "the corpus is not there")
        differ = []
        refusals = 0
        for name, q in queries:
            got = parsed_then_planned(model, q, "trades::RT")
            refusals += got.startswith("ERR")
            if got != jvm.get(name):
                differ.append(f"{name}:\n  jvm:    {jvm.get(name, '(none)')[:200]}\n  native: {got[:200]}")
        self.assertEqual(differ, [], "\n".join(differ))
        self.assertGreater(refusals, 0, "the corpus holds refusals too: none came back as one")
        print(f"{len(queries)} answers identical to the JVM, {refusals} of them refusals")

    def test_every_answer_over_the_model_held_is_the_jvms(self):
        # a DataCube planner's held plan, as the headless host answers it (lite_query_plan_held over
        # Boundary.queryPlanHeld): the model compiled once, each query planned against it, the same answers
        model = Path(os.environ["LEGEND_LITE_CORPUS_MODEL"]).read_text()
        queries = [
            l.split("\t", 1) for l in Path(os.environ["LEGEND_LITE_CORPUS_QUERIES"]).read_text().split("\n") if l
        ]
        jvm = dict(
            re.findall(
                r"<<<([^>]+)>>>\n([\s\S]*?)\n<<<END>>>\n", Path(os.environ["LEGEND_LITE_JVM_ANSWERS"]).read_text()
            )
        )
        held = library().call("lite_hold_model", model)
        self.assertTrue(held.startswith("OK\n"), held)
        handle = held[3:]
        try:

            def held_plan(query):
                return library().call(
                    "lite_query_plan_held", handle, _json.dumps({"query": query, "runtime": "trades::RT"})
                )

            differ = [
                name for name, q in queries if parsed_then_planned(model, q, "trades::RT", held_plan) != jvm.get(name)
            ]
        finally:
            library().call("lite_release_model", handle)
        self.assertEqual(differ, [], f"{len(differ)} answers differ over the model held: {differ[:10]}")


if __name__ == "__main__":
    unittest.main()
