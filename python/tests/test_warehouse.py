"""Python's warehouse driver held to the one corpus (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md C3): each recorded
scenario's action run against the recorded answers, the driver making exactly the recorded calls -- the ones the
binding names (warehouse/src/test/resources/sqlapi-corpus.json; SqlApiCallsTest holds the binding to them, and
engine-client/test/warehouse-corpus.test.ts holds the tab's driver to the same)."""

import base64
import io
import json
import os
import unittest
import urllib.error
from unittest import mock

from legend_lite import warehouse
from legend_lite._planned import PlannedSql  # the corpus's statements, the test's own: typed by name (_planned.py)
from legend_lite._runframe import in_run

CORPUS = os.environ.get("LEGEND_LITE_SQLAPI_CORPUS")
BASE = "https://wh.example"


def session(token: str) -> warehouse.Session:
    return warehouse.Session(BASE, token, "rita", "2099-01-01T00:00:00Z")


class Replay:
    """The corpus's answers, in order; each request held to its recorded call (its headers by name, as HTTP's are)."""

    def __init__(self, test: unittest.TestCase, scenario: dict) -> None:
        self.test, self.scenario, self.next = test, scenario, 0

    def __call__(self, request):
        exchanges = self.scenario["exchanges"]
        self.test.assertLess(self.next, len(exchanges), f"{self.scenario['name']}: a call past the recording")
        e = exchanges[self.next]
        self.next += 1
        data = request.data.decode("utf-8") if request.data is not None else None
        self.test.assertEqual(
            (request.get_method(), request.full_url, {k.lower(): v for k, v in request.header_items()}, data),
            (
                e["call"]["method"],
                BASE + e["call"]["path"],
                {k.lower(): v for k, v in e["call"]["headers"].items()},
                e["call"]["body"],
            ),
            f"{self.scenario['name']}: {e['op']}",
        )
        answer = e["answer"]
        body = base64.b64decode(answer["bodyBase64"]) if "bodyBase64" in answer else answer["body"].encode("utf-8")
        if answer["status"] >= 400:
            raise urllib.error.HTTPError(request.full_url, answer["status"], "refused", {}, io.BytesIO(body))
        reply = mock.MagicMock()
        reply.__enter__.return_value = reply
        reply.status, reply.read.return_value = answer["status"], body
        return reply


def act(scenario: dict) -> dict:
    a = scenario["action"]
    engine = lambda: warehouse.Warehouse(session(a["token"]), a.get("catalog", "main"))  # noqa: E731
    try:
        if a["kind"] == "signIn":
            s = warehouse.sign_in(BASE, a["user"], a["password"])
            return {"principal": s.principal, "token": s.token}
        if a["kind"] == "signInWithKey":
            s = warehouse.sign_in_with_key(BASE, a["key"])
            return {"principal": s.principal, "token": s.token}
        if a["kind"] == "objects":
            return {"objects": warehouse.list_objects(session(a["token"]))}
        if a["kind"] == "run":
            receipt: dict = {}
            # the test plays the run side, which sends a plane's statement in a run (C5-P, 2026-11-15)
            with in_run("a test's run"):
                # a run's values, bound at run: the scenario's parameters carried as the statement API's (the host
                # train, 2026-11-17)
                chunks = [
                    base64.b64encode(c).decode()
                    for c in engine().arrow_chunks(PlannedSql(a["sql"]), receipt.update, a.get("parameters", ()))
                ]
            return {"chunks": chunks, "statementId": receipt.get("statementId"), "rowCount": receipt.get("serverRows")}
        if a["kind"] == "firstChunk":
            # the read stops after its first chunk: the driver closes what it leaves
            with in_run("a test's run"):
                read = engine().arrow_chunks(PlannedSql(a["sql"]))
                first = next(read)
                read.close()
            return {"chunks": [base64.b64encode(first).decode()]}
        if a["kind"] == "refreshThenCheck":
            # the new token is what the next call carries: the replay holds that call to the recorded one
            w = engine()
            w.refresh_token()
            return {"principal": w.principal, "token": w._session.token, "said": w.check("s1")}
        if a["kind"] == "history":
            return {"said": engine().check("s1")}
        raise AssertionError(f"no such action: {a['kind']}")
    except warehouse.SessionExpired:
        return {"expired": True}
    except warehouse.WarehouseError as e:
        return {"failed": str(e)}


@unittest.skipUnless(CORPUS, "the corpus is handed in by Bazel (LEGEND_LITE_SQLAPI_CORPUS)")
class TheCorpus(unittest.TestCase):
    """Every scenario: exactly its calls, and its outcome."""

    def test_every_scenario(self):
        with open(CORPUS, encoding="utf-8") as f:
            scenarios = json.load(f)["scenarios"]
        self.assertGreaterEqual(len(scenarios), 13)
        for scenario in scenarios:
            with self.subTest(scenario["name"]):
                replay = Replay(self, scenario)
                with mock.patch.object(warehouse, "_urlopen", replay):
                    got = act(scenario)
                self.assertEqual(
                    replay.next, len(scenario["exchanges"]), f"{scenario['name']}: every recorded call made"
                )
                want = scenario["outcome"]
                if "failed" in want:
                    self.assertTrue(got.get("failed", "").endswith(want["failed"]), got)
                elif scenario["action"]["kind"] == "history":
                    self.assertRegex(
                        got["said"], r"^On the warehouse's record for rita: statement s1, succeeded, 3 rows"
                    )
                else:
                    self.assertEqual(got, want)


class ParametersCarried(unittest.TestCase):
    """A run's values as the statement API carries them (the host train, 2026-11-17, the orchestrator's ruling (a)):
    positional, each a JSON string, a whole number of 64 bits, another number, a boolean or null -- bound by the server
    at run, never pasted into the statement's text; anything else refused by name before anything is sent."""

    def test_each_kind_of_value_is_carried_as_the_api_takes_it(self):
        import datetime
        from decimal import Decimal

        p = warehouse._parameter
        self.assertEqual(
            [p(v) for v in (3, "desk-a", 1.5, True, None, 2**63 - 1, -(2**63))],
            [3, "desk-a", 1.5, True, None, 2**63 - 1, -(2**63)],
        )
        self.assertIs(p(True), True)
        # a Decimal its exact text, a StrictDate its ISO text
        self.assertEqual((p(Decimal("12.30")), p(datetime.date(2024, 1, 2))), ("12.30", "2024-01-02"))

    def test_a_value_the_api_does_not_take_is_refused_by_name(self):
        for wrong, said in (
            (2**63, "64 bits"),
            (-(2**63) - 1, "64 bits"),
            ([1], "list"),
            ({"a": 1}, "dict"),
            (b"x", "bytes"),
        ):
            with self.subTest(wrong=wrong), self.assertRaisesRegex(warehouse.WarehouseError, said):
                warehouse._parameter(wrong)

    def test_a_statement_that_binds_nothing_sends_no_parameters(self):
        sent = []

        class Recording(warehouse.Warehouse):
            def _call(self, op, args, refresh=True):
                sent.append((op, args))
                if op == "submit":
                    return {"step": "failed", "error": {"code": "X", "message": "stop"}}
                return {}

        for params, want in (((), None), ((1, "a"), [1, "a"])):
            sent.clear()
            with in_run("a test's run"), self.assertRaises(warehouse.WarehouseError):
                list(Recording(session("t"), "main").arrow_chunks(PlannedSql("SELECT ?"), None, params))
            self.assertEqual(sent[0][1].get("parameters"), want)


class Addresses(unittest.TestCase):
    def test_a_warehouse_is_an_http_address(self):
        for address in ("file:///etc", "ftp://wh", "wh.example", "https://"):
            with self.assertRaisesRegex(warehouse.WarehouseError, "http:// or https://"):
                warehouse.sign_in(address, "u", "p")

    def test_an_unreachable_warehouse_is_said_as_one(self):
        def refused(request):
            raise urllib.error.URLError("connection refused")

        with mock.patch.object(warehouse, "_urlopen", refused):
            with self.assertRaisesRegex(warehouse.WarehouseError, "cannot reach the warehouse at https://wh.example"):
                warehouse.sign_in(BASE, "u", "p")

    def test_a_redirect_is_refused_never_followed(self):
        # urllib's own handler would carry the bearer token to wherever a redirect points: the driver's opener follows
        # none (a real request through it, not a replay)
        import http.server
        import threading

        reached = []

        class Elsewhere(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                reached.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.end_headers()

            def log_message(self, *_):
                pass

        elsewhere = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Elsewhere)
        target = f"http://127.0.0.1:{elsewhere.server_address[1]}/sql/v1/objects"

        class Redirecting(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(302)
                self.send_header("Location", target)
                self.end_headers()

            def log_message(self, *_):
                pass

        here = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Redirecting)
        for server in (elsewhere, here):
            threading.Thread(target=server.serve_forever, daemon=True).start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
        session = warehouse.Session(
            f"http://127.0.0.1:{here.server_address[1]}", "secret-token", "rita", "2099-01-01T00:00:00Z"
        )
        with self.assertRaisesRegex(warehouse.WarehouseError, "could not list the tables — INTERNAL: HTTP 302"):
            warehouse.list_objects(session)
        self.assertEqual(reached, [], "the redirect was not followed: the token went nowhere else")

    def test_another_users_session_is_refused(self):
        w = warehouse.Warehouse(session("t"))
        with self.assertRaisesRegex(warehouse.WarehouseError, "read as rita"):
            w.renew(warehouse.Session(BASE, "t2", "bob", "x"))


if __name__ == "__main__":
    unittest.main()
