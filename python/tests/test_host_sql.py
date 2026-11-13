"""THE HOST-SQL GUARD, its Python half (tools/guards/host_sql.txt says the rule; docs/ONE_PLATFORM_PROGRAM_2026_10_12.md
D5): every module of Python's host (python/legend_lite) read by Python's own parser, and each string constant -- a
plain string, each part of an f-string's text -- held to the rule. Comments are not in the tree and docstrings are
skipped, so prose about SQL is free. Its TypeScript half is tools/guards/HostSqlTest.java."""

import ast
import json
import os
import re
import unittest
from pathlib import Path

from python.runfiles import runfiles

FILES = runfiles.Create()


def rule() -> dict:
    """host_sql.txt's sections, as HostSqlTest.java reads them."""
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in Path(FILES.Rlocation(os.environ['HOST_SQL_RULE'])).read_text(encoding='utf-8').splitlines():
        if not line.strip() or line.startswith('#'):
            continue
        if line.startswith('[') and line.endswith(']'):
            current = sections.setdefault(line[1:-1], [])
        elif current is not None:
            current.append(line.strip())
    keywords = [k for line in sections['keywords'] for k in line.split()]
    either = (r'\s*(?:(?:' + '|'.join(keywords) + r')\s|'
              + '|'.join(f'(?:{s})' for s in sections['shapes']) + ')')
    # a kept site's text is written in double quotes, the literal exactly
    kept = [dict(zip(('file', 'text', 'reason'), (p.strip() for p in line.split(' | ', 2)))) for line in sections['kept']]
    kept = [{**k, 'text': json.loads(k['text'])} for k in kept]
    return {'python': sections['python'], 'statement': re.compile(either), 'kept': kept}


def literals(source: str) -> list[tuple[int, str]]:
    """Each string constant of a module, with its line: plain strings and each part of an f-string's text; a module's,
    a class's or a function's docstring left out."""
    tree = ast.parse(source)
    docstrings = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)) and node.body:
            first = node.body[0]
            if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
                docstrings.add(id(first.value))
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and id(node) not in docstrings:
            out.append((node.lineno, node.value))
    return sorted(out)


class HostSqlTest(unittest.TestCase):

    def test_a_statement_in_a_string_is_found_never_in_a_comment_or_a_docstring(self):
        r = rule()
        source = '\n'.join([
            '"""SELECT * FROM T: a module\'s docstring about SQL."""',
            '# CREATE TABLE t (x INT)',
            'def f(name):',
            '    """DROP TABLE t: a function\'s docstring."""',
            "    a = 'SELECT 1'",
            "    b = f'DROP TABLE IF EXISTS {name}'",
            "    c = 'select x from y'",
            "    d = {'label': 'Select a column', 'tag': 'SELECT', 'state': 'COMMITTED', 'kind': 'SELECTED'}",
            "    e = f'{name} and then SELECT 2'",
            "    g = f'{name}SELECT 1 FROM t'",
            "    h = ['install ' + name, 'load ' + name, 'set threads = 4', 'pragma platform', 'use \"db\"']",
            "    i = ['Install the app', 'load more rows', 'set', 'use the filter', 'begin']",
            "    return ('USE ' + name)",
        ])
        self.assertEqual([line for line, text in literals(source) if r['statement'].match(text)],
                         [5, 6, 7, 10, 11, 11, 11, 11, 11, 13])

    def test_no_module_of_pythons_host_spells_sql_but_the_sites_kept(self):
        spells_sql_but_the_sites_kept(self, package_modules(self))

    def test_the_census_fails_on_a_planted_statement(self):
        """CHECK 6 (the final audit's item 5, 2026-11-12): the census's own comparison, run over the package with one
        module planted beside its modules, FAILS naming it -- never merely that its reader finds the literal."""
        planted = 'planted_violator.py'
        self.maxDiff = None
        with self.assertRaises(AssertionError, msg='the census passed over a planted statement') as failed:
            spells_sql_but_the_sites_kept(self, {**package_modules(self), planted: "q = 'SELECT * FROM trades'"})
        self.assertIn(planted, str(failed.exception))


def package_modules(case: unittest.TestCase) -> dict[str, str]:
    """The package's modules, beside its __init__.py: each its text by its file's name."""
    package = Path(FILES.Rlocation(os.environ['HOST_SQL_PACKAGE'])).parent
    modules = sorted(package.glob('*.py'))
    case.assertGreater(len(modules), 10, f'{package} holds {len(modules)} modules: the guard is not looking')
    return {m.name: m.read_text(encoding='utf-8') for m in modules}


def spells_sql_but_the_sites_kept(case: unittest.TestCase, modules: dict[str, str]) -> None:
    """The census over ``modules`` (each module's text by its file's name), compared with the sites kept."""
    r = rule()
    found, used = [], []
    for module_name, module_text in sorted(modules.items()):
        name = f'{r["python"][0]}/{module_name}'
        for line, text in literals(module_text):
            if not r['statement'].match(text):
                continue
            kept = next((k for k in r['kept'] if k['file'] == name and text == k['text']), None)
            if kept is None:
                found.append(f'{name}:{line}: {text[:80]!r}')
            else:
                used.append(kept)
    case.assertEqual(found, [], "SQL spelled in Python's host: name the operation and its data instead "
                                "(legend_lite/_host.py), which the native library writes by the database's type")
    stale = [f"{k['file']}: {k['text']}" for k in r['kept'] if k['file'].endswith('.py') and k not in used]
    case.assertEqual(stale, [], 'a site kept is no longer there: take it out of host_sql.txt')


if __name__ == '__main__':
    unittest.main()
