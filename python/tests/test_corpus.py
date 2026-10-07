"""The planner differential corpus (wasm/corpus: every query, refusals included) through the NATIVE
library, answer for answer against the JVM's (//wasm:jvm_answers) -- which the WebAssembly build is
held to as well (//wasm:differential_test). Three builds of one source, one answer."""

import os
import re
import unittest
from pathlib import Path

from legend_lite._library import library


class Corpus(unittest.TestCase):
    def test_every_answer_is_the_jvms(self):
        model = Path(os.environ['LEGEND_LITE_CORPUS_MODEL']).read_text()
        queries = [l.split('\t', 1) for l in Path(os.environ['LEGEND_LITE_CORPUS_QUERIES']).read_text().split('\n') if l]
        jvm = dict(re.findall(r'<<<([^>]+)>>>\n([\s\S]*?)\n<<<END>>>\n', Path(os.environ['LEGEND_LITE_JVM_ANSWERS']).read_text()))
        self.assertGreater(len(queries), 50, 'the corpus is not there')
        differ = []
        refusals = 0
        for name, q in queries:
            got = library().call('lite_plan_text', model, q, 'trades::RT')
            refusals += got.startswith('ERR')
            if got != jvm.get(name):
                differ.append(f'{name}:\n  jvm:    {jvm.get(name, "(none)")[:200]}\n  native: {got[:200]}')
        self.assertEqual(differ, [], '\n'.join(differ))
        self.assertGreater(refusals, 0, 'the corpus holds refusals too: none came back as one')
        print(f'{len(queries)} answers identical to the JVM, {refusals} of them refusals')


if __name__ == '__main__':
    unittest.main()
