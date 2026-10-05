// Rename or move an element in text (plan A7): its declaration and its references by full path, nothing else -- and the
// renamed workspace still reads as the same model with the new path, by the real grammar.

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { renamePath } from '../src/model/rename.ts';
import { grammar } from './modules.ts';

const DESK = "// demo::trading::Desk, the desk\nClass demo::trading::Desk\n{\n  name: String[1];\n  note: String[1] = 'demo::trading::Desk';\n}\n";
const USES = 'Class demo::trading::Book\n{\n  desk: demo::trading::Desk[1];\n  desks: demo::trading::Desks[*];\n  inner: demo::trading::Desk::Kind[0..1];\n  other: x::demo::trading::Desk[1];\n}\n';

describe('renaming an element in text', () => {
  it('rewrites its declaration and its references by full path, not comments, strings or longer paths', () => {
    assert.equal(renamePath(DESK, 'demo::trading::Desk', 'demo::desks::TradingDesk'),
      "// demo::trading::Desk, the desk\nClass demo::desks::TradingDesk\n{\n  name: String[1];\n  note: String[1] = 'demo::trading::Desk';\n}\n");
    assert.equal(renamePath(USES, 'demo::trading::Desk', 'demo::desks::TradingDesk'),
      'Class demo::trading::Book\n{\n  desk: demo::desks::TradingDesk[1];\n  desks: demo::trading::Desks[*];\n  inner: demo::trading::Desk::Kind[0..1];\n  other: x::demo::trading::Desk[1];\n}\n');
  });

  it('leaves a model the grammar reads with the element at its new path', async () => {
    const model = await grammar.modelJson([DESK, USES.replace(/\n  (desks|inner|other):.*/g, '')].map((t) => renamePath(t, 'demo::trading::Desk', 'demo::desks::TradingDesk')).join('\n'));
    const paths = model.elements.filter((e) => e._type === 'class').map((e) => `${e.package}::${e.name}`).sort();
    assert.deepEqual(paths, ['demo::desks::TradingDesk', 'demo::trading::Book']);
  });
});
