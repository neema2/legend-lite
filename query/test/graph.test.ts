// Which mapping a query on a class opens on: a mapping maps the classes its included mappings map
// (upstream's reading), and with none given the query opens on one some runtime runs. DataCube's
// project is the case: each class its own mapping, a Model that includes them all, and a runtime
// that names only the Model (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.2).

import { strict as assert } from 'node:assert';
import { describe, it } from 'node:test';
import type { PElement, PureModelContextData } from '../../engine-client/src/legend/pmcd.ts';
import { ModelGraph } from '../src/model/graph.ts';

const cls = (name: string): PElement => ({ _type: 'class', package: 'p', name, properties: [] });
const classMapping = (name: string, mapped: string): PElement => ({
  _type: 'mapping', package: 'p::mapping', name,
  classMappings: [{ _type: 'pureInstance', class: `p::${mapped}`, root: true }],
});

const graph = new ModelGraph({
  _type: 'data',
  elements: [
    cls('Trade'), cls('Firm'),
    classMapping('TradeMapping', 'Trade'), classMapping('FirmMapping', 'Firm'),
    {
      _type: 'mapping', package: 'p::mapping', name: 'Model',
      includedMappings: [{ includedMapping: 'p::mapping::TradeMapping' }, { includedMapping: 'p::mapping::FirmMapping' }],
    },
    { _type: 'runtime', package: 'p::runtime', name: 'Browser', runtimeValue: { _type: 'engineRuntime', mappings: [{ path: 'p::mapping::Model' }] } },
  ],
} satisfies PureModelContextData);

describe('the mapping a class query opens on', () => {
  it('counts a mapping that only includes the class mapping', () => {
    assert.deepEqual(graph.mappingsFor('p::Trade'), ['p::mapping::Model', 'p::mapping::TradeMapping']);
    assert.deepEqual(graph.mappingsFor('p::Firm'), ['p::mapping::FirmMapping', 'p::mapping::Model']);
  });

  it('opens on the mapping a runtime runs, with its runtime', () => {
    for (const c of ['p::Trade', 'p::Firm']) {
      const mapping = graph.defaultMappingFor(c);
      assert.equal(mapping, 'p::mapping::Model');
      assert.deepEqual(graph.runtimesFor(mapping!), ['p::runtime::Browser']);
    }
  });

  it('falls back to the first mapping when no runtime runs any', () => {
    const bare = new ModelGraph({ _type: 'data', elements: [cls('Trade'), classMapping('TradeMapping', 'Trade')] });
    assert.equal(bare.defaultMappingFor('p::Trade'), 'p::mapping::TradeMapping');
    assert.equal(bare.defaultMappingFor('p::Nothing'), undefined);
  });
});
