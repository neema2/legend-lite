/**
 * Copyright (c) 2026-present, Goldman Sachs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { describe, expect, test } from '@jest/globals';
import { type PlainObject } from '@finos/legend-shared';
import { DataCubeFunction } from '@finos/legend-data-cube';
import {
  hasCastlessPivot,
  isQuerySupportedByLegendLite,
} from '../LegendLiteQuerySupport.js';

const TEST__func = (name: string, parameters: unknown[]): PlainObject => ({
  _type: 'func',
  function: name,
  parameters,
});
const TEST__lambda = (body: PlainObject): PlainObject => ({
  _type: 'lambda',
  body: [body],
  parameters: [],
});
const TEST__SOURCE = { _type: 'classInstance', type: '>', value: {} };
const TEST__COLS = { _type: 'classInstance', type: 'colSpecArray', value: {} };

describe('legend-lite query support: pivots', () => {
  const pivot = TEST__func('pivot', [TEST__SOURCE, TEST__COLS, TEST__COLS]);
  const pivotDiscovery = TEST__lambda(
    TEST__func('from', [
      TEST__func('limit', [pivot, { _type: 'integer', value: 0 }]),
      { _type: 'packageableElementPtr', fullPath: 'local::rt' },
    ]),
  );
  const pivotWithCast = TEST__lambda(
    TEST__func('from', [
      TEST__func('groupBy', [
        TEST__func('cast', [pivot, { _type: 'genericTypeInstance' }]),
        TEST__COLS,
        TEST__COLS,
      ]),
      { _type: 'packageableElementPtr', fullPath: 'local::rt' },
    ]),
  );
  const noPivot = TEST__lambda(
    TEST__func('from', [
      TEST__func('filter', [TEST__SOURCE, TEST__lambda(TEST__SOURCE)]),
      { _type: 'packageableElementPtr', fullPath: 'local::rt' },
    ]),
  );

  test('a pivot that is not followed by a cast is found, and the query stays with the engine', () => {
    expect(hasCastlessPivot(pivotDiscovery)).toBe(true);
    expect(isQuerySupportedByLegendLite(pivotDiscovery)).toBe(false);
  });

  test('a pivot followed by a cast is not, and the query goes to legend-lite', () => {
    expect(hasCastlessPivot(pivotWithCast)).toBe(false);
    expect(isQuerySupportedByLegendLite(pivotWithCast)).toBe(true);
  });

  test('a query with no pivot goes to legend-lite', () => {
    expect(hasCastlessPivot(noPivot)).toBe(false);
    expect(isQuerySupportedByLegendLite(noPivot)).toBe(true);
  });

  test('functions are matched by full path as well as by name', () => {
    expect(
      hasCastlessPivot(
        TEST__lambda(
          TEST__func(DataCubeFunction.PIVOT, [TEST__SOURCE, TEST__COLS]),
        ),
      ),
    ).toBe(true);
    expect(
      hasCastlessPivot(
        TEST__lambda(
          TEST__func(DataCubeFunction.CAST, [
            TEST__func(DataCubeFunction.PIVOT, [TEST__SOURCE, TEST__COLS]),
            { _type: 'genericTypeInstance' },
          ]),
        ),
      ),
    ).toBe(false);
  });

  test('a cast further out does not count', () => {
    expect(
      hasCastlessPivot(
        TEST__lambda(
          TEST__func('cast', [
            TEST__func('limit', [pivot, { _type: 'integer', value: 0 }]),
            { _type: 'genericTypeInstance' },
          ]),
        ),
      ),
    ).toBe(true);
  });
});
