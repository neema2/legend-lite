// What a harness compares: the view's cells AS THE DATABASE RETURNED THEM, typed by the
// compiler (docs/DATACUBE_TYPES_TO_SERVER_2026_09_27.md, T8).
//
// A figure parsed back out of a rendered cell is not a figure: `(1,234.00)` loses its sign to
// a digit strip, a `k` scale or a unit corrupts it, a Float rounded to two places collides
// with its neighbour, and a date shown as "Mar 01, 2024" does not sort. So a check reads the
// typed result (`window.__dataCube`) and compares by the column's compiler type -- exactly
// for an Integer or a Decimal (a bigint, decimal text), within a relative tolerance only for
// a Float. Rendered text is compared only where the claim IS the rendering.

import { isBoolean, isNumeric, plainType } from '../../engine-client/src/types.ts';
import { asDecimal, exactSum } from '../../engine-client/src/values.ts';

/**
 * The on-screen table, in grid order: each column's name, compiler type and exact values.
 * The Ad Hoc table in that mode, else the cube's view. A bigint crosses the page boundary as
 * `{ bigint: '…' }` and comes back a bigint.
 */
export async function readView(page) {
  const columns = await page.evaluate(() => {
    const app = window.__dataCube;
    const table = app.adhoc ? app.adhoc.view?.table : app.view?.rows;
    const leaves = app.adhoc ? null : app.view?.columns.leaves;
    const all = table?.columns ?? [];
    const shown = leaves ? leaves.map((l) => all[l.index]).filter(Boolean) : all;
    return shown.map((c) => ({
      name: c.name,
      type: c.type,
      values: c.values.map((v) => (typeof v === 'bigint' ? { bigint: String(v) } : v)),
    }));
  });
  return columns.map((c) => ({
    ...c,
    values: c.values.map((v) => (v !== null && typeof v === 'object' && 'bigint' in v ? BigInt(v.bigint) : v)),
  }));
}

/** One column of the on-screen table, by name; an error naming what is there when absent. */
export async function readColumn(page, name) {
  const view = await readView(page);
  const column = view.find((c) => c.name === name);
  if (!column) throw new Error(`no column ${name} on screen; have ${view.map((c) => c.name).join(', ')}`);
  return column;
}

/** A Float: a double, compared within a relative tolerance. Every other number is exact. */
const isFloat = (type) => isNumeric(type) && ['Float', 'Number'].includes(plainType(type));

/** Two numeric cells' order, exactly: -1, 0 or 1. */
function compareExact(a, b) {
  const x = asDecimal(a);
  const y = asDecimal(b);
  if (x === null || y === null) throw new Error(`not numbers: ${String(a)}, ${String(b)}`);
  const scale = Math.max(x.scale, y.scale);
  const l = x.unscaled * 10n ** BigInt(scale - x.scale);
  const r = y.unscaled * 10n ** BigInt(scale - y.scale);
  return l < r ? -1 : l > r ? 1 : 0;
}

/**
 * Two non-null cells of one column in the DATABASE's order, by the column's compiler type:
 * numbers by value (exactly, a Float as a double), a boolean false before true, and text,
 * dates and timestamps by their code points -- DuckDB's binary collation, and the order of
 * the ISO text a date and a timestamp are held in.
 */
export function compareTyped(a, b, type) {
  if (isFloat(type)) return Math.sign(Number(a) - Number(b));
  if (isNumeric(type)) return compareExact(a, b);
  if (isBoolean(type)) return Number(a) - Number(b);
  const x = String(a);
  const y = String(b);
  return x < y ? -1 : x > y ? 1 : 0;
}

/** Whether two cells of a column hold the same value, by its type (a Float within 1e-9). */
export function sameTyped(a, b, type) {
  if (a === null || b === null) return a === b;
  if (isFloat(type)) {
    const x = Number(a);
    const y = Number(b);
    return Math.abs(x - y) <= 1e-9 * Math.max(1, Math.abs(x), Math.abs(y));
  }
  return compareTyped(a, b, type) === 0;
}

/**
 * Whether two figures agree within a relative tolerance: for a Float computed along two
 * different paths (a pivot's total against the unpivoted sum). An exact type compares exactly.
 */
export function closeTyped(a, b, type, tolerance = 1e-9) {
  if (a === null || b === null) return a === b;
  if (!isFloat(type)) return sameTyped(a, b, type);
  const x = Number(a);
  const y = Number(b);
  return Math.abs(x - y) <= tolerance * Math.max(1, Math.abs(x), Math.abs(y));
}

/** The first index where a column's non-null values break `direction` order; -1 when none. */
export function orderBreak(values, type, direction = 'asc') {
  const sign = direction === 'asc' ? 1 : -1;
  const present = values.filter((v) => v !== null);
  for (let i = 1; i < present.length; i++) {
    if (sign * compareTyped(present[i - 1], present[i], type) > 0) return i;
  }
  return -1;
}

/** The sum of numeric cells: exact decimal text for an exact type, a double for a Float. */
export function sumTyped(values, type) {
  const present = values.filter((v) => v !== null);
  return isFloat(type) ? present.reduce((t, v) => t + Number(v), 0) : exactSum(present);
}

/** Whether a numeric cell is below zero, by value. */
export function isNegative(v, type) {
  if (v === null || !isNumeric(type)) return false;
  return isFloat(type) ? Number(v) < 0 : compareExact(v, 0) < 0;
}

/** JSON of typed values that survives a bigint (JSON.stringify throws on one). */
export function stamp(value) {
  return JSON.stringify(value, (_k, v) => (typeof v === 'bigint' ? `${v}n` : v));
}
