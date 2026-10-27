// DATE RANGES RELATIVE TO TODAY (Excel's date filters, Power BI's relative date slicer): "this month", "last quarter",
// "year to date", "the last 30 days" -- each the two conditions `from <= column < to` with ends relative to when the
// query RUNS (snapshot.ts RelativeDate: today(), moved and taken to its period's start by the compiler's adjust and
// firstDayOf*), so a page saved filtered to "last month" shows last month whenever it is opened. One rule for the
// header menu's Date Range and Python's grid.filter_date_range (grid-gestures.ts 'filter.dateRange'); the dates are the
// database's to compute, never the tab's.

import { hasTimeOfDay, isTimeOfDay } from '../../engine-client/src/types.ts';
import { CubeRefusal, type FilterNode, type RelativeDate } from './snapshot.ts';

export const DATE_RANGES = [
  'today', 'yesterday', 'thisWeek', 'lastWeek', 'thisMonth', 'lastMonth', 'thisQuarter', 'lastQuarter', 'thisYear',
  'lastYear', 'weekToDate', 'monthToDate', 'quarterToDate', 'yearToDate', 'last7Days', 'last30Days', 'last90Days',
  'last12Months',
] as const;
export type DateRange = (typeof DATE_RANGES)[number];

/** Words for each range, as the menu says them. */
export const DATE_RANGE_LABELS: Readonly<Record<DateRange, string>> = {
  today: 'Today', yesterday: 'Yesterday', thisWeek: 'This Week', lastWeek: 'Last Week', thisMonth: 'This Month',
  lastMonth: 'Last Month', thisQuarter: 'This Quarter', lastQuarter: 'Last Quarter', thisYear: 'This Year',
  lastYear: 'Last Year', weekToDate: 'Week to Date', monthToDate: 'Month to Date', quarterToDate: 'Quarter to Date',
  yearToDate: 'Year to Date', last7Days: 'Last 7 Days', last30Days: 'Last 30 Days', last90Days: 'Last 90 Days',
  last12Months: 'Last 12 Months',
};

const TODAY: RelativeDate = { relative: 'today' };
const TOMORROW: RelativeDate = { relative: 'today', offset: 1, unit: 'DAYS' };
const start = (of: NonNullable<RelativeDate['startOf']>, offset = 0, unit?: RelativeDate['unit']): RelativeDate =>
  ({ relative: 'today', startOf: of, ...(offset !== 0 && unit ? { offset, unit } : {}) });
const back = (n: number, unit: NonNullable<RelativeDate['unit']>): RelativeDate => ({ relative: 'today', offset: -n, unit });

/** Each range's ends: from (included) and to (left out). */
const ENDS: Readonly<Record<DateRange, readonly [RelativeDate, RelativeDate]>> = {
  today: [TODAY, TOMORROW],
  yesterday: [back(1, 'DAYS'), TODAY],
  thisWeek: [start('week'), start('week', 1, 'WEEKS')],
  lastWeek: [start('week', -1, 'WEEKS'), start('week')],
  thisMonth: [start('month'), start('month', 1, 'MONTHS')],
  lastMonth: [start('month', -1, 'MONTHS'), start('month')],
  thisQuarter: [start('quarter'), start('quarter', 3, 'MONTHS')],
  lastQuarter: [start('quarter', -3, 'MONTHS'), start('quarter')],
  thisYear: [start('year'), start('year', 1, 'YEARS')],
  lastYear: [start('year', -1, 'YEARS'), start('year')],
  weekToDate: [start('week'), TOMORROW],
  monthToDate: [start('month'), TOMORROW],
  quarterToDate: [start('quarter'), TOMORROW],
  yearToDate: [start('year'), TOMORROW],
  last7Days: [back(6, 'DAYS'), TOMORROW],
  last30Days: [back(29, 'DAYS'), TOMORROW],
  last90Days: [back(89, 'DAYS'), TOMORROW],
  last12Months: [back(12, 'MONTHS'), TOMORROW],
};

/** The conditions keeping `column`'s dates in `range`: from it (included) to its end (left out). */
export function dateRangeConditions(column: string, range: DateRange): FilterNode[] {
  const ends = ENDS[range];
  if (ends === undefined) throw new CubeRefusal(`a date range is ${DATE_RANGES.join(', ')}, not ${JSON.stringify(range)}`);
  return [
    { kind: 'condition', column, operator: 'greaterThanEqual', value: ends[0] },
    { kind: 'condition', column, operator: 'lessThan', value: ends[1] },
  ];
}

/** A day as a filter writes one: `2024-06-30`, a real day of its month. */
const DAY = /^(\d{4})-(\d{2})-(\d{2})$/;
export function isDay(text: string): boolean {
  const m = DAY.exec(text);
  if (m === null) return false;
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  return d.getUTCFullYear() === Number(m[1]) && d.getUTCMonth() === Number(m[2]) - 1 && d.getUTCDate() === Number(m[3]);
}

/** A day `by` days on (or back): the calendar's, the day as it is written (YYYY-MM-DD). */
export function dayOn(day: string, by: number): string {
  const m = DAY.exec(day);
  if (m === null || !isDay(day)) throw new CubeRefusal(`a day is written as 2024-06-30, not ${JSON.stringify(day)}`);
  return new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]) + by)).toISOString().slice(0, 10);
}

/** A day's first moment, as a date-time column's value is written (`2024-06-30T00:00:00`). */
export const MIDNIGHT = 'T00:00:00';

/**
 * THE DAYS FROM ONE TO ANOTHER, both kept (a flow's Date Filter between two days, Python's grid.filter_dates), each a
 * day (YYYY-MM-DD), the first not after the last -- refused in words otherwise. A column of days (`type` a StrictDate or
 * Date, or not given): `from <= column <= to`. A date-time column: every moment of both, `from`'s midnight `<= column <`
 * the next day's after `to` (the correctness audit's 5, 2026-10-26: a day compared with a moment was no literal of its
 * type, and `<= to` left out every moment of the last day after its midnight). A time of day has no days: refused.
 */
export function datesBetweenConditions(column: string, from: string, to: string, type: string | undefined): FilterNode[] {
  for (const [day, what] of [[from, 'its first day'], [to, 'its last day']] as const) {
    if (!isDay(day)) throw new CubeRefusal(`${what} is a day, as 2024-06-30, not ${JSON.stringify(day)}`);
  }
  if (from > to) throw new CubeRefusal(`its first day, ${from}, is after its last, ${to}`);
  if (isTimeOfDay(type)) throw new CubeRefusal(`'${column}' is a time of day: it has no days to keep`);
  if (hasTimeOfDay(type)) {
    return [
      { kind: 'condition', column, operator: 'greaterThanEqual', value: `${from}${MIDNIGHT}` },
      { kind: 'condition', column, operator: 'lessThan', value: `${dayOn(to, 1)}${MIDNIGHT}` },
    ];
  }
  return [
    { kind: 'condition', column, operator: 'greaterThanEqual', value: from },
    { kind: 'condition', column, operator: 'lessThanEqual', value: to },
  ];
}
