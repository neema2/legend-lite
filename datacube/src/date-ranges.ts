// DATE RANGES RELATIVE TO TODAY (Excel's date filters, Power BI's relative date slicer): "this month", "last quarter",
// "year to date", "the last 30 days" -- each the two conditions `from <= column < to` with ends relative to when the
// query RUNS (snapshot.ts RelativeDate: today(), moved and taken to its period's start by the compiler's adjust and
// firstDayOf*), so a page saved filtered to "last month" shows last month whenever it is opened. One rule for the
// header menu's Date Range and Python's grid.filter_date_range (grid-gestures.ts 'filter.dateRange'); the dates are the
// database's to compute, never the tab's.

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
