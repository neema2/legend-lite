// The locale the apps' own words are written in (Bazel workplan P3-29): a row count or a clock time in a message reads
// the same on every machine, whatever the browser's or the host's locale. A column's display format names its own
// locale (datacube/src/format.ts); this is only for the text the apps write themselves.
export const UI_LOCALE = 'en-US';
