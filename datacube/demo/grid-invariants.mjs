// What must be true of the grid in EVERY state.
//
// Every fault the user found by hand was an invariant, not a feature:
// headers that did not move with their columns, a flat grid with no
// headers at all, headers that vanished on grouping, a grouping that
// produced no GROUP BY. A per-feature check cannot see those, because
// each is a property that has to hold after every operation rather
// than the result of one operation.
//
// So this is a single browser-side function, shared by every harness
// and run after every state change. Thirty operations times these
// assertions is most of the coverage for a fraction of the code.
//
// It runs inside the page, so it must not close over anything in Node.

/**
 * @returns {string[]} one message per broken invariant, empty if sound
 */
export function gridInvariants() {
  const bad = [];
  const round = (n) => Math.round(n);
  const root = document.querySelector('.dc-grid');
  if (!root) return ['there is no grid at all'];

  // 1. The grid occupies real space. A container collapsed to nothing
  //    lays every cell out correctly and shows none of them.
  const box = root.getBoundingClientRect();
  if (box.width < 200 || box.height < 50) {
    bad.push(`the grid is ${round(box.width)}x${round(box.height)}`);
  }

  // 2. Headers are VISIBLE, not merely present.
  //
  //    THE CELLS ARE THE WRONG THING TO MEASURE, and the first
  //    version of this made exactly that mistake. When `flex-shrink`
  //    crushed the header to 0.015625px, the cells inside it stayed
  //    24px tall and correctly laid out -- they were CLIPPED by
  //    `overflow: hidden` on a container of no height. So every cell
  //    measured fine and a forced 1px header sailed through.
  //
  //    The container against its content is what shows it: a header
  //    box shorter than the cells it holds is a header nobody can
  //    read. Zero-width cells are still worth catching separately,
  //    since a collapsed column clips in the other axis.
  const ths = [...document.querySelectorAll('.dc-th')];
  if (!ths.length) bad.push('no header cells exist');
  const head = document.querySelector('.dc-head');
  if (ths.length && head) {
    const headH = head.getBoundingClientRect().height;
    const cellH = Math.max(
      ...ths.map((e) => e.getBoundingClientRect().height),
    );
    if (headH + 1 < cellH) {
      bad.push(`the header box is ${round(headH)}px but its cells are`
        + ` ${round(cellH)}px — laid out and then clipped`);
    }
    if (headH < 10) {
      bad.push(`the header box is only ${round(headH)}px tall`);
    }
  }
  const flat = ths.filter((e) => e.getBoundingClientRect().width < 1);
  if (flat.length) {
    bad.push(`${flat.length}/${ths.length} header cells have no width`);
  }

  // 3. Headers carry LABELS. A pivot's tree column is deliberately
  //    blank, so one is allowed; every header going blank is the
  //    "grouping lost the headers" fault.
  const blank = ths.filter((e) => (e.textContent ?? '').trim() === '');
  if (ths.length && blank.length > 1) {
    bad.push(`${blank.length}/${ths.length} header cells are blank`);
  }

  // 4. Rows are RECTANGULAR. A ragged row means the column model and
  //    the row renderer disagree about how many columns there are.
  const rows = [...document.querySelectorAll('.dc-row')];
  const widths = new Set(
    rows.map((r) => r.querySelectorAll('.dc-cell').length),
  );
  if (rows.length && widths.size > 1) {
    bad.push(`rows have ${[...widths].join('/')} cells — ragged`);
  }
  if (rows.length && widths.has(0)) bad.push('some rows have no cells');

  // 5. Cells hold RENDERED values. A date arrives as epoch
  //    milliseconds and only date-formats if the formatter is handed a
  //    Date, so the failure mode is a 13-digit number or "Invalid
  //    Date"; a value object reaching textContent prints
  //    "[object Object]".
  //    The digit rule is scoped to TEMPORAL columns by declared type,
  //    because it fired on the "numbers at the edges" sample, where a
  //    nineteen-digit integer is the point of the data rather than an
  //    unformatted date. An invariant that cries wolf on valid data
  //    gets switched off, and then it is not an invariant.
  const typeOf = new Map();
  for (const r of document.querySelectorAll('.dc-tool-panel-row')) {
    typeOf.set(r.dataset.column,
      (r.querySelector('.dc-tool-panel-type')?.textContent ?? '').trim());
  }
  const temporal = (name) => /Date|Time/.test(typeOf.get(name) ?? '');
  const junk = [];
  for (const r of rows.slice(0, 12)) {
    for (const c of r.querySelectorAll('.dc-cell')) {
      const t = (c.textContent ?? '').trim();
      if (/Invalid Date|\[object |^undefined$|^NaN$/.test(t)) {
        junk.push(t.slice(0, 24));
        continue;
      }
      const name = c.dataset.column
        ?? c.closest('[data-column]')?.dataset.column;
      // Epoch milliseconds shown raw: 13 digits in a date column.
      if (temporal(name) && /^[\d,]{13,}$/.test(t)) junk.push(t.slice(0, 24));
    }
  }
  if (junk.length) {
    bad.push(`unrendered values in cells: ${JSON.stringify(junk.slice(0, 3))}`);
  }

  // 6. The PAGE does not scroll sideways. Wide content belongs in the
  //    grid's own scroller; escaping it moves the whole application.
  const de = document.documentElement;
  if (de.scrollWidth > de.clientWidth + 1) {
    bad.push(`the page scrolls horizontally`
      + ` (${de.scrollWidth} > ${de.clientWidth})`);
  }

  // 7. EVERY BODY COLUMN HAS A HEADER AT THE SAME x.
  //
  //    The one that pays for the file. A missing header, a misaligned
  //    header, a header that stopped following a horizontal scroll,
  //    and a grouping that dropped a column are all this one property
  //    failing -- four faults found by hand, one assertion.
  const headAt = new Map();
  for (const e of document.querySelectorAll('.dc-th[data-column]')) {
    headAt.set(e.dataset.column, round(e.getBoundingClientRect().left));
  }
  const first = rows[0];
  if (first) {
    for (const c of first.querySelectorAll('.dc-cell')) {
      const name = c.dataset.column
        ?? c.closest('[data-column]')?.dataset.column;
      if (name === undefined) continue;
      const hx = headAt.get(name);
      if (hx === undefined) {
        bad.push(`column ${JSON.stringify(name)} has no header`);
        continue;
      }
      const cx = round(c.getBoundingClientRect().left);
      if (Math.abs(hx - cx) > 2) {
        bad.push(`column ${JSON.stringify(name)} header at ${hx},`
          + ` its cells at ${cx}`);
      }
    }
  }

  // 9. THE HEADER OVER A COLUMN CLAIMS THAT COLUMN.
  //
  //    By POSITION, not by label, so a column with a custom display
  //    label does not trip it. Every body cell's `data-column` must
  //    match the `data-column` of whatever header sits at the same x.
  //
  //    This is the identity half of invariant 7, and it has to be
  //    checked separately because the two can disagree while the grid
  //    looks perfect: after hiding a column, every header to the
  //    right kept its correct label and correct position but carried
  //    its NEIGHBOUR's `data-column`. So sorting the column headed
  //    `booked_at` sorted `region`, and dragging it grouped by
  //    `region`. Nothing was visibly wrong; every action was one
  //    column across.
  if (first) {
    const headByX = new Map();
    for (const e of document.querySelectorAll('.dc-th[data-column]')) {
      headByX.set(round(e.getBoundingClientRect().left), e.dataset.column);
    }
    for (const c of first.querySelectorAll('.dc-cell')) {
      const name = c.dataset.column
        ?? c.closest('[data-column]')?.dataset.column;
      if (name === undefined) continue;
      const claimed = headByX.get(round(c.getBoundingClientRect().left));
      if (claimed !== undefined && claimed !== name) {
        bad.push(`the header over ${JSON.stringify(name)} claims to be`
          + ` ${JSON.stringify(claimed)}`);
      }
    }
  }

  // 8. The status line is not reporting a failure. An action that
  //    refused says so there, and leaving it red while carrying on is
  //    how a wedged cube went unnoticed for a whole session.
  const status = document.getElementById('status');
  if (status?.classList.contains('bad')) {
    // WITH THE QUERY THAT FAILED. "unknown table 'TRADES'" says
    // something went to the wrong model; which Pure went with it is
    // the difference between a five-minute answer and an hour.
    const pure = (document.getElementById('pure')?.textContent ?? '')
      .replace(/\s+/g, ' ').slice(0, 90);
    bad.push(`the status line reads:`
      + ` ${(status.textContent ?? '').slice(0, 70)}`
      + (pure ? ` (last Pure: ${pure})` : ''));
  }
  return bad;
}
