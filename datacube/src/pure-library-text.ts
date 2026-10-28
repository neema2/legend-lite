// DATACUBE'S PURE LIBRARY, its text (pure-library.ts says what it is and how a model takes it): a module of its own, so a
// page fetches it only where a query calls the library (pure-library.ts loadLibrary, a chunk of its own; the bundle
// budget, 2026-10-27) -- a timeseries tile's, or a person's own query naming datacube::ts.

/**
 * The library, as Pure text: a `###Pure` section a model takes after its own. Each function takes the table, a window
 * of it (its LINE -- the column the table has a line per value of: a symbol --, its ORDER, its frame) or the line and
 * the dates themselves, the column it reads (by `read`: the caller's own), its parameters and the column it writes;
 * every other column carried as it is, its working columns its own (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.7).
 */
export const DATACUBE_LIBRARY = `###Pure
// DataCube's timeseries functions (docs/DATACUBE_TIMESERIES_DESIGN_2026_10_25.md §5.6, §5.7): gs-quant's, over any
// table. A function takes the table, a window -- its line, its order and its frame: over(~symbol, ~day->ascending(),
// rows(-19, 0)) -- or the line and the dates where it joins the line to itself, the column it reads (read: the caller's
// own, never renamed), its parameters and the column it writes; it declares what it answers, and its working columns
// are its own.

// A function's options, each an enumeration (the user, 2026-10-28: a parameter checked when the query compiles, never
// in the SQL): a value the enumeration has not is refused by the type checker, its name said -- gs-quant's own way
// (Returns.LOGARITHMIC, SeasonalModel.ADDITIVE, RebalFreq.WEEKLY).
Enum datacube::ts::Statistic
{
  mean, sum, min, max, range, std, var, count
}

Enum datacube::ts::PairStatistic
{
  corr, cov
}

Enum datacube::ts::Returns
{
  simple, log, absolute
}

Enum datacube::ts::FilterOperator
{
  empty, less_than, greater_than, l_equals, g_equals, equals, not_equals
}

Enum datacube::ts::ThresholdType
{
  percentage, absolute
}

Enum datacube::ts::Direction
{
  positive, negative
}

Enum datacube::ts::SeasonalModel
{
  additive, multiplicative
}

Enum datacube::ts::Rebalance
{
  daily, weekly, monthly
}

Enum datacube::ts::Grain
{
  day, week, month, quarter
}

Enum datacube::ts::Compare
{
  none, rebase, percent
}

// The mean of the window's values, none until it holds ramp of them (gs-quant's mean, its ramp).
function datacube::ts::mean<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->average()))->rename(~m, $out)
}

// The sum of the window's values, none until it holds ramp of them.
function datacube::ts::sum<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->sum()))->rename(~m, $out)
}

// The least of the window's values, none until it holds ramp of them.
function datacube::ts::min<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->min()))->rename(~m, $out)
}

// The most of the window's values, none until it holds ramp of them.
function datacube::ts::max<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->max()))->rename(~m, $out)
}

// The most less the least of the window's values, none until it holds ramp of them.
function datacube::ts::range<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->max()->toOne() - $y->min()->toOne()))->rename(~m, $out)
}

// The sample standard deviation of the window's values, none until it holds ramp of them (at least two).
function datacube::ts::std<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->stdDevSample()))->rename(~m, $out)
}

// The sample variance of the window's values, none until it holds ramp of them (at least two).
function datacube::ts::var<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)}:y|if($y->count() < $ramp, |[], |$y->varianceSample()))->rename(~m, $out)
}

// A statistic of the values a calendar period back from each date of the line, (date - n unit, date] (gs-quant's
// rolling_offset) -- stat a Statistic (mean, sum, min, max, range, std, var, count): the line joined to itself on those dates, the statistic per date, joined back; on every date, however few values its
// period holds.
function datacube::ts::windowPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], stat: datacube::ts::Statistic[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)]);
  let windows = $base->select(~[l, d])->extend(~lo:x|$x.d->toOne()->adjust(0 - $n, $unit))
    ->join($base->select(~[l, d, v])->rename(~l, ~wl)->rename(~d, ~wd)->rename(~v, ~wv), meta::pure::functions::relation::JoinKind.INNER, {a,b|($a.l == $b.wl) && ($b.wd > $a.lo) && ($b.wd <= $a.d)})
    ->groupBy(~[l, d], ~s:x|$x.wv:y|if($stat == datacube::ts::Statistic.mean, |$y->average(), |if($stat == datacube::ts::Statistic.sum, |$y->sum(), |if($stat == datacube::ts::Statistic.min, |$y->min(), |if($stat == datacube::ts::Statistic.max, |$y->max(), |if($stat == datacube::ts::Statistic.range, |$y->max()->toOne() - $y->min()->toOne(), |if($stat == datacube::ts::Statistic.std, |$y->stdDevSample(), |if($stat == datacube::ts::Statistic.count, |$y->count(), |$y->varianceSample()))))))))
    ->rename(~l, ~gl)->rename(~d, ~gd);
  $base->join($windows, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.gl) && ($a.d == $b.gd)})
    ->without(~[l, d, v, gl, gd])
    ->rename(~s, $out);
}

// A column none before a calendar period on from the first date another column has a value (gs-quant's apply_ramp of a
// DateOffset, x.loc[x.index[0] + r:], its start the first date of the series read -- a price's, where the column is of
// its returns).
function datacube::ts::rampPeriod<T,L,D,S,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], since: ColSpec<S⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line), ~fd:{p,f,r|if($r->read($since)->isNotEmpty(), |$r->read($date), |[])}:y|$y->min())
    ->extend(~o:x|if($x->read($date)->toOne() < $x.fd->toOne()->adjust($n, $unit), |[], |$x->read($value)))
    ->without(~fd)->rename(~o, $out)
}

// A statistic of a calendar period back of each date (windowPeriod's), none before the line's first value's date that
// period on (gs-quant's rolling_offset and apply_ramp of a DateOffset).
function datacube::ts::overPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], stat: datacube::ts::Statistic[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::windowPeriod($line, $date, $value, $n, $unit, $stat, ~ws)
    ->datacube::ts::rampPeriod($line, $date, $value, ~ws, $n, $unit, $out)
    ->without(~ws)
}

// The value obs rows back (gs-quant's lag of a count): x[-obs], none for the line's first obs rows.
function datacube::ts::lag<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], obs: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$p->lag($r, $obs)->read($value)})->rename(~m, $out)
}

// The value as of a calendar period back from each date of the line (gs-quant's lag of a period, as of): of its dates
// with a value, the latest at or before (date - n unit) -- an as-of join; none before the line's first that far back.
function datacube::ts::lagPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)]);
  $base->extend(~at:x|$x.d->toOne()->adjust(0 - $n, $unit))
    ->asOfJoin($base->filter(x|$x.v->isNotEmpty())->select(~[l, d, v])->rename(~l, ~bl)->rename(~d, ~bd)->rename(~v, ~o), {a,b|$a.at >= $b.bd}, {a,b|$a.l == $b.bl})
    ->without(~[l, d, v, at, bl, bd])
    ->rename(~o, $out);
}

// The change over obs rows back (gs-quant's diff): x - x[-obs].
function datacube::ts::diff<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], obs: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($value)->toOne() - $p->lag($r, $obs)->read($value)->toOne()})->rename(~m, $out)
}

// The change over a calendar period back (gs-quant's diff of a period): x less its value as of (date - n unit).
function datacube::ts::diffPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::lagPeriod($line, $date, $value, $n, $unit, $out)
    ->extend(~m:r|$r->read($value)->toOne() - $r->read($out)->toOne())
    ->without($out)->rename(~m, $out)
}

// The returns over obs rows back (gs-quant's returns), of a type (Returns: simple, log, absolute): x / x[-obs] - 1, none where x[-obs] is 0 or there is none; log(x) - log(x[-obs]), none where
// either is 0 or below; x - x[-obs].
function datacube::ts::returns<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], obs: Integer[1], type: datacube::ts::Returns[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|if($type == datacube::ts::Returns.log,
      |if(($r->read($value)->toOne() <= 0) || ($p->lag($r, $obs)->read($value)->toOne() <= 0), |[], |log($r->read($value)->toOne()) - log($p->lag($r, $obs)->read($value)->toOne())),
      |if($type == datacube::ts::Returns.absolute,
        |$r->read($value)->toOne() - $p->lag($r, $obs)->read($value)->toOne(),
        |if($p->lag($r, $obs)->read($value)->toOne() == 0, |[], |$r->read($value)->toOne() / $p->lag($r, $obs)->read($value)->toOne() - 1)))})
    ->rename(~m, $out)
}

// The returns over a calendar period back (gs-quant's returns of a period), of a type as returns': each date against
// its value as of (date - n unit).
function datacube::ts::returnsPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], type: datacube::ts::Returns[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::lagPeriod($line, $date, $value, $n, $unit, $out)
    ->extend(~m:r|if($type == datacube::ts::Returns.log,
      |if(($r->read($value)->toOne() <= 0) || ($r->read($out)->toOne() <= 0), |[], |log($r->read($value)->toOne()) - log($r->read($out)->toOne())),
      |if($type == datacube::ts::Returns.absolute,
        |$r->read($value)->toOne() - $r->read($out)->toOne(),
        |if($r->read($out)->toOne() == 0, |[], |$r->read($value)->toOne() / $r->read($out)->toOne() - 1))))
    ->without($out)->rename(~m, $out)
}

// The value annualized (gs-quant's annualize): times the square root of the periods a year (a column, the same down a
// line; none where it is none).
function datacube::ts::annualize<T,V,Y,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~m:r|$r->read($value)->toOne() * sqrt($r->read($perYear)->toOne()))->rename(~m, $out)
}

// The line's first value beside each of its rows (gs-quant's first, on every date): of its dates with a value, the
// first's.
function datacube::ts::first<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)])
    ->extend(over(~l), ~at:{p,f,r|if($r.v->isNotEmpty(), |$r.d, |[])}:y|$y->min())
    ->extend(over(~l), ~o:{p,f,r|if($r.d == $r.at, |$r.v, |[])}:y|$y->max())
    ->without(~[l, d, v, at])->rename(~o, $out)
}

// The line's last value beside each of its rows (gs-quant's last, on every date): of its dates with a value, the
// last's.
function datacube::ts::last<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)])
    ->extend(over(~l), ~at:{p,f,r|if($r.v->isNotEmpty(), |$r.d, |[])}:y|$y->max())
    ->extend(over(~l), ~o:{p,f,r|if($r.d == $r.at, |$r.v, |[])}:y|$y->max())
    ->without(~[l, d, v, at])->rename(~o, $out)
}

// The line's value as of a date beside each of its rows: of its dates with a value, the latest at or before from -- or,
// none at or before it, the first (a base the first date a view shows sets).
function datacube::ts::valueAt<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], from: Date[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)])
    ->extend(over(~l), ~fst:{p,f,r|if($r.v->isNotEmpty(), |$r.d, |[])}:y|$y->min())
    ->extend(over(~l), ~bef:{p,f,r|if($r.v->isNotEmpty() && ($r.d->toOne() <= $from), |$r.d, |[])}:y|$y->max())
    ->extend(~at:x|if($x.bef->isEmpty(), |$x.fst, |$x.bef))
    ->extend(over(~l), ~o:{p,f,r|if($r.d == $r.at, |$r.v, |[])}:y|$y->max())
    ->without(~[l, d, v, fst, bef, at])->rename(~o, $out)
}

// The value as a multiple of the line's first, times initial (gs-quant's index): none where the first is 0.
function datacube::ts::index<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], initial: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::first($line, $date, $value, $out)
    ->extend(~m:r|if($r->read($out)->toOne() == 0, |[], |$r->read($value)->toOne() / $r->read($out)->toOne() * $initial))
    ->without($out)->rename(~m, $out)
}

// The value as a multiple of the line's as of a date (valueAt's), times initial: none where that is 0.
function datacube::ts::indexFrom<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], from: Date[1], initial: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::valueAt($line, $date, $value, $from, $out)
    ->extend(~m:r|if($r->read($out)->toOne() == 0, |[], |$r->read($value)->toOne() / $r->read($out)->toOne() * $initial))
    ->without($out)->rename(~m, $out)
}

// The value less the line's first (gs-quant's change).
function datacube::ts::change<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::first($line, $date, $value, $out)
    ->extend(~m:r|$r->read($value)->toOne() - $r->read($out)->toOne())
    ->without($out)->rename(~m, $out)
}

// The value less the line's as of a date (valueAt's).
function datacube::ts::changeFrom<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], from: Date[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::valueAt($line, $date, $value, $from, $out)
    ->extend(~m:r|$r->read($value)->toOne() - $r->read($out)->toOne())
    ->without($out)->rename(~m, $out)
}

// The value over the line's first, less 1: none where the first is 0.
function datacube::ts::percentChange<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::first($line, $date, $value, $out)
    ->extend(~m:r|if($r->read($out)->toOne() == 0, |[], |$r->read($value)->toOne() / $r->read($out)->toOne() - 1))
    ->without($out)->rename(~m, $out)
}

// The value over the line's as of a date (valueAt's), less 1: none where that is 0.
function datacube::ts::percentChangeFrom<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], from: Date[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::valueAt($line, $date, $value, $from, $out)
    ->extend(~m:r|if($r->read($out)->toOne() == 0, |[], |$r->read($value)->toOne() / $r->read($out)->toOne() - 1))
    ->without($out)->rename(~m, $out)
}

// The annualized volatility of a column of returns, in percent (gs-quant's volatility, its returns the caller's: one of
// the returns above): the sample standard deviation of the window's, times the square root of the periods a year (a
// column, the same down a line), times 100 -- none until the window holds ramp of them (at least two).
function datacube::ts::volatility<T,V,Y,O>(t: Relation<T>[1], win: _Window<T>[1], returns: ColSpec<V⊆T>[1], ramp: Integer[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~m:{p,f,r|$r->read($returns)->toOne() * sqrt($r->read($perYear)->toOne()) * 100}:y|if($y->count() < $ramp, |[], |$y->stdDevSample()))->rename(~m, $out)
}

// The z-score of each value against its window's mean and sample deviation, none until the window holds ramp of them
// or where the deviation is 0 (gs-quant's zscores).
function datacube::ts::zscores<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[m:{p,f,r|$r->read($value)}:y|$y->average(), s:{p,f,r|$r->read($value)}:y|$y->stdDevSample(), n:{p,f,r|$r->read($value)}:y|$y->count()])
    ->extend(~z:x|if(($x.n->toOne() < $ramp) || ($x.s->toOne() == 0), |[], |($x->read($value)->toOne() - $x.m->toOne()) / $x.s->toOne()))
    ->without(~[m, s, n])->rename(~z, $out)
}

// The z-score against a calendar period back of each date (overPeriod's mean and deviation), none where the deviation
// is 0, or before the line's first value's date that period on.
function datacube::ts::zscoresPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::overPeriod($line, $date, $value, $n, $unit, datacube::ts::Statistic.mean, ~m)->datacube::ts::overPeriod($line, $date, $value, $n, $unit, datacube::ts::Statistic.std, ~s)
    ->extend(~z:x|if($x.s->toOne() == 0, |[], |($x->read($value)->toOne() - $x.m->toOne()) / $x.s->toOne()))
    ->without(~[m, s])->rename(~z, $out)
}

// Each value held within limit sample deviations of its window's mean (gs-quant's winsorize).
function datacube::ts::winsorize<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], limit: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[m:{p,f,r|$r->read($value)}:y|$y->average(), s:{p,f,r|$r->read($value)}:y|$y->stdDevSample()])
    ->extend(~[hi:x|$x.m->toOne() + $limit * $x.s->toOne(), lo:x|$x.m->toOne() - $limit * $x.s->toOne()])
    ->extend(~o:x|if($x->read($value)->toOne() > $x.hi->toOne(), |$x.hi, |if($x->read($value)->toOne() < $x.lo->toOne(), |$x.lo, |$x->read($value))))
    ->without(~[m, s, hi, lo])->rename(~o, $out)
}

// The fall from the window's peak, x / peak - 1, none until the window holds ramp values or where the peak is 0
// (gs-quant's drawdown).
function datacube::ts::drawdown<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[pk:{p,f,r|$r->read($value)}:y|$y->max(), n:{p,f,r|$r->read($value)}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.pk->toOne() == 0), |[], |$x->read($value)->toOne() / $x.pk->toOne() - 1))
    ->without(~[pk, n])->rename(~o, $out)
}

// The fall from the peak of a calendar period back of each date (overPeriod's max), none where the peak is 0, or before
// the line's first value's date that period on.
function datacube::ts::drawdownPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::overPeriod($line, $date, $value, $n, $unit, datacube::ts::Statistic.max, ~pk)
    ->extend(~o:x|if($x.pk->toOne() == 0, |[], |$x->read($value)->toOne() / $x.pk->toOne() - 1))
    ->without(~pk)->rename(~o, $out)
}

// The deepest drawdown within the window, each against the window's peak: none until the window holds ramp values
// (gs-quant's max_drawdown).
function datacube::ts::maxDrawdown<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~pk:{p,f,r|$r->read($value)}:y|$y->max())
    ->extend(~dd:x|if($x.pk->toOne() == 0, |[], |$x->read($value)->toOne() / $x.pk->toOne() - 1))
    ->extend($win, ~[md:{p,f,r|$r.dd}:y|$y->min(), n:{p,f,r|$r->read($value)}:y|$y->count()])
    ->extend(~o:x|if($x.n->toOne() < $ramp, |[], |$x.md))
    ->without(~[pk, dd, md, n])->rename(~o, $out)
}

// The annualized Sharpe ratio of a column of returns over the window: their mean over their sample deviation, times the
// square root of the periods a year (a column), none until the window holds ramp of them or where the deviation is 0.
function datacube::ts::sharpeRatio<T,V,Y,O>(t: Relation<T>[1], win: _Window<T>[1], returns: ColSpec<V⊆T>[1], ramp: Integer[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[m:{p,f,r|$r->read($returns)}:y|$y->average(), s:{p,f,r|$r->read($returns)}:y|$y->stdDevSample(), n:{p,f,r|$r->read($returns)}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.s->toOne() == 0), |[], |$x.m->toOne() / $x.s->toOne() * sqrt($x->read($perYear)->toOne())))
    ->without(~[m, s, n])->rename(~o, $out)
}

// Bands k sample deviations either side of the window's mean, and their width (gs-quant's bollinger_bands): none until
// the window holds ramp values, and two.
function datacube::ts::bollingerBands<T,V,O1,O2,O3>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], k: Number[1], lower: ColSpec<O1>[1], upper: ColSpec<O2>[1], width: ColSpec<O3>[1]): Relation<T+O1+O2+O3>[1]
{
  $t->extend($win, ~[m:{p,f,r|$r->read($value)}:y|$y->average(), s:{p,f,r|$r->read($value)}:y|$y->stdDevSample(), n:{p,f,r|$r->read($value)}:y|$y->count()])
    ->extend(~[lo:x|if(($x.n->toOne() < $ramp) || ($x.n->toOne() < 2), |[], |$x.m->toOne() - $k * $x.s->toOne()), hi:x|if(($x.n->toOne() < $ramp) || ($x.n->toOne() < 2), |[], |$x.m->toOne() + $k * $x.s->toOne())])
    ->extend(~wd:x|$x.hi->toOne() - $x.lo->toOne())
    ->without(~[m, s, n])->rename(~lo, $lower)->rename(~hi, $upper)->rename(~wd, $width)
}

// Bollinger bands of a calendar period back of each date (overPeriod's mean and deviation): none before the line's first
// value's date that period on.
function datacube::ts::bollingerBandsPeriod<T,L,D,V,O1,O2,O3>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], k: Number[1], lower: ColSpec<O1>[1], upper: ColSpec<O2>[1], width: ColSpec<O3>[1]): Relation<T+O1+O2+O3>[1]
{
  $t->datacube::ts::overPeriod($line, $date, $value, $n, $unit, datacube::ts::Statistic.mean, ~m)->datacube::ts::overPeriod($line, $date, $value, $n, $unit, datacube::ts::Statistic.std, ~s)
    ->extend(~[lo:x|$x.m->toOne() - $k * $x.s->toOne(), hi:x|$x.m->toOne() + $k * $x.s->toOne()])
    ->extend(~wd:x|$x.hi->toOne() - $x.lo->toOne())
    ->without(~[m, s])->rename(~lo, $lower)->rename(~hi, $upper)->rename(~wd, $width)
}

// The percentile rank of each value within its window (gs-quant's percentiles: scipy's percentileofscore, kind mean):
// the share of the window's values below it, those equal to it counted half, in percent. The window is the w rows to it
// (0: every row from the line's first), the line joined to itself on them; none until it holds w values.
function datacube::ts::percentiles<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)])
    ->extend(over(~l, [~d->ascending()]), ~rn:{p,f,r|$p->rowNumber($r)});
  let pairs = $base->select(~[l, rn, v])
    ->join($base->select(~[l, rn, v])->rename(~l, ~ol)->rename(~rn, ~orn)->rename(~v, ~ov), meta::pure::functions::relation::JoinKind.INNER,
      {a,b|($a.l == $b.ol) && ($b.orn <= $a.rn) && (($w == 0) || ($b.orn > ($a.rn - $w)))})
    ->groupBy(~[l, rn], ~[lt:x|if($x.ov->toOne() < $x.v->toOne(), |1, |[]):y|$y->count(), le:x|if($x.ov->toOne() <= $x.v->toOne(), |1, |[]):y|$y->count(), c:x|$x.ov:y|$y->count()])
    ->rename(~l, ~gl)->rename(~rn, ~grn);
  $base->join($pairs, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.gl) && ($a.rn == $b.grn)})
    ->extend(~o:x|if($x.v->isEmpty() || ($x.c->toOne() == 0) || (($w > 0) && ($x.c->toOne() < $w)), |[], |($x.lt->toOne() + $x.le->toOne()) / (2 * $x.c->toOne()) * 100))
    ->without(~[l, d, v, rn, gl, grn, lt, le, c])->rename(~o, $out);
}

// The percentile rank of each value among those a calendar period back of its date, (date - n unit, date]: none before
// the line's first value's date that period on.
function datacube::ts::percentilesPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)]);
  let pairs = $base->select(~[l, d, v])->extend(~lo:x|$x.d->toOne()->adjust(0 - $n, $unit))
    ->join($base->select(~[l, d, v])->rename(~l, ~ol)->rename(~d, ~od)->rename(~v, ~ov), meta::pure::functions::relation::JoinKind.INNER,
      {a,b|($a.l == $b.ol) && ($b.od > $a.lo) && ($b.od <= $a.d)})
    ->groupBy(~[l, d], ~[lt:x|if($x.ov->toOne() < $x.v->toOne(), |1, |[]):y|$y->count(), le:x|if($x.ov->toOne() <= $x.v->toOne(), |1, |[]):y|$y->count(), c:x|$x.ov:y|$y->count()])
    ->rename(~l, ~gl)->rename(~d, ~gd);
  $base->join($pairs, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.gl) && ($a.d == $b.gd)})
    ->extend(over(~l), ~fd:{p,f,r|if($r.v->isNotEmpty(), |$r.d, |[])}:y|$y->min())
    ->extend(~o:x|if($x.v->isEmpty() || ($x.c->toOne() == 0) || ($x.d->toOne() < $x.fd->toOne()->adjust($n, $unit)), |[], |($x.lt->toOne() + $x.le->toOne()) / (2 * $x.c->toOne()) * 100))
    ->without(~[l, d, v, gl, gd, lt, le, c, fd])->rename(~o, $out);
}

// The q-th quantile of the window's values, continuous (pandas' quantile: between the two nearest; a median the 0.5th).
// The window is the w rows to it (0: every row from the line's first), the line joined to itself on them; none until it
// holds w values.
function datacube::ts::quantile<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], q: Float[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)])
    ->extend(over(~l, [~d->ascending()]), ~rn:{p,f,r|$p->rowNumber($r)});
  let pairs = $base->select(~[l, rn])
    ->join($base->select(~[l, rn, v])->rename(~l, ~ol)->rename(~rn, ~orn)->rename(~v, ~ov), meta::pure::functions::relation::JoinKind.INNER,
      {a,b|($a.l == $b.ol) && ($b.orn <= $a.rn) && (($w == 0) || ($b.orn > ($a.rn - $w)))})
    ->groupBy(~[l, rn], ~[qv:x|$x.ov:y|$y->percentile($q, true, true), c:x|$x.ov:y|$y->count()])
    ->rename(~l, ~gl)->rename(~rn, ~grn);
  $base->join($pairs, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.gl) && ($a.rn == $b.grn)})
    ->extend(~o:x|if(($w > 0) && ($x.c->toOne() < $w), |[], |$x.qv))
    ->without(~[l, d, v, rn, gl, grn, qv, c])->rename(~o, $out);
}

// The q-th quantile of the values a calendar period back of each date, (date - n unit, date]: none before the line's
// first value's date that period on.
function datacube::ts::quantilePeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], q: Float[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)]);
  let pairs = $base->select(~[l, d])->extend(~lo:x|$x.d->toOne()->adjust(0 - $n, $unit))
    ->join($base->select(~[l, d, v])->rename(~l, ~ol)->rename(~d, ~od)->rename(~v, ~ov), meta::pure::functions::relation::JoinKind.INNER,
      {a,b|($a.l == $b.ol) && ($b.od > $a.lo) && ($b.od <= $a.d)})
    ->groupBy(~[l, d], ~qv:x|$x.ov:y|$y->percentile($q, true, true))
    ->rename(~l, ~gl)->rename(~d, ~gd);
  $base->join($pairs, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.gl) && ($a.d == $b.gd)})
    ->extend(over(~l), ~fd:{p,f,r|if($r.v->isNotEmpty(), |$r.d, |[])}:y|$y->min())
    ->extend(~o:x|if($x.d->toOne() < $x.fd->toOne()->adjust($n, $unit), |[], |$x.qv))
    ->without(~[l, d, v, gl, gd, qv, fd])->rename(~o, $out);
}

// The q-th quantile of the line's every value, beside each of its rows: one aggregate a line, grouped by it and joined
// back.
function datacube::ts::quantileOfLine<T,L,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], value: ColSpec<V⊆T>[1], q: Float[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), v:r|$r->read($value)]);
  $base->join($base->groupBy(~[l], ~o:x|$x.v:y|$y->percentile($q, true, true))->rename(~l, ~gl), meta::pure::functions::relation::JoinKind.LEFT, {a,b|$a.l == $b.gl})
    ->without(~[l, v, gl])->rename(~o, $out);
}

// An exponentially weighted average from the at-th value of the line on (pandas' ewm(alpha = 1 - decay,
// adjust=False).mean(), gs-quant's exponential_moving_average): seed's value there, then decay·y[-1] + (1 - decay)·x --
// a recursion no window frame says, written as its closed form, the database's: y_t = (1 - decay)·Σ decay^(t-i)·x'_i,
// x'_0 = the seed / (1 - decay). Summed in blocks of W values, W the fewest with decay^W under 2^-60 (so decay^-pos,
// pos < W, stays under 2^60 in a double): within a block a running sum of x'·decay^-pos, times decay^pos; the block
// before carried in whole -- its total, by windows, read at this block's first value as the value before it -- the
// blocks before it weighing under 2^-60 of it, below a double's precision. None before the at-th value, or where the
// value is none.
function datacube::ts::ewmaFrom<T,L,D,V,S,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], decay: Float[1], at: Integer[1], seed: ColSpec<S⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value), sv:r|$r->read($seed)])
    ->extend(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~n:{p,f,r|$r.v}:y|$y->count())
    ->extend(~k:x|if($x.v->isEmpty() || (($x.n->toOne() - (1 + $at)) < 0), |[], |$x.n->toOne() - (1 + $at)))
    ->extend(~blk:x|floor($x.k->toOne() / ceiling((-60.0 * log(2.0)) / log($decay))))
    ->extend(~pos:x|$x.k->toOne() - ($x.blk->toOne() * ceiling((-60.0 * log(2.0)) / log($decay))))
    ->extend(~u:x|if($x.k->toOne() == 0, |$x.sv->toOne() / (1.0 - $decay), |$x.v->toOne()) * pow($decay, 0 - $x.pos->toOne()))
    ->extend(over(~[l, blk], [~d->ascending()], rows(unbounded(), 0)), ~s:{p,f,r|$r.u}:y|$y->plus())
    ->extend(over(~[l, blk], [~d->ascending()], rows(unbounded(), unbounded())), ~tot:{p,f,r|$r.u}:y|$y->plus())
    ->extend(~has:x|if($x.k->isEmpty(), |0, |1))
    ->extend(over(~[l, has], [~d->ascending()]), ~bf:{p,f,r|$p->lag($r).tot})
    ->extend(over(~[l, blk], [~d->ascending()], rows(unbounded(), unbounded())), ~c:{p,f,r|if($r.pos == 0, |$r.bf, |[])}:y|$y->max())
    ->extend(~o:x|if($x.k->isEmpty(), |[], |if($x.c->isEmpty(), |((1.0 - $decay) * pow($decay, $x.pos->toOne())) * $x.s->toOne(),
      |(((1.0 - $decay) * pow($decay, $x.pos->toOne())) * $x.s->toOne()) + (pow($decay, $x.pos->toOne() + 1) * (((1.0 - $decay) * pow($decay, ceiling((-60.0 * log(2.0)) / log($decay)) - 1)) * $x.c->toOne())))))
    ->without(~[l, d, v, sv, n, k, blk, pos, u, s, tot, has, bf, c])->rename(~o, $out)
}

// An exponentially weighted average from the line's first value (gs-quant's exponential_moving_average, decay its
// beta).
function datacube::ts::ewma<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], decay: Float[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::ewmaFrom($line, $date, $value, $decay, 0, $value, $out)
}

// An exponentially weighted standard deviation (pandas' ewm(alpha = 1 - decay, adjust=False).std(), gs-quant's
// exponential_std): the weighted mean square less the squared weighted mean, of each value less the line's first (so
// nothing large is subtracted from itself), over 1 less the sum of the squared weights (pandas' bias correction:
// decay^2t + (1 - decay)²·(1 - decay^2t) / (1 - decay²) of the t-th value) -- none at the first, whose weights are one.
function datacube::ts::ewmStd<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], decay: Float[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), v:r|$r->read($value)])
    ->extend(over(~l, [$date->ascending()], rows(unbounded(), 0)), ~n:{p,f,r|$r.v}:y|$y->count())
    ->extend(~k:x|if($x.v->isEmpty() || (($x.n->toOne() - 1) < 0), |[], |$x.n->toOne() - 1))
    ->extend(over(~l), ~fst:{p,f,r|if($r.k == 0, |$r.v, |[])}:y|$y->max())
    ->extend(~dv:x|$x.v->toOne() - $x.fst->toOne())
    ->extend(~d2:x|$x.dv->toOne() * $x.dv->toOne())
    ->datacube::ts::ewma($line, $date, ~dv, $decay, ~m1)
    ->datacube::ts::ewma($line, $date, ~d2, $decay, ~m2)
    ->extend(~den:x|1.0 - (pow($decay, 2 * $x.k->toOne()) + ((((1.0 - $decay) * (1.0 - $decay)) * (1.0 - pow($decay, 2 * $x.k->toOne()))) / (1.0 - ($decay * $decay)))))
    ->extend(~var:x|$x.m2->toOne() - ($x.m1->toOne() * $x.m1->toOne()))
    ->extend(~o:x|if($x.k->isEmpty() || ($x.den->toOne() <= 0), |[], |sqrt(if($x.var->toOne() < 0, |0.0, |$x.var->toOne()) / $x.den->toOne())))
    ->without(~[l, v, n, k, fst, dv, d2, m1, m2, den, var])->rename(~o, $out)
}

// Wilder's smoothed average over w (gs-quant's smoothed_moving_average): its first the mean of the w values ending at the
// w-th after the first (gs-quant's ramp of w), then each (w - 1) parts the one before and one part the value -- an
// exponential average of decay (w - 1) / w from that mean; of one, each value from the line's second on (w a whole
// number, 1 or more: the branch is taken when the call is expanded).
function datacube::ts::smoothedMean<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  if($w == 1,
    |$t->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~n:{p,f,r|$r->read($value)}:y|$y->count())
      ->extend(~o:x|if($x->read($value)->isEmpty() || (($x.n->toOne() - 2) < 0), |[], |$x->read($value)))
      ->without(~n)->rename(~o, $out),
    |$t->extend(over($line, [$date->ascending()], rows(1 - $w, 0)), ~sm:{p,f,r|$r->read($value)}:y|$y->average())
      ->datacube::ts::ewmaFrom($line, $date, $value, ($w - 1) / $w, $w, ~sm, $out)
      ->without(~sm))
}

// An exponential average of span k (a weight 2 / (k + 1) on each new value, the one before (k - 1) / (k + 1)); of a span
// of 1, each value as it is.
function datacube::ts::ewmaSpan<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], k: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  if($k == 1, |$t->extend(~o:x|$x->read($value))->rename(~o, $out), |$t->datacube::ts::ewma($line, $date, $value, ($k - 1) / ($k + 1), $out))
}

// The MACD (gs-quant's macd): the exponential average of span m less that of span n, itself averaged over span s.
function datacube::ts::macd<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], m: Integer[1], n: Integer[1], s: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::ewmaSpan($line, $date, $value, $m, ~em)->datacube::ts::ewmaSpan($line, $date, $value, $n, ~en)
    ->extend(~df:x|$x.em->toOne() - $x.en->toOne())
    ->datacube::ts::ewmaSpan($line, $date, ~df, $s, ~o)
    ->without(~[em, en, df])->rename(~o, $out)
}

// The relative strength index over w (gs-quant's relative_strength_index): each change's gain and loss, each Wilder's
// smoothed average over w (smoothedMean), 100 - 100 / (1 + gains / losses) -- 100 where the losses' average is 0.
function datacube::ts::rsi<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()]), ~dx:{p,f,r|$r->read($value)->toOne() - $p->lag($r)->read($value)->toOne()})
    ->extend(~[gn:x|if($x.dx->isEmpty(), |[], |if($x.dx->toOne() > 0, |$x.dx->toOne(), |0.0)), ls:x|if($x.dx->isEmpty(), |[], |if($x.dx->toOne() < 0, |0.0 - $x.dx->toOne(), |0.0))])
    ->datacube::ts::smoothedMean($line, $date, ~gn, $w, ~up)->datacube::ts::smoothedMean($line, $date, ~ls, $w, ~dn)
    ->extend(~o:x|if($x.up->isEmpty(), |[], |if($x.dn->toOne() == 0, |100.0, |100.0 - (100.0 / (1 + ($x.up->toOne() / $x.dn->toOne()))))))
    ->without(~[dx, gn, ls, up, dn])->rename(~o, $out)
}

// The annualized exponentially weighted volatility of the simple returns, in percent (gs-quant's
// exponential_volatility: annualize(exponential_std(returns(x), beta)) * 100), the periods a year a column.
function datacube::ts::exponentialVolatility<T,L,D,V,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], decay: Float[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::returns(over($line, [$date->ascending()]), $value, 1, datacube::ts::Returns.simple, ~rt)
    ->datacube::ts::ewmStd($line, $date, ~rt, $decay, ~sd)
    ->extend(~o:x|($x.sd->toOne() * sqrt($x->read($perYear)->toOne())) * 100)
    ->without(~[rt, sd])->rename(~o, $out)
}

// The annualized exponentially weighted volatility of the changes, in the series' own units (gs-quant's
// exponential_spread_volatility: annualize(exponential_std(diff(x, 1), beta))).
function datacube::ts::exponentialSpreadVolatility<T,L,D,V,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], decay: Float[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::diff(over($line, [$date->ascending()]), $value, 1, ~dx)
    ->datacube::ts::ewmStd($line, $date, ~dx, $decay, ~sd)
    ->extend(~o:x|$x.sd->toOne() * sqrt($x->read($perYear)->toOne()))
    ->without(~[dx, sd])->rename(~o, $out)
}

// The true range of each period (Wilder's): its high less its low, or, with a close before it, the most of that and the
// distances of the high and the low from that close.
function datacube::ts::trueRange<T,L,D,H,N,C,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], high: ColSpec<H⊆T>[1], low: ColSpec<N⊆T>[1], close: ColSpec<C⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()]), ~pc:{p,f,r|$p->lag($r)->read($close)})
    ->extend(~o:x|if($x.pc->isEmpty(), |$x->read($high)->toOne() - $x->read($low)->toOne(),
      |max(max($x->read($high)->toOne() - $x->read($low)->toOne(), abs($x->read($high)->toOne() - $x.pc->toOne())), abs($x.pc->toOne() - $x->read($low)->toOne()))))
    ->without(~pc)->rename(~o, $out)
}

// The average true range over w (gs-quant's and TA-Lib's atr): Wilder's smoothed average of the true ranges.
function datacube::ts::atr<T,L,D,H,N,C,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], high: ColSpec<H⊆T>[1], low: ColSpec<N⊆T>[1], close: ColSpec<C⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::trueRange($line, $date, $high, $low, $close, ~tr)->datacube::ts::smoothedMean($line, $date, ~tr, $w, $out)->without(~tr)
}

// The stochastic oscillator over w (TA-Lib's fast %K): 100 × (close − lowest low) / (highest high − lowest low) of the
// w periods to each, none where those are equal or until there are w closes.
function datacube::ts::stochastic<T,L,D,H,N,C,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], high: ColSpec<H⊆T>[1], low: ColSpec<N⊆T>[1], close: ColSpec<C⊆T>[1], w: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()], rows(1 - $w, 0)), ~[hh:{p,f,r|$r->read($high)}:y|$y->max(), ll:{p,f,r|$r->read($low)}:y|$y->min(), n:{p,f,r|$r->read($close)}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $w) || ($x.hh->toOne() == $x.ll->toOne()), |[], |100 * (($x->read($close)->toOne() - $x.ll->toOne()) / ($x.hh->toOne() - $x.ll->toOne()))))
    ->without(~[hh, ll, n])->rename(~o, $out)
}

// Keltner channels (gs-quant's): k average true ranges over a (atr) either side of the close's exponential average of
// span w (ewmaSpan), and their width -- none until the average holds w closes.
function datacube::ts::keltnerChannels<T,L,D,H,N,C,O1,O2,O3>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], high: ColSpec<H⊆T>[1], low: ColSpec<N⊆T>[1], close: ColSpec<C⊆T>[1], w: Integer[1], a: Integer[1], k: Number[1], lower: ColSpec<O1>[1], upper: ColSpec<O2>[1], width: ColSpec<O3>[1]): Relation<T+O1+O2+O3>[1]
{
  $t->datacube::ts::ewmaSpan($line, $date, $close, $w, ~mid)->datacube::ts::atr($line, $date, $high, $low, $close, $a, ~av)
    ->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~n:{p,f,r|$r->read($close)}:y|$y->count())
    ->extend(~[lo:x|if($x.n->toOne() < $w, |[], |$x.mid->toOne() - ($k * $x.av->toOne())), hi:x|if($x.n->toOne() < $w, |[], |$x.mid->toOne() + ($k * $x.av->toOne()))])
    ->extend(~wd:x|$x.hi->toOne() - $x.lo->toOne())
    ->without(~[mid, av, n])->rename(~lo, $lower)->rename(~hi, $upper)->rename(~wd, $width)
}

// Donchian channels over the window (gs-quant's): the lowest low and the highest high, and their width -- each none until
// the window holds ramp of its column's values.
function datacube::ts::donchianChannels<T,H,N,O1,O2,O3>(t: Relation<T>[1], win: _Window<T>[1], high: ColSpec<H⊆T>[1], low: ColSpec<N⊆T>[1], ramp: Integer[1], lower: ColSpec<O1>[1], upper: ColSpec<O2>[1], width: ColSpec<O3>[1]): Relation<T+O1+O2+O3>[1]
{
  $t->extend($win, ~[hh:{p,f,r|$r->read($high)}:y|$y->max(), nh:{p,f,r|$r->read($high)}:y|$y->count(), ll:{p,f,r|$r->read($low)}:y|$y->min(), nl:{p,f,r|$r->read($low)}:y|$y->count()])
    ->extend(~[hi:x|if($x.nh->toOne() < $ramp, |[], |$x.hh), lo:x|if($x.nl->toOne() < $ramp, |[], |$x.ll)])
    ->extend(~wd:x|$x.hi->toOne() - $x.lo->toOne())
    ->without(~[hh, nh, ll, nl])->rename(~lo, $lower)->rename(~hi, $upper)->rename(~wd, $width)
}

// Donchian channels of a calendar period back of each date (overPeriod's max and min): each none before its column's
// first value's date that period on.
function datacube::ts::donchianChannelsPeriod<T,L,D,H,N,O1,O2,O3>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], high: ColSpec<H⊆T>[1], low: ColSpec<N⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], lower: ColSpec<O1>[1], upper: ColSpec<O2>[1], width: ColSpec<O3>[1]): Relation<T+O1+O2+O3>[1]
{
  $t->datacube::ts::overPeriod($line, $date, $high, $n, $unit, datacube::ts::Statistic.max, ~hi)->datacube::ts::overPeriod($line, $date, $low, $n, $unit, datacube::ts::Statistic.min, ~lo)
    ->extend(~wd:x|$x.hi->toOne() - $x.lo->toOne())
    ->rename(~lo, $lower)->rename(~hi, $upper)->rename(~wd, $width)
}

// On-balance volume (gs-quant's): from 0, each period's volume added on a rise of the close, taken off on a fall.
function datacube::ts::obv<T,L,D,C,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], close: ColSpec<C⊆T>[1], volume: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()]), ~dx:{p,f,r|$r->read($close)->toOne() - $p->lag($r)->read($close)->toOne()})
    ->extend(~sv:x|if($x.dx->isEmpty(), |0.0, |if($x.dx->toOne() > 0, |$x->read($volume)->toOne(), |if($x.dx->toOne() < 0, |0.0 - $x->read($volume)->toOne(), |0.0))))
    ->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~o:{p,f,r|$r.sv}:y|$y->plus())
    ->without(~[dx, sv])->rename(~o, $out)
}

// Each row's price times its volume: the value traded (vwap's and vwapPeriod's Σ price × volume), none where either is
// none.
function datacube::ts::tradedValue<T,P,V,O>(t: Relation<T>[1], price: ColSpec<P⊆T>[1], volume: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($r->read($price)->isEmpty() || $r->read($volume)->isEmpty(), |[], |$r->read($price)->toOne() * $r->read($volume)->toOne()))->rename(~o, $out)
}

// The volume-weighted average price over the window: Σ price × volume (traded, tradedValue's: at a frequency, the sum of
// its periods' days') over Σ volume, none where the volume is 0 or until the window holds ramp prices.
function datacube::ts::vwap<T,P,X,V,O>(t: Relation<T>[1], win: _Window<T>[1], price: ColSpec<P⊆T>[1], traded: ColSpec<X⊆T>[1], volume: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[pv:{p,f,r|$r->read($traded)}:y|$y->plus(), vs:{p,f,r|$r->read($volume)}:y|$y->plus(), n:{p,f,r|$r->read($price)}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.vs->toOne() == 0), |[], |$x.pv->toOne() / $x.vs->toOne()))
    ->without(~[pv, vs, n])->rename(~o, $out)
}

// The annualized volatility of a column of returns over a calendar period back of each date, in percent (windowPeriod's
// sample deviation, times the square root of the periods a year, times 100): none before the first date of the series
// read (price) that period on -- gs-quant's returns keep the first price's date, which its ramp counts from.
function datacube::ts::volatilityPeriod<T,L,D,P,R,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], price: ColSpec<P⊆T>[1], returns: ColSpec<R⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::windowPeriod($line, $date, $returns, $n, $unit, datacube::ts::Statistic.std, ~sd)
    ->extend(~v:x|($x.sd->toOne() * sqrt($x->read($perYear)->toOne())) * 100)
    ->datacube::ts::rampPeriod($line, $date, $price, ~v, $n, $unit, $out)
    ->without(~[sd, v])
}

// The deepest drawdown within a calendar period back of each date, each against that period's peak (windowPeriod's max,
// then its min): none before the line's first value's date that period on.
function datacube::ts::maxDrawdownPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::windowPeriod($line, $date, $value, $n, $unit, datacube::ts::Statistic.max, ~pk)
    ->extend(~dd:x|if($x.pk->toOne() == 0, |[], |$x->read($value)->toOne() / $x.pk->toOne() - 1))
    ->datacube::ts::windowPeriod($line, $date, ~dd, $n, $unit, datacube::ts::Statistic.min, ~md)
    ->datacube::ts::rampPeriod($line, $date, $value, ~md, $n, $unit, $out)
    ->without(~[pk, dd, md])
}

// The annualized Sharpe ratio of a column of returns over a calendar period back of each date: their mean over their
// sample deviation, times the square root of the periods a year -- none where the deviation is 0, or before the first
// date of the series read (price) that period on.
function datacube::ts::sharpeRatioPeriod<T,L,D,P,R,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], price: ColSpec<P⊆T>[1], returns: ColSpec<R⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], perYear: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::windowPeriod($line, $date, $returns, $n, $unit, datacube::ts::Statistic.mean, ~m)->datacube::ts::windowPeriod($line, $date, $returns, $n, $unit, datacube::ts::Statistic.std, ~s)
    ->extend(~v:x|if($x.s->toOne() == 0, |[], |($x.m->toOne() / $x.s->toOne()) * sqrt($x->read($perYear)->toOne())))
    ->datacube::ts::rampPeriod($line, $date, $price, ~v, $n, $unit, $out)
    ->without(~[m, s, v])
}

// The volume-weighted average price over a calendar period back of each date: Σ price × volume (traded) over Σ volume
// (windowPeriod's sums), none where the volume is 0, or before the first price's date that period on.
function datacube::ts::vwapPeriod<T,L,D,P,X,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], price: ColSpec<P⊆T>[1], traded: ColSpec<X⊆T>[1], volume: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::windowPeriod($line, $date, $traded, $n, $unit, datacube::ts::Statistic.sum, ~pv)->datacube::ts::windowPeriod($line, $date, $volume, $n, $unit, datacube::ts::Statistic.sum, ~vs)
    ->extend(~v:x|if($x.vs->toOne() == 0, |[], |$x.pv->toOne() / $x.vs->toOne()))
    ->datacube::ts::rampPeriod($line, $date, $price, ~v, $n, $unit, $out)
    ->without(~[pv, vs, v])
}

// -- row maths (gs-quant's algebra): each value's own figure, none where it is none --

// The natural log, none at 0 or below.
function datacube::ts::log<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($value)->toOne() <= 0, |[], |log($x->read($value)->toOne())))->rename(~o, $out)
}

// e to the value.
function datacube::ts::exp<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|exp($x->read($value)->toOne()))->rename(~o, $out)
}

// The square root, none below 0.
function datacube::ts::sqrt<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($value)->toOne() < 0, |[], |sqrt($x->read($value)->toOne())))->rename(~o, $out)
}

// The absolute value.
function datacube::ts::abs<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|abs($x->read($value)->toOne()))->rename(~o, $out)
}

// The value to the power y.
function datacube::ts::power<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], y: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|pow($x->read($value)->toOne(), $y))->rename(~o, $out)
}

// The value, no lower than a floor (gs-quant's floor).
function datacube::ts::floor<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], least: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($value)->toOne() < $least, |$least, |$x->read($value)->toOne()))->rename(~o, $out)
}

// The value, no higher than a cap (gs-quant's ceil).
function datacube::ts::ceil<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], most: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($value)->toOne() > $most, |$most, |$x->read($value)->toOne()))->rename(~o, $out)
}

// 1 where the value is 0, else 0 (gs-quant's not_): none where it is none.
function datacube::ts::not<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($value)->isEmpty(), |[], |if($x->read($value)->toOne() == 0, |1, |0)))->rename(~o, $out)
}

// The rows whose value the operator matches removed (gs-quant's filter_), a FilterOperator: less_than, greater_than, l_equals,
// g_equals, equals or not_equals a number, or empty. A comparison with
// none never holds, so none is kept, but by not_equals, where it does (NaN != v): the values equal to it alone kept.
function datacube::ts::filterValues<T,V>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], operator: datacube::ts::FilterOperator[1], than: Number[1]): Relation<T>[1]
{
  $t->filter(x|if($operator == datacube::ts::FilterOperator.empty, |$x->read($value)->isNotEmpty(),
    |if($operator == datacube::ts::FilterOperator.not_equals, |$x->read($value)->toOne() == $than,
      |$x->read($value)->isEmpty() || !(if($operator == datacube::ts::FilterOperator.less_than, |$x->read($value)->toOne() < $than,
        |if($operator == datacube::ts::FilterOperator.greater_than, |$x->read($value)->toOne() > $than,
          |if($operator == datacube::ts::FilterOperator.l_equals, |$x->read($value)->toOne() <= $than,
            |if($operator == datacube::ts::FilterOperator.g_equals, |$x->read($value)->toOne() >= $than, |$x->read($value)->toOne() == $than))))))))
}

// -- along the line --

// The product of the window's values, an empty one passed over (pandas' prod): 0 where one is 0, its sign from the
// count of those below 0, its magnitude e to the sum of the logs of the others -- the database's.
function datacube::ts::windowProduct<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[zs:{p,f,r|if($r->read($value)->toOne() == 0, |1, |[])}:y|$y->count(), ng:{p,f,r|if($r->read($value)->toOne() < 0, |1, |[])}:y|$y->count(),
      lg:{p,f,r|if($r->read($value)->toOne() == 0, |[], |log(abs($r->read($value)->toOne())))}:y|$y->plus()])
    ->extend(~o:x|if($x.zs->toOne() > 0, |0.0, |pow(-1.0, $x.ng->toOne()) * exp(if($x.lg->isEmpty(), |0.0, |$x.lg->toOne()))))
    ->without(~[zs, ng, lg])->rename(~o, $out)
}

// The product of the window's values (gs-quant's product): none where the value is none, or until the window holds ramp
// of them.
function datacube::ts::product<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::windowProduct($win, $value, ~pr)
    ->extend($win, ~n:{p,f,r|$r->read($value)}:y|$y->count())
    ->extend(~o:x|if($x->read($value)->isEmpty() || ($x.n->toOne() < $ramp), |[], |$x.pr))
    ->without(~[pr, n])->rename(~o, $out)
}

// The product of the values a calendar period back of each date (product's, windowPeriod's): 0 where one is 0, else
// the sign of its negatives times the exponential of its logs' sum; none where the value is none, or before the line's
// first value's date that period on.
function datacube::ts::productPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[z:x|if($x->read($value)->toOne() == 0, |1, |[]), ng:x|if($x->read($value)->toOne() < 0, |1, |[]),
      lg:x|if($x->read($value)->toOne() == 0, |[], |log(abs($x->read($value)->toOne())))])
    ->datacube::ts::windowPeriod($line, $date, ~z, $n, $unit, datacube::ts::Statistic.count, ~zs)
    ->datacube::ts::windowPeriod($line, $date, ~ng, $n, $unit, datacube::ts::Statistic.count, ~ns)
    ->datacube::ts::windowPeriod($line, $date, ~lg, $n, $unit, datacube::ts::Statistic.sum, ~ls)
    ->extend(~pr:x|if($x.zs->toOne() > 0, |0.0, |pow(-1.0, $x.ns->toOne()) * exp(if($x.ls->isEmpty(), |0.0, |$x.ls->toOne()))))
    ->datacube::ts::rampPeriod($line, $date, $value, ~pr, $n, $unit, ~rp)
    ->extend(~o:x|if($x->read($value)->isEmpty(), |[], |$x.rp))
    ->without(~[z, ng, lg, zs, ns, ls, pr, rp])->rename(~o, $out)
}

// The compounded return to each date (gs-quant's geometrically_aggregate): the product of (1 + x) from the line's first,
// less 1; none where the value is none.
function datacube::ts::geometricallyAggregate<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~g:x|1 + $x->read($value)->toOne())
    ->datacube::ts::windowProduct(over($line, [$date->ascending()], rows(unbounded(), 0)), ~g, ~pr)
    ->extend(~o:x|if($x->read($value)->isEmpty(), |[], |$x.pr->toOne() - 1))
    ->without(~[g, pr])->rename(~o, $out)
}

// A value beyond both neighbours (by 1 + threshold times, percentage, or threshold beyond, absolute) their mean
// (gs-quant's smooth_spikes): beside none no spike (a comparison with NaN never holds, kept); the line's first and last
// rows none.
function datacube::ts::smoothSpikes<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], threshold: Number[1], type: datacube::ts::ThresholdType[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()]), ~pv:{p,f,r|$p->lag($r)->read($value)})
    ->extend(over($line, [$date->ascending()]), ~nx:{p,f,r|$p->lead($r)->read($value)})
    ->extend(over($line, [$date->ascending()]), ~rn:{p,f,r|$p->rowNumber($r)})
    ->extend(over($line), ~rs:{p,f,r|$r.rn}:y|$y->count())
    ->extend(~sp:x|$x.pv->isNotEmpty() && $x.nx->isNotEmpty() && $x->read($value)->isNotEmpty()
      && (if($type == datacube::ts::ThresholdType.absolute,
          |(($x->read($value)->toOne() > ($x.pv->toOne() + $threshold)) && ($x->read($value)->toOne() > ($x.nx->toOne() + $threshold)))
            || (($x.pv->toOne() > ($x->read($value)->toOne() + $threshold)) && ($x.nx->toOne() > ($x->read($value)->toOne() + $threshold))),
          |(($x->read($value)->toOne() > ($x.pv->toOne() * (1 + $threshold))) && ($x->read($value)->toOne() > ($x.nx->toOne() * (1 + $threshold))))
            || (($x.pv->toOne() > ($x->read($value)->toOne() * (1 + $threshold))) && ($x.nx->toOne() > ($x->read($value)->toOne() * (1 + $threshold)))))))
    ->extend(~o:x|if(($x.rn->toOne() == 1) || ($x.rn->toOne() == $x.rs->toOne()), |[], |if($x.sp->toOne(), |($x.pv->toOne() + $x.nx->toOne()) / 2, |$x->read($value))))
    ->without(~[pv, nx, rn, rs, sp])->rename(~o, $out)
}

// The variance swap's volatility over the window (gs-quant's vol_swap_volatility): the root mean square of the log
// returns (a mean of zero), times the root of 252, in percent -- none until the window holds ramp prices.
function datacube::ts::volSwapVolatility<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::returns($win, $value, 1, datacube::ts::Returns.log, ~lr)
    ->extend(~r2:x|$x.lr->toOne() * $x.lr->toOne())
    ->extend($win, ~[ms:{p,f,r|$r.r2}:y|$y->average(), n:{p,f,r|$r->read($value)}:y|$y->count()])
    ->extend(~o:x|if($x.n->toOne() < $ramp, |[], |(sqrt($x.ms->toOne()) * sqrt(252)) * 100))
    ->without(~[lr, r2, ms, n])->rename(~o, $out)
}

// The root mean square of the log returns over a calendar period back of each date, annualized by 252, in percent
// (volSwapVolatility's, windowPeriod's mean); none before the line's first value's date that period on.
function datacube::ts::volSwapVolatilityPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date)])
    ->datacube::ts::returns(over(~l, [~d->ascending()]), $value, 1, datacube::ts::Returns.log, ~lr)
    ->extend(~r2:x|$x.lr->toOne() * $x.lr->toOne())
    ->datacube::ts::windowPeriod($line, $date, ~r2, $n, $unit, datacube::ts::Statistic.mean, ~ms)
    ->extend(~vs:x|(sqrt($x.ms->toOne()) * sqrt(252)) * 100)
    ->datacube::ts::rampPeriod($line, $date, $value, ~vs, $n, $unit, ~o)
    ->without(~[l, d, lr, r2, ms, vs])->rename(~o, $out)
}

// The run of values above 0 (Direction.positive) or below (negative) to each date, counted (gs-quant's consecutive): 0 where
// it breaks, each break beginning a run.
function datacube::ts::consecutive<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], direction: datacube::ts::Direction[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:x|$x->read($line), ys:x|if($x->read($value)->isNotEmpty() && (if($direction == datacube::ts::Direction.negative, |$x->read($value)->toOne() < 0, |$x->read($value)->toOne() > 0)), |1, |0)])
    ->extend(over(~l, [$date->ascending()], rows(unbounded(), 0)), ~gp:{p,f,r|1 - $r.ys}:y|$y->plus())
    ->extend(over(~[l, gp], [$date->ascending()], rows(unbounded(), 0)), ~o:{p,f,r|$r.ys}:y|$y->plus())
    ->without(~[l, ys, gp])->rename(~o, $out)
}

// The count of the line's values to each date (gs-quant's count).
function datacube::ts::count<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~o:{p,f,r|$r->read($value)}:y|$y->count())->rename(~o, $out)
}

// -- the date's parts (gs-quant's datetime): of every date, those of none too --

// The day of the month.
function datacube::ts::day<T,D,O>(t: Relation<T>[1], date: ColSpec<D⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|$x->read($date)->toOne()->dayOfMonth())->rename(~o, $out)
}

// The month's number, 1 January's.
function datacube::ts::month<T,D,O>(t: Relation<T>[1], date: ColSpec<D⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|$x->read($date)->toOne()->monthNumber())->rename(~o, $out)
}

// The year.
function datacube::ts::year<T,D,O>(t: Relation<T>[1], date: ColSpec<D⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|$x->read($date)->toOne()->year())->rename(~o, $out)
}

// The quarter's number.
function datacube::ts::quarter<T,D,O>(t: Relation<T>[1], date: ColSpec<D⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|$x->read($date)->toOne()->quarterNumber())->rename(~o, $out)
}

// The day of the week, pandas' 0 for a Monday.
function datacube::ts::weekday<T,D,O>(t: Relation<T>[1], date: ColSpec<D⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|$x->read($date)->toOne()->dayOfWeekNumber(meta::pure::functions::date::DayOfWeek.Monday) - 1)->rename(~o, $out)
}

// -- two columns of one table (a pair's, aligned by the caller: each series on its own dates, then joined), over the
// window's pairs, the dates both have a value; none until the window holds ramp pairs --

// The correlation of the pairs (gs-quant's correlation, of the returns its caller reads).
function datacube::ts::correlation<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  // none where a side does not vary, as on every database (DuckDB's corr says NaN there, Postgres' none: the focused
  // Pure audit, 2026-10-28) -- every correlation of the library so (pairPeriod's, leadLag's; the conversion's review)
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->extend($win, ~[c:{p,f,r|rowMapper($r.pa, $r.pb)}:y|$y->corr(), n:{p,f,r|$r.pa}:y|$y->count(), va:{p,f,r|$r.pa}:y|$y->varianceSample(), vb:{p,f,r|$r.pb}:y|$y->varianceSample()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || $x.va->isEmpty() || $x.vb->isEmpty() || ($x.va->toOne() == 0) || ($x.vb->toOne() == 0), |[], |$x.c))
    ->without(~[pa, pb, c, n, va, vb])->rename(~o, $out)
}

// The correlation of the pairs squared (gs-quant's r_squared of a regression of the first on the second).
function datacube::ts::rSquared<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::correlation($win, $first, $second, $ramp, ~c)
    ->extend(~o:x|$x.c->toOne() * $x.c->toOne())
    ->without(~c)->rename(~o, $out)
}

// The sample covariance of the pairs (gs-quant's cov, of the series as they are).
function datacube::ts::covariance<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->extend($win, ~[cv:{p,f,r|rowMapper($r.pa, $r.pb)}:y|$y->covarSample(), n:{p,f,r|$r.pa}:y|$y->count()])
    ->extend(~o:x|if($x.n->toOne() < $ramp, |[], |$x.cv))
    ->without(~[pa, pb, cv, n])->rename(~o, $out)
}

// The slope of the first on the second over the pairs, least squares (gs-quant's beta of returns, its hedge_ratio of
// levels): their covariance over the second's variance, none where that is 0.
function datacube::ts::beta<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->extend($win, ~[cv:{p,f,r|rowMapper($r.pa, $r.pb)}:y|$y->covarSample(), vb:{p,f,r|$r.pb}:y|$y->varianceSample(), n:{p,f,r|$r.pa}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.vb->toOne() == 0), |[], |$x.cv->toOne() / $x.vb->toOne()))
    ->without(~[pa, pb, cv, vb, n])->rename(~o, $out)
}

// The intercept of the first on the second over the pairs, annualized (gs-quant's alpha): the first's mean less the
// slope times the second's, times the periods a year (a column).
function datacube::ts::alpha<T,X,Y,Z,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], perYear: ColSpec<Z⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->extend($win, ~[cv:{p,f,r|rowMapper($r.pa, $r.pb)}:y|$y->covarSample(), vb:{p,f,r|$r.pb}:y|$y->varianceSample(), ma:{p,f,r|$r.pa}:y|$y->average(), mb:{p,f,r|$r.pb}:y|$y->average(), n:{p,f,r|$r.pa}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.vb->toOne() == 0), |[], |($x.ma->toOne() - (($x.cv->toOne() / $x.vb->toOne()) * $x.mb->toOne())) * $x->read($perYear)->toOne()))
    ->without(~[pa, pb, cv, vb, ma, mb, n])->rename(~o, $out)
}

// The residual of the first on the second (gs-quant's residual of a regression of levels): the first less its fit,
// the intercept plus the slope times the second, each over the window's pairs.
function datacube::ts::residual<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->extend($win, ~[cv:{p,f,r|rowMapper($r.pa, $r.pb)}:y|$y->covarSample(), vb:{p,f,r|$r.pb}:y|$y->varianceSample(), ma:{p,f,r|$r.pa}:y|$y->average(), mb:{p,f,r|$r.pb}:y|$y->average(), n:{p,f,r|$r.pa}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.vb->toOne() == 0), |[],
      |$x->read($first)->toOne() - (($x.ma->toOne() - (($x.cv->toOne() / $x.vb->toOne()) * $x.mb->toOne())) + (($x.cv->toOne() / $x.vb->toOne()) * $x->read($second)->toOne()))))
    ->without(~[pa, pb, cv, vb, ma, mb, n])->rename(~o, $out)
}

// Each of two columns where both have a value, else none (a pair's): the pairs a window of two columns reads.
function datacube::ts::pairs<T,X,Y,A,B>(t: Relation<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], a: ColSpec<A>[1], b: ColSpec<B>[1]): Relation<T+A+B>[1]
{
  $t->extend(~[pa:r|if($r->read($second)->isEmpty(), |[], |$r->read($first)), pb:r|if($r->read($first)->isEmpty(), |[], |$r->read($second))])
    ->rename(~pa, $a)->rename(~pb, $b)
}

// A statistic of the pairs of two columns a calendar period back from each date of the line, (date - n unit, date]
// (windowPeriod's, of two columns) -- a PairStatistic, corr or cov (the sample's): the
// line joined to itself on those dates, the statistic per date, joined back; on every date. A correlation none where a
// side does not vary over the pairs, as correlation's.
function datacube::ts::pairPeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], stat: datacube::ts::PairStatistic[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), a:r|$r->read($first), b:r|$r->read($second)]);
  let windows = $base->select(~[l, d])->extend(~lo:x|$x.d->toOne()->adjust(0 - $n, $unit))
    ->join($base->select(~[l, d, a, b])->rename(~l, ~wl)->rename(~d, ~wd)->rename(~a, ~wa)->rename(~b, ~wb), meta::pure::functions::relation::JoinKind.INNER, {x,y|($x.l == $y.wl) && ($y.wd > $x.lo) && ($y.wd <= $x.d)})
    ->groupBy(~[l, d], ~[c:x|rowMapper($x.wa, $x.wb):y|$y->corr(), cv:x|rowMapper($x.wa, $x.wb):y|$y->covarSample(),
      va:x|if($x.wb->isEmpty(), |[], |$x.wa):y|$y->varianceSample(), vb:x|if($x.wa->isEmpty(), |[], |$x.wb):y|$y->varianceSample()])
    ->extend(~s:x|if($stat == datacube::ts::PairStatistic.corr, |if($x.va->isEmpty() || $x.vb->isEmpty() || ($x.va->toOne() == 0) || ($x.vb->toOne() == 0), |[], |$x.c), |$x.cv))
    ->select(~[l, d, s])->rename(~l, ~gl)->rename(~d, ~gd);
  $base->join($windows, meta::pure::functions::relation::JoinKind.LEFT, {x,y|($x.l == $y.gl) && ($x.d == $y.gd)})
    ->without(~[l, d, a, b, gl, gd])
    ->rename(~s, $out);
}

// The slope of the first column on the second over their pairs a calendar period back of each date, least squares:
// their covariance over the second's variance, none where that is 0; on every date.
function datacube::ts::slopePeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairPeriod($line, $date, $first, $second, $n, $unit, datacube::ts::PairStatistic.cov, ~cv)
    ->datacube::ts::windowPeriod($line, $date, $second, $n, $unit, datacube::ts::Statistic.var, ~vb)
    ->extend(~o:x|if($x.vb->toOne() == 0, |[], |$x.cv->toOne() / $x.vb->toOne()))
    ->without(~[cv, vb])->rename(~o, $out)
}

// -- the same, over a calendar period back of each date (gs-quant's Window of a DateOffset): the pairs whose dates are
// in (date - n unit, date], none before the first pair's date that period on --

// The correlation of the pairs over a calendar period (correlation's).
function datacube::ts::correlationPeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->datacube::ts::pairPeriod($line, $date, ~pa, ~pb, $n, $unit, datacube::ts::PairStatistic.corr, ~c)
    ->datacube::ts::rampPeriod($line, $date, ~pa, ~c, $n, $unit, ~o)
    ->without(~[pa, pb, c])->rename(~o, $out)
}

// The correlation of the pairs over a calendar period squared (rSquared's).
function datacube::ts::rSquaredPeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::correlationPeriod($line, $date, $first, $second, $n, $unit, ~c)
    ->extend(~o:x|$x.c->toOne() * $x.c->toOne())
    ->without(~c)->rename(~o, $out)
}

// The sample covariance of the pairs over a calendar period (covariance's).
function datacube::ts::covariancePeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->datacube::ts::pairPeriod($line, $date, ~pa, ~pb, $n, $unit, datacube::ts::PairStatistic.cov, ~cv)
    ->datacube::ts::rampPeriod($line, $date, ~pa, ~cv, $n, $unit, ~o)
    ->without(~[pa, pb, cv])->rename(~o, $out)
}

// The slope of the first on the second over the pairs of a calendar period (beta's).
function datacube::ts::betaPeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->datacube::ts::slopePeriod($line, $date, ~pa, ~pb, $n, $unit, ~s)
    ->datacube::ts::rampPeriod($line, $date, ~pa, ~s, $n, $unit, ~o)
    ->without(~[pa, pb, s])->rename(~o, $out)
}

// The intercept of the first on the second over the pairs of a calendar period, annualized (alpha's): the first's mean
// less the slope times the second's, each over the same pairs, times the periods a year (a column).
function datacube::ts::alphaPeriod<T,L,D,X,Y,Z,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], perYear: ColSpec<Z⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->datacube::ts::slopePeriod($line, $date, ~pa, ~pb, $n, $unit, ~s)
    ->datacube::ts::windowPeriod($line, $date, ~pa, $n, $unit, datacube::ts::Statistic.mean, ~ma)
    ->datacube::ts::windowPeriod($line, $date, ~pb, $n, $unit, datacube::ts::Statistic.mean, ~mb)
    ->extend(~al:x|($x.ma->toOne() - ($x.s->toOne() * $x.mb->toOne())) * $x->read($perYear)->toOne())
    ->datacube::ts::rampPeriod($line, $date, ~pa, ~al, $n, $unit, ~o)
    ->without(~[pa, pb, s, ma, mb, al])->rename(~o, $out)
}

// The residual of the first on the second over the pairs of a calendar period (residual's): the first less its fit,
// the intercept plus the slope times the second.
function datacube::ts::residualPeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->datacube::ts::slopePeriod($line, $date, ~pa, ~pb, $n, $unit, ~s)
    ->datacube::ts::windowPeriod($line, $date, ~pa, $n, $unit, datacube::ts::Statistic.mean, ~ma)
    ->datacube::ts::windowPeriod($line, $date, ~pb, $n, $unit, datacube::ts::Statistic.mean, ~mb)
    ->extend(~rs:x|$x->read($first)->toOne() - (($x.ma->toOne() - ($x.s->toOne() * $x.mb->toOne())) + ($x.s->toOne() * $x->read($second)->toOne())))
    ->datacube::ts::rampPeriod($line, $date, ~pa, ~rs, $n, $unit, ~o)
    ->without(~[pa, pb, s, ma, mb, rs])->rename(~o, $out)
}

// The annualized tracking error of the first's returns against the second's, in percent: the sample deviation of their
// difference over the window, times the square root of the periods a year (a column), times 100 -- none until the
// window holds ramp differences.
function datacube::ts::trackingError<T,X,Y,Z,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], perYear: ColSpec<Z⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~ac:x|$x->read($first)->toOne() - $x->read($second)->toOne())
    ->extend($win, ~[sd:{p,f,r|$r.ac}:y|$y->stdDevSample(), n:{p,f,r|$r.ac}:y|$y->count()])
    ->extend(~o:x|if($x.n->toOne() < $ramp, |[], |($x.sd->toOne() * sqrt($x->read($perYear)->toOne())) * 100))
    ->without(~[ac, sd, n])->rename(~o, $out)
}

// The information ratio of the first's returns against the second's: the mean of their difference over its sample
// deviation over the window, annualized -- none where the deviation is 0, or until the window holds ramp differences.
function datacube::ts::informationRatio<T,X,Y,Z,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], perYear: ColSpec<Z⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~ac:x|$x->read($first)->toOne() - $x->read($second)->toOne())
    ->extend($win, ~[m:{p,f,r|$r.ac}:y|$y->average(), sd:{p,f,r|$r.ac}:y|$y->stdDevSample(), n:{p,f,r|$r.ac}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.sd->toOne() == 0), |[], |($x.m->toOne() / $x.sd->toOne()) * sqrt($x->read($perYear)->toOne())))
    ->without(~[ac, m, sd, n])->rename(~o, $out)
}

// The up capture of the first's returns against the second's: the first's mean return on the periods the second rose,
// over the second's mean then, over the window -- none where that is 0, or until the window holds ramp of the first's.
function datacube::ts::upCapture<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[ca:x|if($x->read($second)->toOne() > 0, |$x->read($first), |[]), cb:x|if($x->read($second)->toOne() > 0, |$x->read($second), |[])])
    ->extend($win, ~[ma:{p,f,r|$r.ca}:y|$y->average(), mb:{p,f,r|$r.cb}:y|$y->average(), n:{p,f,r|$r->read($first)}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.mb->toOne() == 0), |[], |$x.ma->toOne() / $x.mb->toOne()))
    ->without(~[ca, cb, ma, mb, n])->rename(~o, $out)
}

// The down capture of the first's returns against the second's: as upCapture, on the periods the second fell.
function datacube::ts::downCapture<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[ca:x|if($x->read($second)->toOne() < 0, |$x->read($first), |[]), cb:x|if($x->read($second)->toOne() < 0, |$x->read($second), |[])])
    ->extend($win, ~[ma:{p,f,r|$r.ca}:y|$y->average(), mb:{p,f,r|$r.cb}:y|$y->average(), n:{p,f,r|$r->read($first)}:y|$y->count()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.mb->toOne() == 0), |[], |$x.ma->toOne() / $x.mb->toOne()))
    ->without(~[ca, cb, ma, mb, n])->rename(~o, $out)
}

// The correlation of the first's returns with the second's lagged (or led) on its own dates (gs-quant's lead-lag
// study, the caller's lag): over the window, none until it holds ramp of the first's, or where a side does not vary
// over the pairs (correlation's).
function datacube::ts::leadLag<T,X,Y,O>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~[c:{p,f,r|rowMapper($r->read($first), $r->read($second))}:y|$y->corr(), n:{p,f,r|$r->read($first)}:y|$y->count(),
      va:{p,f,r|if($r->read($second)->isEmpty(), |[], |$r->read($first))}:y|$y->varianceSample(),
      vb:{p,f,r|if($r->read($first)->isEmpty(), |[], |$r->read($second))}:y|$y->varianceSample()])
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || $x.va->isEmpty() || $x.vb->isEmpty() || ($x.va->toOne() == 0) || ($x.vb->toOne() == 0), |[], |$x.c))
    ->without(~[c, n, va, vb])->rename(~o, $out)
}

// -- the same, over a calendar period back of each date: the returns whose dates are in (date - n unit, date], none
// before the first return's date that period on --

// The tracking error over a calendar period (trackingError's): overPeriod's sample deviation of the differences.
function datacube::ts::trackingErrorPeriod<T,L,D,X,Y,Z,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], perYear: ColSpec<Z⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~ac:x|$x->read($first)->toOne() - $x->read($second)->toOne())
    ->datacube::ts::overPeriod($line, $date, ~ac, $n, $unit, datacube::ts::Statistic.std, ~sd)
    ->extend(~o:x|($x.sd->toOne() * sqrt($x->read($perYear)->toOne())) * 100)
    ->without(~[ac, sd])->rename(~o, $out)
}

// The information ratio over a calendar period (informationRatio's): none where the deviation is 0.
function datacube::ts::informationRatioPeriod<T,L,D,X,Y,Z,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], perYear: ColSpec<Z⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~ac:x|$x->read($first)->toOne() - $x->read($second)->toOne())
    ->datacube::ts::overPeriod($line, $date, ~ac, $n, $unit, datacube::ts::Statistic.std, ~sd)
    ->datacube::ts::windowPeriod($line, $date, ~ac, $n, $unit, datacube::ts::Statistic.mean, ~m)
    ->extend(~o:x|if($x.sd->toOne() == 0, |[], |($x.m->toOne() / $x.sd->toOne()) * sqrt($x->read($perYear)->toOne())))
    ->without(~[ac, sd, m])->rename(~o, $out)
}

// The up capture over a calendar period (upCapture's): none where the second's mean then is 0.
function datacube::ts::upCapturePeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[ca:x|if($x->read($second)->toOne() > 0, |$x->read($first), |[]), cb:x|if($x->read($second)->toOne() > 0, |$x->read($second), |[])])
    ->datacube::ts::windowPeriod($line, $date, ~ca, $n, $unit, datacube::ts::Statistic.mean, ~ma)
    ->datacube::ts::windowPeriod($line, $date, ~cb, $n, $unit, datacube::ts::Statistic.mean, ~mb)
    ->extend(~cp:x|if($x.mb->toOne() == 0, |[], |$x.ma->toOne() / $x.mb->toOne()))
    ->datacube::ts::rampPeriod($line, $date, $first, ~cp, $n, $unit, ~o)
    ->without(~[ca, cb, ma, mb, cp])->rename(~o, $out)
}

// The down capture over a calendar period (downCapture's): as upCapturePeriod, on the periods the second fell.
function datacube::ts::downCapturePeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[ca:x|if($x->read($second)->toOne() < 0, |$x->read($first), |[]), cb:x|if($x->read($second)->toOne() < 0, |$x->read($second), |[])])
    ->datacube::ts::windowPeriod($line, $date, ~ca, $n, $unit, datacube::ts::Statistic.mean, ~ma)
    ->datacube::ts::windowPeriod($line, $date, ~cb, $n, $unit, datacube::ts::Statistic.mean, ~mb)
    ->extend(~cp:x|if($x.mb->toOne() == 0, |[], |$x.ma->toOne() / $x.mb->toOne()))
    ->datacube::ts::rampPeriod($line, $date, $first, ~cp, $n, $unit, ~o)
    ->without(~[ca, cb, ma, mb, cp])->rename(~o, $out)
}

// The lead-lag correlation over a calendar period (leadLag's, pairPeriod's).
function datacube::ts::leadLagPeriod<T,L,D,X,Y,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::pairPeriod($line, $date, $first, $second, $n, $unit, datacube::ts::PairStatistic.corr, ~c)
    ->datacube::ts::rampPeriod($line, $date, $first, ~c, $n, $unit, ~o)
    ->without(~c)->rename(~o, $out)
}

// Prices from returns (gs-quant's prices): initial at the first date, and each after it its returns to date compounded
// -- Returns.simple (the product of 1 + r), log (e to their sum) or absolute (their sum added); a return of none (the first) leaves the level where it was.
function datacube::ts::prices<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], initial: Number[1], type: datacube::ts::Returns[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~sm:{p,f,r|$r->read($value)}:y|$y->plus())
    ->extend(~g:x|1 + $x->read($value)->toOne())
    ->datacube::ts::windowProduct(over($line, [$date->ascending()], rows(unbounded(), 0)), ~g, ~pr)
    ->extend(~o:x|if($type == datacube::ts::Returns.absolute, |$initial + if($x.sm->isEmpty(), |0.0, |$x.sm->toOne()),
      |if($type == datacube::ts::Returns.log, |$initial * exp(if($x.sm->isEmpty(), |0.0, |$x.sm->toOne())), |$initial * $x.pr->toOne())))
    ->without(~[sm, g, pr])->rename(~o, $out)
}

// Dickey and Fuller's t-statistic over the window (statsmodels' adfuller of no lags, a constant): Δx = a + b·x[-1] by
// least squares, b their covariance over x[-1]'s variance, over its standard error √((var Δx − b² var x[-1]) / ((n − 2)
// var x[-1])) -- none of a perfect fit, or until the window holds ramp of them.
function datacube::ts::dickeyFuller<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~xl:{p,f,r|$p->lag($r)->read($value)})
    ->extend(~dx:x|$x->read($value)->toOne() - $x.xl->toOne())
    ->extend(~[pl:x|if($x.dx->isEmpty(), |[], |$x.xl), pd:x|if($x.xl->isEmpty(), |[], |$x.dx)])
    ->extend($win, ~[vl:{p,f,r|$r.pl}:y|$y->varianceSample(), cv:{p,f,r|rowMapper($r.pd, $r.pl)}:y|$y->covarSample(), vd:{p,f,r|$r.pd}:y|$y->varianceSample(), n:{p,f,r|$r.pl}:y|$y->count()])
    ->extend(~b:x|if($x.vl->toOne() == 0, |[], |$x.cv->toOne() / $x.vl->toOne()))
    ->extend(~rs:x|$x.vd->toOne() - (($x.b->toOne() * $x.b->toOne()) * $x.vl->toOne()))
    ->extend(~se:x|if($x.b->isEmpty() || ($x.n->toOne() < 3) || ($x.rs->toOne() <= 0), |[], |sqrt($x.rs->toOne() / (($x.n->toOne() - 2) * $x.vl->toOne()))))
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || ($x.se->toOne() == 0), |[], |$x.b->toOne() / $x.se->toOne()))
    ->without(~[xl, dx, pl, pd, vl, cv, vd, n, b, rs, se])->rename(~o, $out)
}

// The half-life of a reversion to the mean over the window: −ln 2 / ln(1 + b), b of Δx = a + b·x[-1] -- none where it
// does not revert (b at or above 0), overshoots past its mean (b at or below -1), or until the window holds ramp of them.
function datacube::ts::halfLife<T,V,O>(t: Relation<T>[1], win: _Window<T>[1], value: ColSpec<V⊆T>[1], ramp: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend($win, ~xl:{p,f,r|$p->lag($r)->read($value)})
    ->extend(~dx:x|$x->read($value)->toOne() - $x.xl->toOne())
    ->extend(~[pl:x|if($x.dx->isEmpty(), |[], |$x.xl), pd:x|if($x.xl->isEmpty(), |[], |$x.dx)])
    ->extend($win, ~[vl:{p,f,r|$r.pl}:y|$y->varianceSample(), cv:{p,f,r|rowMapper($r.pd, $r.pl)}:y|$y->covarSample(), n:{p,f,r|$r.pl}:y|$y->count()])
    ->extend(~b:x|if($x.vl->toOne() == 0, |[], |$x.cv->toOne() / $x.vl->toOne()))
    ->extend(~o:x|if(($x.n->toOne() < $ramp) || $x.b->isEmpty() || ($x.b->toOne() >= 0) || ($x.b->toOne() <= -1), |[], |(0.0 - log(2.0)) / log(1 + $x.b->toOne())))
    ->without(~[xl, dx, pl, pd, vl, cv, n, b])->rename(~o, $out)
}

// Dickey and Fuller's t-statistic over a calendar period back of each date (dickeyFuller's): Δx = a + b·x[-1] over
// the period's pairs, b's estimate over its standard error; none before the first pair's date that period on.
function datacube::ts::dickeyFullerPeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date)])
    ->extend(over(~l, [~d->ascending()]), ~xl:{p,f,r|$p->lag($r)->read($value)})
    ->extend(~dx:x|$x->read($value)->toOne() - $x.xl->toOne())
    ->datacube::ts::pairs(~dx, ~xl, ~pd, ~pl)
    ->datacube::ts::pairPeriod($line, $date, ~pd, ~pl, $n, $unit, datacube::ts::PairStatistic.cov, ~cv)
    ->datacube::ts::windowPeriod($line, $date, ~pl, $n, $unit, datacube::ts::Statistic.var, ~vl)
    ->datacube::ts::windowPeriod($line, $date, ~pd, $n, $unit, datacube::ts::Statistic.var, ~vd)
    ->datacube::ts::windowPeriod($line, $date, ~pl, $n, $unit, datacube::ts::Statistic.count, ~c)
    ->extend(~b:x|if($x.vl->toOne() == 0, |[], |$x.cv->toOne() / $x.vl->toOne()))
    ->extend(~rs:x|$x.vd->toOne() - (($x.b->toOne() * $x.b->toOne()) * $x.vl->toOne()))
    ->extend(~se:x|if($x.b->isEmpty() || ($x.c->toOne() < 3) || ($x.rs->toOne() <= 0), |[], |sqrt($x.rs->toOne() / (($x.c->toOne() - 2) * $x.vl->toOne()))))
    ->extend(~tt:x|if($x.se->toOne() == 0, |[], |$x.b->toOne() / $x.se->toOne()))
    ->datacube::ts::rampPeriod($line, $date, ~pl, ~tt, $n, $unit, ~o)
    ->without(~[l, d, xl, dx, pd, pl, cv, vl, vd, c, b, rs, se, tt])->rename(~o, $out)
}

// The half-life of mean reversion over a calendar period back of each date (halfLife's): -ln 2 / ln(1 + b), b the
// slope of Δx on x[-1] over the period's pairs; none where it does not revert (b at or above 0) or overshoots (b at or
// below -1), or before the first pair's date that period on.
function datacube::ts::halfLifePeriod<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], n: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date)])
    ->extend(over(~l, [~d->ascending()]), ~xl:{p,f,r|$p->lag($r)->read($value)})
    ->extend(~dx:x|$x->read($value)->toOne() - $x.xl->toOne())
    ->datacube::ts::pairs(~dx, ~xl, ~pd, ~pl)
    ->datacube::ts::slopePeriod($line, $date, ~pd, ~pl, $n, $unit, ~b)
    ->extend(~hl:x|if($x.b->isEmpty() || ($x.b->toOne() >= 0) || ($x.b->toOne() <= -1), |[], |(0.0 - log(2.0)) / log(1 + $x.b->toOne())))
    ->datacube::ts::rampPeriod($line, $date, ~pl, ~hl, $n, $unit, ~o)
    ->without(~[l, d, xl, dx, pd, pl, b, hl])->rename(~o, $out)
}

// The excess returns over a rate (gs-quant's excess_returns of a number): x less the rate accrued on it period by
// period, actual/360 -- er_j = er_j-1 + x_j - x_j-1 (1 + rate f_j), summed: x_j - rate × Σ x_k-1 f_k to date, f a
// period's days over 360: the difference of its time (a number, each row's: its last observation's, counted in
// timeUnit -- DAYS or SECONDS) from the one before, over the timeUnits a day has (the database's dateDiff: 1; 86400).
function datacube::ts::excessReturns<T,L,D,V,N,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], time: ColSpec<N⊆T>[1], timeUnit: meta::pure::functions::date::DurationUnit[1], rate: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()]), ~ac:{p,f,r|$p->lag($r)->read($value)->toOne() * ((($r->read($time)->toOne() - $p->lag($r)->read($time)->toOne()) / dateDiff(%1970-01-01, %1970-01-02, $timeUnit)) / 360)})
    ->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~tot:{p,f,r|$r.ac}:y|$y->plus())
    ->extend(~o:x|$x->read($value)->toOne() - ($rate * if($x.tot->isEmpty(), |0.0, |$x.tot->toOne())))
    ->without(~[ac, tot])->rename(~o, $out)
}

// The excess returns over a cash index (gs-quant's excess_returns of a series): on the dates both have, x's first value
// times the product of (1 + x's return − the index's) to date; none where x is none.
function datacube::ts::excessReturnsOver<T,L,D,V,C,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], cash: ColSpec<C⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::returns(over($line, [$date->ascending()]), $value, 1, datacube::ts::Returns.simple, ~rx)
    ->datacube::ts::returns(over($line, [$date->ascending()]), $cash, 1, datacube::ts::Returns.simple, ~ry)
    ->extend(~g:x|(1 + $x.rx->toOne()) - $x.ry->toOne())
    ->datacube::ts::windowProduct(over($line, [$date->ascending()], rows(unbounded(), 0)), ~g, ~pr)
    ->datacube::ts::first($line, $date, $value, ~f0)
    ->extend(~o:x|if($x->read($value)->isEmpty(), |[], |$x.f0->toOne() * $x.pr->toOne()))
    ->without(~[rx, ry, g, pr, f0])->rename(~o, $out)
}

// Which of two columns is greater (gs-quant's compare, of series aligned by the caller): 1, -1, or 0 where they are
// equal or either is none ((x > y) - (x < y)).
function datacube::ts::compare<T,X,Y,O>(t: Relation<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($first)->toOne() > $x->read($second)->toOne(), |1, |if($x->read($first)->toOne() < $x->read($second)->toOne(), |-1, |0)))->rename(~o, $out)
}

// A branch by a column of flags (gs-quant's if_): yes where the flag is not 0, no where it is -- none where the flag is
// none.
function datacube::ts::ifThen<T,F,X,Y,O>(t: Relation<T>[1], flags: ColSpec<F⊆T>[1], yes: ColSpec<X⊆T>[1], no: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($flags)->isEmpty(), |[], |if(!($x->read($flags)->toOne() == 0), |$x->read($yes), |$x->read($no))))->rename(~o, $out)
}

// The first column where it has a value, else the second (gs-quant's union, of series aligned by the caller).
function datacube::ts::union<T,X,Y,O>(t: Relation<T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:x|if($x->read($first)->isNotEmpty(), |$x->read($first), |$x->read($second)))->rename(~o, $out)
}

// The first series' rows to its last, then the second's after it (gs-quant's append, of series aligned by the caller:
// hasFirst and hasSecond whether each has a row on the date) -- the other rows dropped -- its value the first's where
// that has a row, else the second's.
function datacube::ts::append<T,L,D,X,Y,HX,HY,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], hasFirst: ColSpec<HX⊆T>[1], hasSecond: ColSpec<HY⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line), ~xe:{p,f,r|if($r->read($hasFirst)->isNotEmpty(), |$r->read($date), |[])}:y|$y->max())
    ->filter(x|$x->read($hasFirst)->isNotEmpty() || ($x->read($hasSecond)->isNotEmpty() && ($x.xe->isEmpty() || ($x->read($date)->toOne() > $x.xe->toOne()))))
    ->extend(~o:x|if($x->read($hasFirst)->isNotEmpty(), |$x->read($first), |$x->read($second)))
    ->without(~xe)->rename(~o, $out)
}

// The first series' rows before the second's first, then the second's every row (gs-quant's prepend, of series aligned
// by the caller as append's) -- the other rows dropped -- its value the second's where that has a row, else the first's.
function datacube::ts::prepend<T,L,D,X,Y,HX,HY,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], hasFirst: ColSpec<HX⊆T>[1], hasSecond: ColSpec<HY⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line), ~ys:{p,f,r|if($r->read($hasSecond)->isNotEmpty(), |$r->read($date), |[])}:y|$y->min())
    ->filter(x|$x->read($hasSecond)->isNotEmpty() || ($x->read($hasFirst)->isNotEmpty() && ($x.ys->isEmpty() || ($x->read($date)->toOne() < $x.ys->toOne()))))
    ->extend(~o:x|if($x->read($hasSecond)->isNotEmpty(), |$x->read($second), |$x->read($first)))
    ->without(~ys)->rename(~o, $out)
}

// The trend of a seasonal series (gs-quant's trend, statsmodels' classical decomposition): a centred moving average over
// a cycle of cycle periods, half its reach either side (cycle / 2, its floor: a window's frame, a literal) -- of an
// odd cycle the mean of the values either side, of an even one its two ends weighed a half -- none under two whole
// cycles, or where the window is short of them (the line's ends).
function datacube::ts::trend<T,L,D,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], cycle: Integer[1], half: Integer[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()], rows(0 - $half, $half)), ~[n:{p,f,r|$r->read($value)}:y|$y->count(), sm:{p,f,r|$r->read($value)}:y|$y->plus()])
    ->extend(over($line, [$date->ascending()]), ~lg:{p,f,r|$p->lag($r, $half)->read($value)})
    ->extend(over($line, [$date->ascending()]), ~ld:{p,f,r|$p->lead($r, $half)->read($value)})
    ->extend(over($line, [$date->ascending()]), ~rn:{p,f,r|$p->rowNumber($r)})
    ->extend(over($line), ~rs:{p,f,r|$r.rn}:y|$y->count())
    ->extend(~o:x|if(($x.rs->toOne() < (2 * $cycle)) || ($x.n->toOne() < if(($cycle - (2 * $half)) == 1, |$cycle, |$cycle + 1)), |[],
      |if(($cycle - (2 * $half)) == 1, |$x.sm->toOne() / $cycle, |($x.sm->toOne() - (0.5 * ($x.lg->toOne() + $x.ld->toOne()))) / $cycle)))
    ->without(~[n, sm, lg, ld, rn, rs])->rename(~o, $out)
}

// The series less its season (gs-quant's seasonally_adjusted, statsmodels' classical decomposition): each value less
// (over, SeasonalModel.multiplicative) its place in the cycle's mean of the detrended values (place, a column: each row's number in
// the cycle's calendar), those means centred on their own mean -- none where the trend (trend's) is none.
function datacube::ts::seasonallyAdjusted<T,L,D,V,P,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], place: ColSpec<P⊆T>[1], cycle: Integer[1], half: Integer[1], method: datacube::ts::SeasonalModel[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::trend($line, $date, $value, $cycle, $half, ~tr)
    ->extend(~dt:x|if($method == datacube::ts::SeasonalModel.multiplicative, |if($x.tr->toOne() == 0, |[], |$x->read($value)->toOne() / $x.tr->toOne()), |$x->read($value)->toOne() - $x.tr->toOne()))
    ->extend(~[l:x|$x->read($line), ps:x|$x->read($place)->toOne() - (floor($x->read($place)->toOne() / $cycle) * $cycle)])
    ->extend(over(~[l, ps]), ~[pm:{p,f,r|$r.dt}:y|$y->average(), pr:{p,f,r|1}:y|$y->count()])
    ->extend(over(~l), ~al:{p,f,r|$r.pm->toOne() / $r.pr->toOne()}:y|$y->plus())
    ->extend(~ss:x|if($method == datacube::ts::SeasonalModel.multiplicative, |$x.pm->toOne() / ($x.al->toOne() / $cycle), |$x.pm->toOne() - ($x.al->toOne() / $cycle)))
    ->extend(~o:x|if($x.tr->isEmpty(), |[], |if($method == datacube::ts::SeasonalModel.multiplicative, |$x->read($value)->toOne() / $x.ss->toOne(), |$x->read($value)->toOne() - $x.ss->toOne())))
    ->without(~[tr, dt, l, ps, pm, pr, al, ss])->rename(~o, $out)
}

// The total return of a price with a yield a year accrued day by day (gs-quant's total_return of a number): its first
// value, then each period's growth compounded -- its price over the one before, plus the yield over the days since (the
// difference of its time -- a number, each row's, counted in timeUnit -- from the one before, over the timeUnits a day
// has: 1; 86400) over 365; none where the price is none.
function datacube::ts::totalReturn<T,L,D,P,N,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], price: ColSpec<P⊆T>[1], time: ColSpec<N⊆T>[1], timeUnit: meta::pure::functions::date::DurationUnit[1], yield: Number[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$date->ascending()]), ~g:{p,f,r|($r->read($price)->toOne() / $p->lag($r)->read($price)->toOne()) + ($yield * ((($r->read($time)->toOne() - $p->lag($r)->read($time)->toOne()) / dateDiff(%1970-01-01, %1970-01-02, $timeUnit)) / 365))})
    ->datacube::ts::windowProduct(over($line, [$date->ascending()], rows(unbounded(), 0)), ~g, ~pr)
    ->datacube::ts::first($line, $date, $price, ~f0)
    ->extend(~o:x|if($x->read($price)->isEmpty(), |[], |$x.f0->toOne() * $x.pr->toOne()))
    ->without(~[g, pr, f0])->rename(~o, $out)
}

// The total return of a price with its dividends reinvested on their dates (gs-quant's total_return of a series; the
// two aligned by the caller, each on its own dates: a price of none on a dividend's date alone): (price + the dividends
// since the price before) / the price before, on the price's dates -- a dividend of a date the price has none of paid
// at its next -- compounded from its first value; the rows of a dividend alone dropped.
function datacube::ts::totalReturnOf<T,L,D,P,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], price: ColSpec<P⊆T>[1], dividend: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~dv:x|if($x->read($dividend)->isEmpty(), |0.0, |$x->read($dividend)->toOne()))
    ->extend(over($line, [$date->ascending()], rows(unbounded(), 0)), ~pd:{p,f,r|$r.dv}:y|$y->plus())
    ->filter(x|$x.pd->isNotEmpty() && $x->read($price)->isNotEmpty())
    ->extend(over($line, [$date->ascending()]), ~g:{p,f,r|($r->read($price)->toOne() + ($r.pd->toOne() - $p->lag($r).pd->toOne())) / $p->lag($r)->read($price)->toOne()})
    ->datacube::ts::windowProduct(over($line, [$date->ascending()], rows(unbounded(), 0)), ~g, ~pr)
    ->datacube::ts::first($line, $date, $price, ~f0)
    ->extend(~o:x|if($x->read($price)->isEmpty(), |[], |$x.f0->toOne() * $x.pr->toOne()))
    ->without(~[dv, pd, g, pr, f0])->rename(~o, $out)
}

// -- many series as a long table: a row a member and date (kdb+'s, panel pandas', Polars' model), its value and weight
// beside it --

// The weighted sum of the members at each date (gs-quant's weighted_sum): Σ weight × value over Σ weight, on the dates
// all members (members of them) have a value -- one row for each of the keys (by: the line and the date).
function datacube::ts::weightedSum<T,Z,V,W,O>(t: Relation<T>[1], by: ColSpecArray<Z⊆T>[1], value: ColSpec<V⊆T>[1], weight: ColSpec<W⊆T>[1], members: Integer[1], out: ColSpec<O>[1]): Relation<Z+O>[1]
{
  $t->filter(x|$x->read($value)->isNotEmpty())
    ->groupBy($by, ~[s:x|$x->read($weight)->toOne() * $x->read($value)->toOne():y|$y->plus(), ws:x|$x->read($weight):y|$y->plus(), n:x|$x->read($value):y|$y->count()])
    ->filter(x|$x.n == $members)
    ->extend(~o:x|$x.s->toOne() / $x.ws->toOne())
    ->without(~[s, ws, n])->rename(~o, $out)
}

// A basket of members held in weights from 100 (gs-quant's backtesting basket_series, its costs none): units fixed
// between rebalances, set at each to the basket's then in its weights. Each date's rebalance period its own (Rebalance.daily),
// or the weeks or the months (weekly, monthly) from the line's first date -- a rebalance the first date on or after
// each, a month's day kept or its last where it has none -- a week's days counted by time (a number, each row's: its
// last observation's, counted in timeUnit) over the timeUnits a day has (1; 86400). Within a period, 1 + Σ weight × (price / its price at the period's first date − 1); into it, the
// period before's growth to its first date; the basket 100 times those compounded, times its growth within -- none from
// a date some member's price is none on. One row a line and date.
function datacube::ts::basket<T,L,D,N,M,P,W,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], time: ColSpec<N⊆T>[1], member: ColSpec<M⊆T>[1], price: ColSpec<P⊆T>[1], weight: ColSpec<W⊆T>[1], members: Integer[1], rebalance: datacube::ts::Rebalance[1], timeUnit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<L+D+O>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), tm:r|$r->read($time), m:r|$r->read($member), px:r|$r->read($price), wt:r|$r->read($weight)])
    ->extend(over(~l), ~[d0:{p,f,r|$r.d}:y|$y->min(), t0:{p,f,r|$r.tm}:y|$y->min()])
    ->extend(~mo:x|((($x.d->toOne()->year() - $x.d0->toOne()->year()) * 12) + ($x.d->toOne()->monthNumber() - $x.d0->toOne()->monthNumber())))
    ->extend(~k:x|if($rebalance == datacube::ts::Rebalance.daily, |$x.tm->toOne(),
      |if($rebalance == datacube::ts::Rebalance.weekly, |floor((($x.tm->toOne() - $x.t0->toOne()) / dateDiff(%1970-01-01, %1970-01-02, $timeUnit)) / 7),
        |$x.mo->toOne() - if($x.d->toOne() < $x.d0->toOne()->adjust($x.mo->toOne(), meta::pure::functions::date::DurationUnit.MONTHS), |1, |0))))
    ->extend(over(~[l, k]), ~fd:{p,f,r|$r.d}:y|$y->min())
    ->extend(over(~[l, m, k]), ~bs:{p,f,r|if($r.d == $r.fd, |$r.px, |[])}:y|$y->max())
    ->extend(over(~[l, m], [~d->ascending()]), ~bf:{p,f,r|$p->lag($r).bs})
    ->groupBy(~[l, d], ~[wi:x|$x.wt->toOne() * (($x.px->toOne() / $x.bs->toOne()) - 1):y|$y->plus(),
      wo:x|$x.wt->toOne() * (($x.px->toOne() / $x.bf->toOne()) - 1):y|$y->plus(),
      af:x|if($x.d == $x.fd, |1, |0):y|$y->max(), nb:x|$x.bf:y|$y->count(), n:x|$x.px:y|$y->count()])
    ->extend(~gi:x|if(($x.af->toOne() == 1) && ($x.nb->toOne() > 0), |1 + $x.wo->toOne(), |[]))
    ->datacube::ts::windowProduct(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~gi, ~pr)
    ->extend(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~gp:{p,f,r|if($r.n->toOne() < $members, |1, |[])}:y|$y->count())
    ->extend(~o:x|if($x.gp->toOne() > 0, |[], |100.0 * ($x.pr->toOne() * (1 + $x.wi->toOne()))))
    ->select(~[l, d, o])->rename(~l, $line)->rename(~d, $date)->rename(~o, $out)
}

// THE TILE'S TABLES AND VIEWS (the focused Pure audit, 2026-10-28: their maths the library's, as the catalog's is).

// The periods a year a line's dates are (gs-quant's econometrics._get_annualization_factor): the average days between
// them -- its time's span over their count less one, its time counted in timeUnit (a day 1 of DAYS, 86400 of SECONDS) --
// bucketed: under 2.1 days, 252; 6 to 8, 52; 14 to 17, 26; 25 to 35, 12; 85 to 97, 4; 360 to 386, 1; else, or under two
// dates, none.
function datacube::ts::periodsPerYear<T,L,N,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], time: ColSpec<N⊆T>[1], timeUnit: meta::pure::functions::date::DurationUnit[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), tm:r|$r->read($time)])
    ->extend(over(~l), ~[hi:{p,f,r|$r.tm}:y|$y->max(), lo:{p,f,r|$r.tm}:y|$y->min(), n:{p,f,r|$r.tm}:y|$y->count()])
    ->extend(~g:x|if($x.n->toOne() < 2, |[], |(($x.hi->toOne() - $x.lo->toOne()) / dateDiff(%1970-01-01, %1970-01-02, $timeUnit)) / ($x.n->toOne() - 1)))
    ->extend(~o:x|if($x.g->isEmpty(), |[],
      |if(($x.g->toOne() >= 1.0) && ($x.g->toOne() < 2.1), |252,
      |if(($x.g->toOne() >= 6.0) && ($x.g->toOne() < 8.0), |52,
      |if(($x.g->toOne() >= 14.0) && ($x.g->toOne() < 17.0), |26,
      |if(($x.g->toOne() >= 25.0) && ($x.g->toOne() < 35.0), |12,
      |if(($x.g->toOne() >= 85.0) && ($x.g->toOne() < 97.0), |4,
      |if(($x.g->toOne() >= 360.0) && ($x.g->toOne() < 386.0), |1, |[]))))))))
    ->without(~[l, tm, hi, lo, n, g])->rename(~o, $out)
}

// The least-squares line of the first on the second over the window's pairs (ordinary least squares, as gs-quant's
// beta and alpha are): its slope (their sample covariance over the second's variance), its intercept (through their
// means), their correlation and its square -- none until the window holds ramp pairs, none where the second does not
// vary, and no correlation where the first does not.
function datacube::ts::regression<T,Y,X,O1,O2,O3,O4>(t: Relation<T>[1], win: _Window<T>[1], first: ColSpec<Y⊆T>[1], second: ColSpec<X⊆T>[1], ramp: Integer[1], slope: ColSpec<O1>[1], intercept: ColSpec<O2>[1], corr: ColSpec<O3>[1], r2: ColSpec<O4>[1]): Relation<T+O1+O2+O3+O4>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->extend($win, ~[cv:{p,f,r|rowMapper($r.pa, $r.pb)}:y|$y->covarSample(), va:{p,f,r|$r.pa}:y|$y->varianceSample(), vb:{p,f,r|$r.pb}:y|$y->varianceSample(), ma:{p,f,r|$r.pa}:y|$y->average(), mb:{p,f,r|$r.pb}:y|$y->average(), n:{p,f,r|$r.pa}:y|$y->count()])
    ->extend(~fl:x|($x.n->toOne() < $ramp) || $x.vb->isEmpty() || ($x.vb->toOne() == 0))
    ->extend(~[s:x|if($x.fl, |[], |$x.cv->toOne() / $x.vb->toOne()),
      c:x|if($x.fl || $x.va->isEmpty() || ($x.va->toOne() == 0), |[], |$x.cv->toOne() / sqrt($x.va->toOne() * $x.vb->toOne()))])
    ->extend(~[i:x|if($x.s->isEmpty(), |[], |$x.ma->toOne() - ($x.s->toOne() * $x.mb->toOne())), q:x|if($x.c->isEmpty(), |[], |$x.c->toOne() * $x.c->toOne())])
    ->without(~[pa, pb, cv, va, vb, ma, mb, n, fl])->rename(~s, $slope)->rename(~i, $intercept)->rename(~c, $corr)->rename(~q, $r2)
}

// A line's value at x: its intercept plus its slope times x, none where either is none.
function datacube::ts::fitAt<T,I,S,X,O>(t: Relation<T>[1], intercept: ColSpec<I⊆T>[1], slope: ColSpec<S⊆T>[1], x: ColSpec<X⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($r->read($slope)->isEmpty() || $r->read($intercept)->isEmpty(), |[], |$r->read($intercept)->toOne() + ($r->read($slope)->toOne() * $r->read($x)->toOne())))->rename(~o, $out)
}

// A TREND PROJECTED, a row a line: its least-squares line of its value on its time (regression's) over its last window
// values, and that line's value at the first of them, at the last, and at the date ahead units past the last -- those
// three dates and values, the line's slope a day and how many
// values it was fit to; none of the fit under two values of different times. Its time counts timeUnits (days, or
// seconds of date-times): a day's slope its slope times the timeUnits in a day, the database's.
function datacube::ts::projection<T,L,D,N,V,O1,O2,O3,O4,O5,O6,O7,O8>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], time: ColSpec<N⊆T>[1], value: ColSpec<V⊆T>[1], window: Integer[1], ahead: Integer[1], unit: meta::pure::functions::date::DurationUnit[1], timeUnit: meta::pure::functions::date::DurationUnit[1], firstDate: ColSpec<O1>[1], firstValue: ColSpec<O2>[1], lastDate: ColSpec<O3>[1], lastValue: ColSpec<O4>[1], endDate: ColSpec<O5>[1], endValue: ColSpec<O6>[1], slope: ColSpec<O7>[1], n: ColSpec<O8>[1]): Relation<L+O1+O2+O3+O4+O5+O6+O7+O8>[1]
{
  $t->filter(r|$r->read($value)->isNotEmpty())
    ->extend(~[l:r|$r->read($line), d:r|$r->read($date), tm:r|$r->read($time), v:r|$r->read($value)])
    ->extend(over(~l, [~tm->ascending()], rows(0, unbounded())), ~rest:{p,f,r|$r.v}:y|$y->count())
    ->filter(x|$x.rest->toOne() <= $window)
    ->extend(over(~l), ~[k:{p,f,r|$r.tm}:y|$y->count(), t0:{p,f,r|$r.tm}:y|$y->min(), d0:{p,f,r|$r.d}:y|$y->min()])
    ->datacube::ts::regression(over(~l), ~v, ~tm, 2, ~s, ~i, ~c, ~q)
    ->filter(x|$x.rest->toOne() == 1)
    ->extend(~e:x|$x.d->toOne()->adjust($ahead, $unit))
    ->extend(~te:x|$x.tm->toOne() + dateDiff($x.d->toOne(), $x.e->toOne(), $timeUnit))
    ->datacube::ts::fitAt(~i, ~s, ~t0, ~f0)->datacube::ts::fitAt(~i, ~s, ~tm, ~f1)->datacube::ts::fitAt(~i, ~s, ~te, ~f2)
    ->extend(~sd:x|if($x.s->isEmpty(), |[], |$x.s->toOne() * dateDiff($x.d->toOne(), $x.d->toOne()->adjust(1, meta::pure::functions::date::DurationUnit.DAYS), $timeUnit)))
    ->select(~[l, d0, f0, d, f1, e, f2, sd, k])
    ->rename(~l, $line)->rename(~d0, $firstDate)->rename(~f0, $firstValue)->rename(~d, $lastDate)->rename(~f1, $lastValue)->rename(~e, $endDate)
    ->rename(~f2, $endValue)->rename(~sd, $slope)->rename(~k, $n)
}

// The value at x on the line through (x0, y0) and (x1, y1): linear between two values in time (pandas' interpolate by
// time, gs-quant's align and interpolate 'time'), y0 where x0 is x1, none where y0 or y1 is none.
function datacube::ts::linearAt<T,A,B,P,Q,X,O>(t: Relation<T>[1], x0: ColSpec<A⊆T>[1], y0: ColSpec<B⊆T>[1], x1: ColSpec<P⊆T>[1], y1: ColSpec<Q⊆T>[1], x: ColSpec<X⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($r->read($y0)->isEmpty() || $r->read($y1)->isEmpty(), |[],
    |if($r->read($x0)->toOne() == $r->read($x1)->toOne(), |$r->read($y0),
      |$r->read($y0)->toOne() + ((($r->read($y1)->toOne() - $r->read($y0)->toOne()) * ($r->read($x)->toOne() - $r->read($x0)->toOne())) / ($r->read($x1)->toOne() - $r->read($x0)->toOne())))))
    ->rename(~o, $out)
}

// ALIGNMENT (the user, 2026-10-28, "do timeseries RIGHT and EXPERT on platform": what the tile wrote as joins and windows
// to put two series on one set of dates, functions a person's own Pure calls): a part's values put onto every date of
// its line, as gs-quant's align puts a series. A part is the rows one series has; its marker the column whose value
// says a date is the part's (its first: a part's columns are one row's), so each of its columns is put onto the same
// dates.

// The period of its line a part last has a value at, at or before each date (a running maximum of its periods); none
// before its first.
function datacube::ts::lastValued<T,L,P,M,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], order: ColSpec<P⊆T>[1], marker: ColSpec<M⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$order->ascending()], rows(unbounded(), 0)), ~o:{p,f,r|if($r->read($marker)->isNotEmpty(), |$r->read($order), |[])}:y|$y->max())
    ->rename(~o, $out)
}

// The period of its line a part first has a value at, on every date of it.
function datacube::ts::firstValued<T,L,P,M,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], order: ColSpec<P⊆T>[1], marker: ColSpec<M⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$order->ascending()], rows(unbounded(), unbounded())), ~o:{p,f,r|if($r->read($marker)->isNotEmpty(), |$r->read($order), |[])}:y|$y->min())
    ->rename(~o, $out)
}

// The period of its line a part next has a value at, at or after each date; none after its last.
function datacube::ts::nextValued<T,L,P,M,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], order: ColSpec<P⊆T>[1], marker: ColSpec<M⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(over($line, [$order->ascending()], rows(0, unbounded())), ~o:{p,f,r|if($r->read($marker)->isNotEmpty(), |$r->read($order), |[])}:y|$y->min())
    ->rename(~o, $out)
}

// A column's value at the period another column names (lastValued's, firstValued's, nextValued's), on each date of the
// line; none where it names none.
function datacube::ts::valueAt<T,L,P,Q,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], order: ColSpec<P⊆T>[1], at: ColSpec<Q⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~[l:r|$r->read($line), q:r|$r->read($at)])
    ->extend(over(~[l, q], [$order->ascending()], rows(unbounded(), unbounded())), ~o:{p,f,r|if($r->read($order) == $r.q, |$r->read($value), |[])}:y|$y->max())
    ->without(~[l, q])->rename(~o, $out)
}

// A column where it has a value, else another (a step's last value, else its first).
function datacube::ts::orElse<T,V,W,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], otherwise: ColSpec<W⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($r->read($value)->isEmpty(), |$r->read($otherwise), |$r->read($value)))->rename(~o, $out)
}

// A part's column, 0 where it has none (gs-quant's align 'zero').
function datacube::ts::fillZero<T,V,O>(t: Relation<T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($r->read($value)->isEmpty(), |0, |$r->read($value)))->rename(~o, $out)
}

// A part's column on every date of its line by step (gs-quant's align 'step', pandas' ffill then bfill): each date its
// value at the period it last has one at, before its first its first.
function datacube::ts::fillStep<T,L,P,M,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], order: ColSpec<P⊆T>[1], marker: ColSpec<M⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::lastValued($line, $order, $marker, ~lp)->datacube::ts::firstValued($line, $order, $marker, ~fp)
    ->datacube::ts::valueAt($line, $order, ~lp, $value, ~a)->datacube::ts::valueAt($line, $order, ~fp, $value, ~b)
    ->datacube::ts::orElse(~a, ~b, ~o)
    ->without(~[lp, fp, a, b])->rename(~o, $out)
}

// A part's column on every date of its line by time (gs-quant's align 'time', pandas' interpolate by time with
// limit_area 'inside'): linear in time between its values at the periods before and after (linearAt) -- time the part's
// own (its rows' time), at the row's -- none outside them.
function datacube::ts::fillTime<T,L,P,M,N,X,V,O>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], order: ColSpec<P⊆T>[1], marker: ColSpec<M⊆T>[1], time: ColSpec<N⊆T>[1], at: ColSpec<X⊆T>[1], value: ColSpec<V⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->datacube::ts::lastValued($line, $order, $marker, ~lp)->datacube::ts::nextValued($line, $order, $marker, ~np)
    ->datacube::ts::valueAt($line, $order, ~lp, $time, ~t0)->datacube::ts::valueAt($line, $order, ~np, $time, ~t1)
    ->datacube::ts::valueAt($line, $order, ~lp, $value, ~a)->datacube::ts::valueAt($line, $order, ~np, $value, ~b)
    ->datacube::ts::linearAt(~t0, ~a, ~t1, ~b, $at, ~o)
    ->without(~[lp, np, t0, t1, a, b])->rename(~o, $out)
}

// The correlation of each pair of lines (by: the two columns that name them) over the rows they share: of the pairs
// where both have a value, their correlation -- none where a side does not vary (DuckDB's corr says NaN there,
// Postgres' none) -- and how many pairs; a row a pair of lines (the statistics panel's Correlations tab).
function datacube::ts::correlationOf<T,Z,X,Y,O,N>(t: Relation<T>[1], by: ColSpecArray<Z⊆T>[1], first: ColSpec<X⊆T>[1], second: ColSpec<Y⊆T>[1], corr: ColSpec<O>[1], n: ColSpec<N>[1]): Relation<Z+O+N>[1]
{
  $t->datacube::ts::pairs($first, $second, ~pa, ~pb)
    ->groupBy($by, ~[c:x|rowMapper($x.pa, $x.pb):y|$y->corr(), k:x|$x.pa:y|$y->count(), va:x|$x.pa:y|$y->varianceSample(), vb:x|$x.pb:y|$y->varianceSample()])
    ->extend(~o:x|if($x.va->isEmpty() || $x.vb->isEmpty() || ($x.va->toOne() == 0) || ($x.vb->toOne() == 0), |[], |$x.c))
    ->without(~[c, va, vb])->rename(~o, $corr)->rename(~k, $n)
}
// A line's own figures, a row a line (the statistics table's, beside performanceStatistics' of a level): how many
// values it has, its first and last dates, its first and last values (by time: a number, a minBy's and maxBy's key),
// its least and its most (of low and high: a candle's lows and highs, a line's values) and the dates they are at, its
// values' mean and sample deviation, the calendar days from its first date to its last, its change (its last less its
// first) and its % change (gs-quant's x / x0 - 1, percent_change's: none from a first of 0).
function datacube::ts::summaryStatistics<T,L,D,N,V,W,H,O1,O2,O3,O4,O5,O6,O7,O8,O9,O10,O11,O12,O13,O14>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], time: ColSpec<N⊆T>[1], value: ColSpec<V⊆T>[1], low: ColSpec<W⊆T>[1], high: ColSpec<H⊆T>[1], count: ColSpec<O1>[1], firstDate: ColSpec<O2>[1], lastDate: ColSpec<O3>[1], first: ColSpec<O4>[1], last: ColSpec<O5>[1], least: ColSpec<O6>[1], leastDate: ColSpec<O7>[1], most: ColSpec<O8>[1], mostDate: ColSpec<O9>[1], mean: ColSpec<O10>[1], std: ColSpec<O11>[1], days: ColSpec<O12>[1], change: ColSpec<O13>[1], pct: ColSpec<O14>[1]): Relation<L+O1+O2+O3+O4+O5+O6+O7+O8+O9+O10+O11+O12+O13+O14>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), tm:r|$r->read($time), v:r|$r->read($value), lw:r|$r->read($low), hg:r|$r->read($high)])
    ->groupBy(~[l], ~[n:x|$x.v:y|$y->count(), d0:x|$x.d:y|$y->min(), d1:x|$x.d:y|$y->max(), v0:x|rowMapper($x.v, $x.tm):y|$y->minBy(),
      v1:x|rowMapper($x.v, $x.tm):y|$y->maxBy(), lo:x|$x.lw:y|$y->min(), la:x|rowMapper($x.d, $x.lw):y|$y->minBy(), hi:x|$x.hg:y|$y->max(),
      ha:x|rowMapper($x.d, $x.hg):y|$y->maxBy(), m:x|$x.v:y|$y->average(), s:x|$x.v:y|$y->stdDevSample()])
    ->extend(~[dy:x|dateDiff($x.d0->toOne(), $x.d1->toOne(), meta::pure::functions::date::DurationUnit.DAYS), ch:x|$x.v1->toOne() - $x.v0->toOne(),
      pc:x|if($x.v0->toOne() == 0, |[], |($x.v1->toOne() / $x.v0->toOne()) - 1)])
    ->rename(~l, $line)->rename(~n, $count)->rename(~d0, $firstDate)->rename(~d1, $lastDate)->rename(~v0, $first)->rename(~v1, $last)
    ->rename(~lo, $least)->rename(~la, $leastDate)->rename(~hi, $most)->rename(~ha, $mostDate)->rename(~m, $mean)->rename(~s, $std)
    ->rename(~dy, $days)->rename(~ch, $change)->rename(~pc, $pct)
}


// A line's performance statistics, in one pass over it (pyfolio's perf_stats; the statistics panel's): of its level, its
// simple and its log returns and its periods a year (a column, a line's one) --
//   vol: its log returns' sample deviation, annualized, in percent (gs-quant's volatility over the line; two at least);
//   sharpe: its returns' mean over their sample deviation, annualized (gs-quant's sharpe_ratio; two at least, none of a
//     deviation of 0);
//   drawdown: its level's deepest fall from its running peak, by date (gs-quant's max_drawdown);
//   growth: its compound annual growth, its first value to its last by date (by time: a number, a maxBy's key): (last / first)^(365.25 / their days) - 1,
//     over a calendar year or more alone (a return of under a year is never annualized, GIPS'), none from a first of 0;
//   skew, kurtosis: its returns' sample skewness and excess kurtosis about their mean (pandas' skew and kurt,
//     bias-corrected; three and four at least, none where they do not vary);
//   sortino: their mean over the root mean square of those below zero (the others 0), annualized; none where none is;
//   hit: the share of them above zero;
//   valueAtRisk: their q-quantile (continuous, pandas'); shortfall: the mean of those at or below it -- those ranked (ties
//     at their least) at most floor((n - 1)·q + 1), the rank the quantile is interpolated from;
//   best, worst: the most and the least of them;
//   annualizedBy: the periods a year it annualized by (perYear's, the most of them: a tile of no frequency's inferred);
//   least: the level's least (at or below zero, the reason every other is none).
// Every one but the last two none of a line whose level is at or below zero anywhere (a yield's ratios mean nothing). A row a line: its
// windows the mean, the running peak and the returns' ranks alone, every figure one aggregate of the line -- a window a
// figure nested as many deep, and DuckDB's filter pushdown doubled its time at each (2026-10-28: 1.3 s of 130 ms).
function datacube::ts::performanceStatistics<T,L,D,N,V,R,G,Y,O1,O2,O3,O4,O5,O6,O7,O8,O9,O10,O11,O12,O13,O14>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], time: ColSpec<N⊆T>[1], level: ColSpec<V⊆T>[1], returns: ColSpec<R⊆T>[1], logReturns: ColSpec<G⊆T>[1], perYear: ColSpec<Y⊆T>[1], q: Float[1], vol: ColSpec<O1>[1], sharpe: ColSpec<O2>[1], drawdown: ColSpec<O3>[1], growth: ColSpec<O4>[1], skew: ColSpec<O5>[1], kurtosis: ColSpec<O6>[1], sortino: ColSpec<O7>[1], hit: ColSpec<O8>[1], valueAtRisk: ColSpec<O9>[1], shortfall: ColSpec<O10>[1], best: ColSpec<O11>[1], worst: ColSpec<O12>[1], annualizedBy: ColSpec<O13>[1], least: ColSpec<O14>[1]): Relation<L+O1+O2+O3+O4+O5+O6+O7+O8+O9+O10+O11+O12+O13+O14>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), tm:r|$r->read($time), lv:r|$r->read($level), x:r|$r->read($returns), g:r|$r->read($logReturns), py:r|$r->read($perYear), hx:r|$r->read($returns)->isNotEmpty()])
    ->extend(over(~l), ~[mx:{p,f,r|$r.x}:y|$y->average(), nx:{p,f,r|$r.x}:y|$y->count()])
    ->extend(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~pk:{p,f,r|$r.lv}:y|$y->max())
    ->extend(over(~[l, hx], [~x->ascending()]), ~rk:{p,f,r|$p->rank($f, $r)})
    ->extend(~[dd:x|if($x.pk->toOne() == 0, |[], |$x.lv->toOne() / $x.pk->toOne() - 1),
      dv:x|if($x.x->isEmpty(), |[], |$x.x->toOne() - $x.mx->toOne()),
      dn:x|if($x.x->isEmpty(), |[], |if($x.x->toOne() < 0, |$x.x->toOne() * $x.x->toOne(), |0.0)),
      up:x|if($x.x->isEmpty(), |[], |if($x.x->toOne() > 0, |1.0, |0.0)),
      tl:x|if($x.x->isEmpty() || ($x.rk > floor((($x.nx->toOne() - 1) * $q) + 1)), |[], |$x.x),
      dl:x|if($x.lv->isNotEmpty(), |$x.d, |[]), tl0:x|if($x.lv->isNotEmpty(), |$x.tm, |[])])
    ->extend(~d2:x|if($x.dv->isEmpty(), |[], |$x.dv->toOne() * $x.dv->toOne()))
    ->extend(~[d3:x|if($x.dv->isEmpty(), |[], |$x.d2->toOne() * $x.dv->toOne()), d4:x|if($x.dv->isEmpty(), |[], |$x.d2->toOne() * $x.d2->toOne())])
    ->groupBy(~[l], ~[ng:x|$x.g:y|$y->count(), sg:x|$x.g:y|$y->stdDevSample(), n:x|$x.x:y|$y->count(), m:x|$x.x:y|$y->average(),
      s:x|$x.x:y|$y->stdDevSample(), yr:x|$x.py:y|$y->max(), lo:x|$x.lv:y|$y->min(), mdd:x|$x.dd:y|$y->min(),
      d0:x|$x.dl:y|$y->min(), d1:x|$x.dl:y|$y->max(), v0:x|rowMapper($x.lv, $x.tl0):y|$y->minBy(), v1:x|rowMapper($x.lv, $x.tl0):y|$y->maxBy(),
      m2:x|$x.d2:y|$y->average(), m3:x|$x.d3:y|$y->average(), m4:x|$x.d4:y|$y->average(), dn2:x|$x.dn:y|$y->average(),
      hr:x|$x.up:y|$y->average(), vr:x|$x.x:y|$y->percentile($q, true, true), es:x|$x.tl:y|$y->average(), bx:x|$x.x:y|$y->max(), wx:x|$x.x:y|$y->min()])
    ->extend(~bad:x|$x.lo->isEmpty() || ($x.lo->toOne() <= 0))
    ->extend(~[o1:x|if($x.bad || ($x.ng->toOne() < 2), |[], |($x.sg->toOne() * sqrt($x.yr->toOne())) * 100),
      o2:x|if($x.bad || ($x.n->toOne() < 2) || ($x.s->toOne() == 0), |[], |($x.m->toOne() / $x.s->toOne()) * sqrt($x.yr->toOne())),
      o3:x|if($x.bad, |[], |$x.mdd),
      o4:x|if($x.bad || $x.d0->isEmpty() || ($x.d1->toOne() < $x.d0->toOne()->adjust(1, meta::pure::functions::date::DurationUnit.YEARS)) || ($x.v0->toOne() == 0), |[],
        |pow($x.v1->toOne() / $x.v0->toOne(), 365.25 / dateDiff($x.d0->toOne(), $x.d1->toOne(), meta::pure::functions::date::DurationUnit.DAYS)) - 1),
      o5:x|if($x.bad || ($x.n->toOne() <= 2) || ($x.m2->toOne() == 0), |[], |(($x.m3->toOne() / pow($x.m2->toOne(), 1.5)) * sqrt($x.n->toOne() * ($x.n->toOne() - 1))) / ($x.n->toOne() - 2)),
      o6:x|if($x.bad || ($x.n->toOne() <= 3) || ($x.m2->toOne() == 0), |[],
        |(((($x.n->toOne() + 1) * ($x.n->toOne() - 1)) * $x.m4->toOne()) / ($x.m2->toOne() * $x.m2->toOne()) - (3 * (($x.n->toOne() - 1) * ($x.n->toOne() - 1)))) / (($x.n->toOne() - 2) * ($x.n->toOne() - 3))),
      o7:x|if($x.bad || $x.dn2->isEmpty() || ($x.dn2->toOne() == 0), |[], |($x.m->toOne() / sqrt($x.dn2->toOne())) * sqrt($x.yr->toOne())),
      o8:x|if($x.bad, |[], |$x.hr), o9:x|if($x.bad, |[], |$x.vr), o10:x|if($x.bad, |[], |$x.es), o11:x|if($x.bad, |[], |$x.bx), o12:x|if($x.bad, |[], |$x.wx)])
    ->select(~[l, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10, o11, o12, yr, lo])
    ->rename(~l, $line)->rename(~o1, $vol)->rename(~o2, $sharpe)->rename(~o3, $drawdown)->rename(~o4, $growth)->rename(~o5, $skew)->rename(~o6, $kurtosis)
    ->rename(~o7, $sortino)->rename(~o8, $hit)->rename(~o9, $valueAtRisk)->rename(~o10, $shortfall)->rename(~o11, $best)->rename(~o12, $worst)
    ->rename(~yr, $annualizedBy)->rename(~lo, $least)
}

// A Calmar ratio of each row: its growth over its drawdown's depth -- of a growth and a drawdown below zero.
function datacube::ts::calmarRatio<T,G,D,O>(t: Relation<T>[1], growth: ColSpec<G⊆T>[1], drawdown: ColSpec<D⊆T>[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($r->read($growth)->isEmpty() || $r->read($drawdown)->isEmpty() || ($r->read($drawdown)->toOne() >= 0), |[], |$r->read($growth)->toOne() / abs($r->read($drawdown)->toOne())))->rename(~o, $out)
}

// A line's drawdowns, deepest first (gs-quant's max_drawdown, as a table of them): each run from a peak -- a date at or
// above every one before it -- to the next: its peak's date, its trough's (its least level's), the date it recovered (the
// line's next peak; none, not yet), its depth (trough over peak, less 1), its decline's days and its recovery's (none,
// not yet) and its rank by depth -- of the runs that fall, the top of them a line; none of a line whose least level is
// at or below zero (a yield's ratios mean nothing). A row a drawdown.
function datacube::ts::drawdownsOfLine<T,L,D,V,O1,O2,O3,O4,O5,O6,O7>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], value: ColSpec<V⊆T>[1], top: Integer[1], peak: ColSpec<O1>[1], trough: ColSpec<O2>[1], recovered: ColSpec<O3>[1], depth: ColSpec<O4>[1], decline: ColSpec<O5>[1], recovery: ColSpec<O6>[1], rank: ColSpec<O7>[1]): Relation<L+O1+O2+O3+O4+O5+O6+O7>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), v:r|$r->read($value)])
    ->extend(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~pk:{p,f,r|$r.v}:y|$y->max())
    ->extend(over(~l), ~lw:{p,f,r|$r.v}:y|$y->min())
    ->extend(~tp:x|if($x.v->toOne() >= $x.pk->toOne(), |1, |0))
    ->extend(~pa:x|if($x.tp == 1, |$x.d, |[]))
    ->extend(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~rn:{p,f,r|$r.tp}:y|$y->plus())
    ->extend(over(~l, [~d->ascending()], rows(1, unbounded())), ~nx:{p,f,r|$r.pa}:y|$y->min())
    ->groupBy(~[l, rn], ~[pd:x|$x.d:y|$y->min(), pv:x|$x.v:y|$y->max(), lo:x|$x.v:y|$y->min(), td:x|rowMapper($x.d, $x.v):y|$y->minBy(),
      rc:x|$x.nx:y|$y->min(), ll:x|$x.lw:y|$y->min()])
    ->extend(~dp:x|if($x.ll->toOne() <= 0, |[], |($x.lo->toOne() / $x.pv->toOne()) - 1))
    ->filter(x|$x.dp->toOne() < 0)
    ->extend(over(~l, [~dp->ascending()]), ~rk:{p,f,r|$p->rowNumber($r)})
    ->filter(x|$x.rk <= $top)
    ->extend(~[dc:x|dateDiff($x.pd->toOne(), $x.td->toOne(), meta::pure::functions::date::DurationUnit.DAYS),
      rv:x|if($x.rc->isEmpty(), |[], |dateDiff($x.td->toOne(), $x.rc->toOne(), meta::pure::functions::date::DurationUnit.DAYS))])
    ->select(~[l, pd, td, rc, dp, dc, rv, rk])
    ->rename(~l, $line)->rename(~pd, $peak)->rename(~td, $trough)->rename(~rc, $recovered)->rename(~dp, $depth)->rename(~dc, $decline)->rename(~rv, $recovery)->rename(~rk, $rank)
}

// A condition's runs (gaps and islands): each change of it -- a 1 where it holds, a 0 where not -- begins a run; each
// date where it holds carries its run's first date and where its shading ends: the line's next date after the run's
// last, from which it no longer holds (its own last at the line's end), a run of one date a period wide; none where it
// does not hold.
function datacube::ts::runs<T,L,D,F,O1,O2>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], flag: ColSpec<F⊆T>[1], from: ColSpec<O1>[1], to: ColSpec<O2>[1]): Relation<T+O1+O2>[1]
{
  $t->extend(~[l:r|$r->read($line), d:r|$r->read($date), fl:r|$r->read($flag)])
    ->extend(over(~l, [~d->ascending()]), ~[pf:{p,f,r|$p->lag($r).fl}, nd:{p,f,r|$p->lead($r).d}])
    ->extend(~[ch:x|if($x.pf->isEmpty() || !($x.fl->toOne() == $x.pf->toOne()), |1, |0), en:x|if($x.nd->isEmpty(), |$x.d, |$x.nd)])
    ->extend(over(~l, [~d->ascending()], rows(unbounded(), 0)), ~rn:{p,f,r|$r.ch}:y|$y->plus())
    ->extend(over(~[l, rn]), ~[f0:{p,f,r|$r.d}:y|$y->min(), l0:{p,f,r|$r.en}:y|$y->max()])
    ->extend(~[a:x|if($x.fl == 1, |$x.f0, |[]), b:x|if($x.fl == 1, |$x.l0, |[])])
    ->without(~[l, d, fl, pf, nd, ch, en, rn, f0, l0])->rename(~a, $from)->rename(~b, $to)
}

// A level's calendar of returns (the monthly returns table every desk keeps): each month of each line its return --
// its last level over the month before's, less 1: none over a missing month (a month a number, its year's twelve after
// the year before's, so the month before is its number less one, never the last month present), or from a level at or
// below zero -- and its year's, its last month's level over the year before's; and the line's least month-end level (a
// return from one at or below zero is none, said so). Each month's last level the latest by time (a number: a maxBy's
// key). A row a line and month.
function datacube::ts::calendarReturns<T,L,D,N,V,O1,O2,O3,O4,O5>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], time: ColSpec<N⊆T>[1], value: ColSpec<V⊆T>[1], year: ColSpec<O1>[1], month: ColSpec<O2>[1], monthly: ColSpec<O3>[1], yearly: ColSpec<O4>[1], low: ColSpec<O5>[1]): Relation<L+O1+O2+O3+O4+O5>[1]
{
  let months = $t->filter(x|$x->read($value)->isNotEmpty())
    ->extend(~[l:r|$r->read($line), yr:r|$r->read($date)->toOne()->year(), mo:r|$r->read($date)->toOne()->monthNumber(), v:r|$r->read($value), tm:r|$r->read($time)])
    ->groupBy(~[l, yr, mo], ~lv:x|rowMapper($x.v, $x.tm):y|$y->maxBy())
    ->extend(~i:x|($x.yr * 12) + $x.mo);
  let years = $months->groupBy(~[l, yr], ~yl:x|rowMapper($x.lv, $x.i):y|$y->maxBy());
  let yearRows = $years->join($years->rename(~l, ~ql)->rename(~yr, ~qy)->rename(~yl, ~qv), meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.ql) && ($a.yr == ($b.qy + 1))})
    ->extend(~yrt:x|if($x.qv->isEmpty() || ($x.qv->toOne() <= 0), |[], |($x.yl->toOne() / $x.qv->toOne()) - 1))
    ->select(~[l, yr, yrt])->rename(~l, ~kl)->rename(~yr, ~ky);
  $months->join($months->select(~[l, i, lv])->rename(~l, ~pl)->rename(~i, ~pi)->rename(~lv, ~pv), meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.pl) && ($a.i == ($b.pi + 1))})
    ->extend(~rt:x|if($x.pv->isEmpty() || ($x.pv->toOne() <= 0), |[], |($x.lv->toOne() / $x.pv->toOne()) - 1))
    ->join($yearRows, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.l == $b.kl) && ($a.yr == $b.ky)})
    ->extend(over(~l), ~lo:{p,f,r|$r.lv}:y|$y->min())
    ->select(~[l, yr, mo, rt, yrt, lo])
    ->rename(~l, $line)->rename(~yr, $year)->rename(~mo, $month)->rename(~rt, $monthly)->rename(~yrt, $yearly)->rename(~lo, $low);
}

// A histogram of each line's values: bins of one width from its least to its most (the most in the last), each counted
// -- a bin no value fell in counted 0 -- and beside each the count a normal of the values' mean and sample deviation
// would give it (n times the width times its density at the bin's middle; none where they do not vary); with the
// values' q-quantile (continuous, pandas' quantile), mean, deviation and count. A row a line and bin, its edges and
// middle; values all one, one bin of no width. The bins numbered by the line's first ten rows each with each: a hundred
// bins at most.
function datacube::ts::histogramOfLine<T,L,V,O1,O2,O3,O4,O5,O6,O7,O8,O9>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], value: ColSpec<V⊆T>[1], bins: Integer[1], q: Float[1], low: ColSpec<O1>[1], middle: ColSpec<O2>[1], high: ColSpec<O3>[1], count: ColSpec<O4>[1], normal: ColSpec<O5>[1], quantile: ColSpec<O6>[1], mean: ColSpec<O7>[1], sd: ColSpec<O8>[1], n: ColSpec<O9>[1]): Relation<L+O1+O2+O3+O4+O5+O6+O7+O8+O9>[1]
{
  let base = $t->extend(~[l:r|$r->read($line), v:r|$r->read($value)])
    ->extend(over(~l), ~[lo:{p,f,r|$r.v}:y|$y->min(), hi:{p,f,r|$r.v}:y|$y->max(), m:{p,f,r|$r.v}:y|$y->average(), s:{p,f,r|$r.v}:y|$y->stdDevSample(), k:{p,f,r|$r.v}:y|$y->count()])
    ->extend(~w:x|($x.hi->toOne() - $x.lo->toOne()) / $bins->toFloat())
    ->extend(~raw:x|if($x.w->toOne() == 0, |0, |floor(($x.v->toOne() - $x.lo->toOne()) / $x.w->toOne())))
    ->extend(~bn:x|if($x.raw->toOne() >= $bins, |$bins - 1, |$x.raw))
    ->extend(over(~l, [~v->ascending()]), ~rn:{p,f,r|$p->rowNumber($r)});
  let ten = $base->filter(x|$x.rn <= 10)->select(~[l, rn]);
  let numbers = $ten->rename(~l, ~tk)->rename(~rn, ~tr)
    ->join($ten->rename(~l, ~uk)->rename(~rn, ~ur), meta::pure::functions::relation::JoinKind.INNER, {a,b|$a.tk == $b.uk})
    ->extend(over(~tk, [~tr->ascending(), ~ur->ascending()]), ~bi:{p,f,r|$p->rowNumber($r) - 1})
    ->filter(x|$x.bi < $bins)
    ->select(~[tk, bi]);
  let figures = $base->groupBy(~[l], ~[cl:x|$x.lo:y|$y->max(), cw:x|$x.w:y|$y->max(), cm:x|$x.m:y|$y->max(), cs:x|$x.s:y|$y->max(), cn:x|$x.k:y|$y->max(),
    cq:x|$x.v:y|$y->percentile($q, true, true)]);
  let counted = $base->groupBy(~[l, bn], ~cc:x|$x.v:y|$y->count())->rename(~l, ~ck)->rename(~bn, ~cb);
  $numbers->join($figures, meta::pure::functions::relation::JoinKind.INNER, {a,b|$a.tk == $b.l})
    ->join($counted, meta::pure::functions::relation::JoinKind.LEFT, {a,b|($a.tk == $b.ck) && ($a.bi == $b.cb)})
    ->filter(x|($x.bi == 0) || ($x.cw->toOne() > 0))
    ->extend(~lw:x|$x.cl->toOne() + ($x.bi * $x.cw->toOne()))
    ->extend(~[md:x|$x.lw + ($x.cw->toOne() / 2), hg:x|$x.lw + $x.cw->toOne(), ct:x|if($x.cc->isEmpty(), |0, |$x.cc)])
    ->extend(~nm:x|if($x.cs->isEmpty() || ($x.cs->toOne() == 0), |[],
      |(($x.cn->toOne() * $x.cw->toOne()) * exp(-0.5 * ((($x.md - $x.cm->toOne()) / $x.cs->toOne()) * (($x.md - $x.cm->toOne()) / $x.cs->toOne())))) / ($x.cs->toOne() * sqrt(6.283185307179586))))
    ->select(~[l, lw, md, hg, ct, nm, cq, cm, cs, cn])
    ->rename(~l, $line)->rename(~lw, $low)->rename(~md, $middle)->rename(~hg, $high)->rename(~ct, $count)->rename(~nm, $normal)
    ->rename(~cq, $quantile)->rename(~cm, $mean)->rename(~cs, $sd)->rename(~cn, $n);
}

// Of each place in a year of each line (its month, quarter or week: the place column's), the least, most and mean of
// its values there over the years before the line's latest -- the range a seasonality draws its latest year against --
// beside each row; none of a place no earlier year has.
function datacube::ts::seasonalRange<T,L,Y,P,V,O1,O2,O3>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], year: ColSpec<Y⊆T>[1], place: ColSpec<P⊆T>[1], value: ColSpec<V⊆T>[1], low: ColSpec<O1>[1], high: ColSpec<O2>[1], mean: ColSpec<O3>[1]): Relation<T+O1+O2+O3>[1]
{
  $t->extend(~[l:r|$r->read($line), yr:r|$r->read($year), pl:r|$r->read($place)])
    ->extend(over(~l), ~ly:{p,f,r|$r.yr}:y|$y->max())
    ->extend(~bv:x|if($x.yr->toOne() < $x.ly->toOne(), |$x->read($value), |[]))
    ->extend(over(~[l, pl]), ~[a:{p,f,r|$r.bv}:y|$y->min(), b:{p,f,r|$r.bv}:y|$y->max(), c:{p,f,r|$r.bv}:y|$y->average()])
    ->without(~[l, yr, pl, ly, bv])->rename(~a, $low)->rename(~b, $high)->rename(~c, $mean)
}

// A SEASONALITY (the commodities desk's chart, Bloomberg's SEAG), a row a date of a line's last years: each date's year
// and its place in the year by grain (Grain.month, quarter; week, the week of the year from its first day; day, those
// weeks, each year's week its last value by time), its value as compare says (Compare.none; rebase, a multiple of its
// year's first value times 100, gs-quant's index; percent, over its year's first less 1, percent_change's; none from a
// first of 0), and at its place the least, the most and the mean of the years before the latest (seasonalRange's), the
// most less the least, and the latest year: of the dates with a value, the latest year's and the years before it.
function datacube::ts::seasonality<T,L,D,N,V,O1,O2,O3,O4,O5,O6,O7,O8>(t: Relation<T>[1], line: ColSpec<L⊆T>[1], date: ColSpec<D⊆T>[1], time: ColSpec<N⊆T>[1], value: ColSpec<V⊆T>[1], grain: datacube::ts::Grain[1], compare: datacube::ts::Compare[1], years: Integer[1], year: ColSpec<O1>[1], place: ColSpec<O2>[1], compared: ColSpec<O3>[1], low: ColSpec<O4>[1], high: ColSpec<O5>[1], mean: ColSpec<O6>[1], span: ColSpec<O7>[1], latest: ColSpec<O8>[1]): Relation<T+O1+O2+O3+O4+O5+O6+O7+O8>[1]
{
  $t->filter(r|$r->read($value)->isNotEmpty())
    ->extend(~[l:r|$r->read($line), d:r|$r->read($date), tm:r|$r->read($time), v:r|$r->read($value), yr:r|$r->read($date)->toOne()->year(),
      pl:r|if($grain == datacube::ts::Grain.month, |$r->read($date)->toOne()->monthNumber(), |if($grain == datacube::ts::Grain.quarter, |$r->read($date)->toOne()->quarterNumber(),
        |floor(($r->read($date)->toOne()->dayOfYear() - 1) / 7) + 1))])
    ->extend(over(~[l, yr]), ~d0:{p,f,r|$r.d}:y|$y->min())
    ->extend(over(~[l, yr]), ~v0:{p,f,r|if($r.d == $r.d0, |$r.v, |[])}:y|$y->max())
    ->extend(~cv:x|if($compare == datacube::ts::Compare.rebase, |if($x.v0->toOne() == 0, |[], |($x.v->toOne() / $x.v0->toOne()) * 100.0),
      |if($compare == datacube::ts::Compare.percent, |if($x.v0->toOne() == 0, |[], |($x.v->toOne() / $x.v0->toOne()) - 1),
        |$x.v)))
    ->extend(over(~[l, yr, pl]), ~wl:{p,f,r|$r.tm}:y|$y->max())
    ->filter(x|($grain != datacube::ts::Grain.day) || ($x.tm->toOne() == $x.wl->toOne()))
    ->extend(over(~l), ~ly:{p,f,r|$r.yr}:y|$y->max())
    ->filter(x|$x.yr->toOne() >= ($x.ly->toOne() - $years))
    ->datacube::ts::seasonalRange(~l, ~yr, ~pl, ~cv, ~lo, ~hi, ~mn)
    ->extend(~sp:x|if($x.hi->isEmpty() || $x.lo->isEmpty(), |[], |$x.hi->toOne() - $x.lo->toOne()))
    ->without(~[l, d, tm, v, d0, v0, wl])->rename(~yr, $year)->rename(~pl, $place)->rename(~cv, $compared)->rename(~lo, $low)->rename(~hi, $high)
    ->rename(~mn, $mean)->rename(~sp, $span)->rename(~ly, $latest)
}

// Each date's period's number by grain, consecutive (the place in a cycle a decomposition reads, by its number modulo
// the cycle): its months' (the year twelve times and its month less 1), its quarters' (the year four times and its
// quarter less 1), its weeks' (sevens of days from Monday 1970-01-05) or its days' (from 1970-01-01).
function datacube::ts::periodIndex<T,D,O>(t: Relation<T>[1], date: ColSpec<D⊆T>[1], grain: datacube::ts::Grain[1], out: ColSpec<O>[1]): Relation<T+O>[1]
{
  $t->extend(~o:r|if($grain == datacube::ts::Grain.month, |($r->read($date)->toOne()->year() * 12) + ($r->read($date)->toOne()->monthNumber() - 1),
    |if($grain == datacube::ts::Grain.quarter, |($r->read($date)->toOne()->year() * 4) + ($r->read($date)->toOne()->quarterNumber() - 1),
      |if($grain == datacube::ts::Grain.week, |floor(dateDiff(%1970-01-05, $r->read($date)->toOne(), meta::pure::functions::date::DurationUnit.DAYS) / 7),
        |dateDiff(%1970-01-01, $r->read($date)->toOne(), meta::pure::functions::date::DurationUnit.DAYS)))))->rename(~o, $out)
}
`;
