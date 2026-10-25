// THE STRESS CORPUS'S TEST DATA ON A DEEPHAVEN SERVER (docs/DEEPHAVEN_TRADER_DEMOS_2026_10_24.md, task 15), test only:
// lite writes no rows into Deephaven yet (its input tables' writes, docs/DEEPHAVEN_PLAN_2026_10_22.md §6), so the
// corpus lane's server starts with the corpus's tables made here, from the file //core:stress_deephaven_cases writes
// (CORPUS_TABLES): each table its declared columns' Deephaven kinds -- the column types the Deephaven dialect's DDL
// gives them -- and its rows' text, read as the stress suites read the corpus's CSV. A value the kind cannot read, or a
// table that cannot be made, is said in `corpus_load`, never left out unsaid.

import groovy.json.JsonSlurper
import io.deephaven.engine.util.TableTools
import io.deephaven.util.QueryConstants

import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneOffset

/** A timestamp's text as the corpus writes it -- `2024-06-03 09:30:00[.nnn]`, an hour of one digit (`5:05:00`), or a
 *  day alone, each as SQL's TIMESTAMP reads it -- as an instant in UTC. */
Instant instantOf(String v) {
    def m = v.trim() =~ /^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2})(?:\.(\d{1,9}))?)?)?$/
    if (!m.matches()) throw new IllegalArgumentException("not a timestamp: " + v)
    int nanos = m.group(7) == null ? 0 : Integer.parseInt(m.group(7).padRight(9, '0'))
    return LocalDateTime.of(m.group(1) as int, m.group(2) as int, m.group(3) as int, (m.group(4) ?: '0') as int,
            (m.group(5) ?: '0') as int, (m.group(6) ?: '0') as int, nanos).toInstant(ZoneOffset.UTC)
}

Boolean booleanOf(String v) {
    String b = v.trim().toLowerCase()
    if (b == 'true' || b == '1') return Boolean.TRUE
    if (b == 'false' || b == '0') return Boolean.FALSE
    throw new IllegalArgumentException("not a boolean: " + v)
}

/** A column of `kind` (the dialect's DDL: int, long, double, BigDecimal, Boolean, LocalDate, Instant, LocalTime,
 *  String), its values from their text, a null where there is none. */
def columnOf(String name, String kind, List<String> text) {
    switch (kind) {
        case 'int': return TableTools.intCol(name, text.collect { it == null ? QueryConstants.NULL_INT : new BigDecimal(it.trim()).intValueExact() } as int[])
        case 'long': return TableTools.longCol(name, text.collect { it == null ? QueryConstants.NULL_LONG : new BigDecimal(it.trim()).longValueExact() } as long[])
        case 'double': return TableTools.doubleCol(name, text.collect { it == null ? QueryConstants.NULL_DOUBLE : Double.parseDouble(it.trim()) } as double[])
        case 'BigDecimal': return TableTools.col(name, text.collect { it == null ? null : new BigDecimal(it.trim()) } as BigDecimal[])
        case 'Boolean': return TableTools.col(name, text.collect { it == null ? null : booleanOf(it) } as Boolean[])
        case 'LocalDate': return TableTools.col(name, text.collect { it == null ? null : LocalDate.parse(it.trim().substring(0, 10)) } as LocalDate[])
        case 'Instant': return TableTools.col(name, text.collect { it == null ? null : instantOf(it) } as Instant[])
        case 'LocalTime': return TableTools.col(name, text.collect { it == null ? null : LocalTime.parse(it.trim()) } as LocalTime[])
        default: return TableTools.stringCol(name, text as String[])
    }
}

List<String> items = []
List<String> details = []
def tables = new JsonSlurper().parse(new File(System.getenv('CORPUS_TABLES')))
int made = 0
for (t in tables) {
    try {
        def columns = []
        t.columns.eachWithIndex { c, i ->
            List<String> text = t.rows.collect { r -> i < r.size() ? (String) r[i] : null }
            columns << columnOf((String) c.name, (String) c.kind, text)
        }
        binding.setVariable((String) t.table, TableTools.newTable(*columns))
        made++
    } catch (Throwable e) {
        items << (String) t.table
        details << String.valueOf(e)
    }
}
corpus_load = TableTools.newTable(
        TableTools.stringCol('item', (['tables'] + items) as String[]),
        TableTools.stringCol('detail', ([String.valueOf(made)] + details) as String[]))
System.out.println('the corpus\'s tables: ' + made + ' made, ' + items.size() + ' refused')
items.eachWithIndex { t, i -> System.out.println('  refused ' + t + ': ' + details[i]) }
