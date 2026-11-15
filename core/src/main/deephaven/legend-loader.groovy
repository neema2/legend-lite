// THE LEGEND LOADER, version 1 (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9): fixed and versioned, shipped with
// legend-lite, never generated. A Deephaven server runs it at start; it reads one deployment's plan -- data the planner
// rendered (com.legend.sql.dialect.DeephavenLoad), named by the environment's LEGEND_DEEPHAVEN_PLAN -- and makes each
// table the plan lists: read from its topic's beginning by Deephaven's own reader (KafkaTools.consumeToTable), its
// records' JSON by the table's columns, or from its delivered file, read whole (CsvTools, each column parsed as the type
// it is held as) (a day read as its text and made a LocalDate here: 42.7's JSON reader takes none,
// measured 2026-10-30), then kept as the plan says -- an appended table its latest n rows (a ring) or all of them; a
// milestoned table each key's latest record (a blink table, lastBy its key), the delete indicator's keys dropped, its
// milestoning columns filled: the record's partition-prefixed offset its batch, the open batch id, a hash of its own
// columns -- and published under its name, its declared columns alone, in order, the milestoning columns after, each
// checked against the plan before it is published (a table not as declared fails the install, naming it). The planner
// checked every name and value this reads (the dialect's part), so the loader checks only that the plan is one.
import groovy.json.JsonSlurper
import io.deephaven.csv.CsvTools
import io.deephaven.csv.parsers.Parsers
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.kafka.KafkaTools

def LOADER_VERSION = 1
def path = System.getenv('LEGEND_DEEPHAVEN_PLAN')
if (path == null || path.isEmpty()) {
    throw new IllegalStateException('the Legend loader: LEGEND_DEEPHAVEN_PLAN names no plan')
}
def plan = new JsonSlurper().parse(new File(path))
if (plan.kind != 'legend.deephaven.load' || plan.version != LOADER_VERSION) {
    throw new IllegalStateException('the Legend loader reads version ' + LOADER_VERSION + ' plans: ' + path + ' is '
            + plan.kind + ' version ' + plan.version)
}
def read = [
    'java.lang.String': { String n -> ColumnDefinition.ofString(n) },
    'java.time.LocalDate': { String n -> ColumnDefinition.ofString(n) },
    'double': { String n -> ColumnDefinition.ofDouble(n) },
    'float': { String n -> ColumnDefinition.ofFloat(n) },
    'int': { String n -> ColumnDefinition.ofInt(n) },
    'long': { String n -> ColumnDefinition.ofLong(n) },
    'short': { String n -> ColumnDefinition.ofShort(n) },
    'byte': { String n -> ColumnDefinition.ofByte(n) },
    'java.lang.Boolean': { String n -> ColumnDefinition.ofBoolean(n) },
    'java.time.Instant': { String n -> ColumnDefinition.ofTime(n) },
]
// a delivered CSV file's columns, by the Java type each is held as (a day read as text, made a date below)
def csv = [
    'java.lang.String': Parsers.STRING, 'java.time.LocalDate': Parsers.STRING, 'double': Parsers.DOUBLE,
    'float': Parsers.FLOAT_FAST, 'int': Parsers.INT, 'long': Parsers.LONG, 'short': Parsers.SHORT, 'byte': Parsers.BYTE,
    'java.lang.Boolean': Parsers.BOOLEAN, 'java.time.Instant': Parsers.DATETIME,
]
plan.tables.each { t ->
    def keep = t.keep
    def table
    if (t.source.kind == 'topic') {
        def kafka = new Properties()
        kafka.put('bootstrap.servers', t.source.brokers)
        def columns = t.columns.collect { c ->
            def of = read[c.type]
            if (of == null) throw new IllegalStateException('the Legend loader: ' + t.name + '.' + c.name + ' is held as ' + c.type)
            of(c.name as String)
        } as ColumnDefinition[]
        def type = keep.kind == 'rows' ? KafkaTools.TableType.ring(keep.rows as int)
                : keep.kind == 'all' ? KafkaTools.TableType.append()
                : keep.kind == 'current' ? KafkaTools.TableType.blink()
                : null
        if (type == null) throw new IllegalStateException('the Legend loader: ' + t.name + ' keeps ' + keep.kind)
        table = KafkaTools.consumeToTable(kafka, t.source.topic as String, KafkaTools.ALL_PARTITIONS,
                KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING, KafkaTools.Consume.IGNORE,
                KafkaTools.Consume.jsonSpec(columns), type)
    } else if (t.source.kind == 'file') {
        // a delivered file, read whole: its header the declared columns, each parsed as the type it is held as
        def specs = CsvTools.builder()
        t.columns.each { c ->
            def parser = csv[c.type]
            if (parser == null) throw new IllegalStateException('the Legend loader: ' + t.name + '.' + c.name + ' is held as ' + c.type)
            specs.putParserForName(c.name as String, parser)
        }
        table = CsvTools.readCsv(new File(new URI(t.source.url as String)).toPath(), specs.build())
        if (keep.kind == 'rows') table = table.tail(keep.rows as long)
    } else {
        throw new IllegalStateException('the Legend loader: ' + t.name + ' comes from a ' + t.source.kind)
    }
    def days = t.columns.findAll { it.type == 'java.time.LocalDate' }.collect { c ->
        c.name + ' = isNull(' + c.name + ') ? (java.time.LocalDate) null : java.time.LocalDate.parse(' + c.name + ')'
    }
    if (!days.isEmpty()) table = table.update(days as String[])
    def view = t.columns.collect { it.name as String }
    if (keep.kind == 'current') {
        table = table.lastBy(keep.key as String[])
        if (keep.delete != null) {
            def marked = keep.delete.values.collect { v -> 'String.valueOf(' + keep.delete.field + ').equals(`' + v + '`)' }
            table = table.where('!(' + marked.join(' || ') + ')')
        }
        def m = keep.milestones
        // a record's batch its partition-prefixed offset; a delivered file's, its delivery
        table = table.update(
                m.partitionPrefix != null ? m.batchIn + ' = KafkaPartition * ' + m.partitionPrefix + 'L + KafkaOffset'
                        : m.batchIn + ' = ' + m.delivery + 'L',
                m.batchOut + ' = ' + m.openBatch + 'L',
                m.digest + ' = Integer.toHexString(java.util.Objects.hash(' + view.join(', ') + '))')
        view += [m.digest as String, m.batchIn as String, m.batchOut as String]
    }
    table = table.view(view as String[])
    // THE CHECK AFTER INSTALLING (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9): the table made is its declared columns, in
    // order, each of the type the plan names (a milestoned table's three after them), or the install fails naming it
    def expected = t.columns.collect { [it.name as String, it.type as String] }
    if (keep.kind == 'current') {
        def m = keep.milestones
        expected += [[m.digest as String, 'java.lang.String'], [m.batchIn as String, 'long'], [m.batchOut as String, 'long']]
    }
    def made = table.getDefinition().getColumns().collect { [it.getName(), it.getDataType().getName()] }
    if (made != expected) {
        throw new IllegalStateException('the Legend loader made ' + t.name + ' as ' + made + ', not as its plan declares it: '
                + expected)
    }
    binding.setVariable(t.name as String, table)
}
