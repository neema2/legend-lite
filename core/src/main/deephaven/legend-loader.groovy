// THE LEGEND LOADER, version 1 (docs/DEPLOYMENT_DESIGN_2026_11_14.md §9): fixed and versioned, shipped with
// legend-lite, never generated. A Deephaven server runs it at start; it reads one deployment's plan -- data the planner
// rendered (com.legend.sql.dialect.DeephavenLoad), named by the environment's LEGEND_DEEPHAVEN_PLAN -- and makes each
// table the plan lists: read from its topic's beginning by Deephaven's own reader (KafkaTools.consumeToTable), its
// records' JSON by the table's columns (a day read as its text and made a LocalDate here: 42.7's JSON reader takes none,
// measured 2026-10-30), then kept as the plan says -- an appended table its latest n rows (a ring) or all of them; a
// milestoned table each key's latest record (a blink table, lastBy its key), the delete indicator's keys dropped, its
// milestoning columns filled: the record's partition-prefixed offset its batch, the open batch id, a hash of its own
// columns -- and published under its name, its declared columns alone, in order, the milestoning columns after. The
// planner checked every name and value this reads (the dialect's part), so the loader checks only that the plan is one.
import groovy.json.JsonSlurper
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
plan.tables.each { t ->
    def kafka = new Properties()
    kafka.put('bootstrap.servers', t.brokers)
    def columns = t.columns.collect { c ->
        def of = read[c.type]
        if (of == null) throw new IllegalStateException('the Legend loader: ' + t.name + '.' + c.name + ' is held as ' + c.type)
        of(c.name as String)
    } as ColumnDefinition[]
    def keep = t.keep
    def type = keep.kind == 'rows' ? KafkaTools.TableType.ring(keep.rows as int)
            : keep.kind == 'all' ? KafkaTools.TableType.append()
            : keep.kind == 'current' ? KafkaTools.TableType.blink()
            : null
    if (type == null) throw new IllegalStateException('the Legend loader: ' + t.name + ' keeps ' + keep.kind)
    def table = KafkaTools.consumeToTable(kafka, t.topic as String, KafkaTools.ALL_PARTITIONS,
            KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING, KafkaTools.Consume.IGNORE, KafkaTools.Consume.jsonSpec(columns),
            type)
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
        table = table.update(
                m.batchIn + ' = KafkaPartition * ' + m.partitionPrefix + 'L + KafkaOffset',
                m.batchOut + ' = ' + m.openBatch + 'L',
                m.digest + ' = Integer.toHexString(java.util.Objects.hash(' + view.join(', ') + '))')
        view += [m.digest as String, m.batchIn as String, m.batchOut as String]
    }
    binding.setVariable(t.name as String, table.view(view as String[]))
}
