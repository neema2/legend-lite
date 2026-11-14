// THE DOOR'S TOPICS, READ BY THE PINNED DEEPHAVEN (docs/DESK_DATA_ROADMAP_2026_10_29.md B1; a test's probe, never the
// desk's program -- B2 generates that from the model): each topic the test names (DOOR_TOPICS: a line a topic, its name,
// its id columns and its columns with their Deephaven types, tab-separated) consumed from its beginning with
// Deephaven's own Kafka reader, each record's JSON read by the topic's columns (a day as text: the reader takes no
// LocalDate, B1's measurement). Once the test says the world has stopped (DOOR_DONE: a line a topic, the records and
// keys it wrote), each table's rows and distinct ids are written to DOOR_OUT when they reach what was written (or at 90 s),
// then "done".
import io.deephaven.kafka.KafkaTools
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.context.ExecutionContext

def env = System.getenv()
OUT = new File(env['DOOR_OUT'])
OUT.text = ''
def props = new Properties()
props.put('bootstrap.servers', '127.0.0.1:' + env['DOOR_PORT'])
def column = { String name, String type ->
    switch (type) {
        case 'java.lang.String': return ColumnDefinition.ofString(name)
        case 'double': return ColumnDefinition.ofDouble(name)
        case 'int': return ColumnDefinition.ofInt(name)
        case 'long': return ColumnDefinition.ofLong(name)
        case 'java.time.Instant': return ColumnDefinition.ofTime(name)
        case 'java.lang.Boolean': return ColumnDefinition.ofBoolean(name)
        case 'java.time.LocalDate': return ColumnDefinition.ofString(name)
        default: throw new IllegalArgumentException('the probe reads no column of type ' + type + ' (' + name + ')')
    }
}
def read = [:]
new File(env['DOOR_TOPICS']).eachLine { line ->
    def parts = line.split('\t')
    def defs = parts[2].split(',').collect { c -> def nt = c.split(':'); column(nt[0], nt[1]) } as ColumnDefinition[]
    def t = KafkaTools.consumeToTable(props, parts[0], KafkaTools.ALL_PARTITIONS,
            KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING, KafkaTools.Consume.IGNORE, KafkaTools.Consume.jsonSpec(defs),
            KafkaTools.TableType.append())
    read[parts[0]] = [table: t, ids: parts[1].split(',') as String[]]
}
OUT << ('consuming ' + read.size() + ' topics\n')
def ctx = ExecutionContext.getContext()
Thread.start {
    def s = ctx.open()
    try {
        def done = new File(env['DOOR_DONE'])
        while (!done.exists()) Thread.sleep(200)
        Thread.sleep(500)
        def written = [:]
        done.eachLine { line ->
            def p = line.split('\t', -1)
            written[p[0]] = [records: p[1] as long, keys: p[2] ? p[2] as long : null]
        }
        def counts = [:]
        def started = System.currentTimeMillis()
        while (true) {
            read.each { topic, r ->
                def lock = r.table.getUpdateGraph().sharedLock()
                lock.doLocked({
                    counts[topic] = [rows: r.table.size(), distinct: r.table.selectDistinct(r.ids).size()]
                } as io.deephaven.util.function.ThrowingRunnable)
            }
            def reached = written.every { topic, w ->
                def c = counts[topic]
                c != null && (w.keys != null ? c.distinct >= w.keys : c.rows >= w.records)
            }
            if (reached || System.currentTimeMillis() - started > 90000) break
            Thread.sleep(1000)
        }
        counts.each { topic, c -> OUT << (topic + '\t' + c.rows + '\t' + c.distinct + '\n') }
    } catch (Throwable e) {
        OUT << ('failed: ' + e + '\n')
        e.printStackTrace()
    } finally {
        OUT << 'done\n'
        s.close()
    }
}
