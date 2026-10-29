import io.deephaven.kafka.KafkaTools
import io.deephaven.engine.table.ColumnDefinition
import io.deephaven.engine.context.ExecutionContext
import static io.deephaven.api.agg.Aggregation.*
OUT = new File(System.getenv('PROBE_OUT'))
OUT.text = ''
def started = System.currentTimeMillis()
def props = new Properties()
props.put('bootstrap.servers', '127.0.0.1:' + (System.getenv('SHIM_PORT') ?: '19092'))
def cols = [ColumnDefinition.ofString('Sym'), ColumnDefinition.ofDouble('Bid'), ColumnDefinition.ofDouble('Ask'),
            ColumnDefinition.ofLong('Seq')] as ColumnDefinition[]
tape = KafkaTools.consumeToTable(props, 'quotes', KafkaTools.ALL_PARTITIONS, KafkaTools.ALL_PARTITIONS_SEEK_TO_BEGINNING,
        KafkaTools.Consume.IGNORE, KafkaTools.Consume.jsonSpec(cols), KafkaTools.TableType.append())
lagged = tape.update('Arrived = now()', 'LagMs = (epochNanos(Arrived) - epochNanos(KafkaTimestamp)) / 1e6')
latest = tape.lastBy('Sym')
OUT << ('consuming after ' + (System.currentTimeMillis() - started) + ' ms\n')
def ctx = ExecutionContext.getContext()
def lock = tape.getUpdateGraph().sharedLock()
def read = { t, c -> t.getColumnSource(c).get(t.getRowSet().firstRowKey()) }
Thread.start {
    def s = ctx.open()
    try {
        Thread.sleep(15000)
        def s0 = 0L
        lock.doLocked({ s0 = read(tape.aggBy([AggMax('M=Seq')]), 'M') } as io.deephaven.util.function.ThrowingRunnable)
        OUT << ('caught up to Seq ' + s0 + '\n')
        Thread.sleep(20000)
        lock.doLocked({
            def all = tape.aggBy([AggCount('N'), AggMin('Lo=Seq'), AggMax('Hi=Seq')])
            def n = read(all, 'N'); def lo = read(all, 'Lo'); def hi = read(all, 'Hi')
            OUT << ('rows ' + n + ' seq ' + lo + '..' + hi + (n == hi - lo + 1 ? ' complete' : ' GAPS') + '\n')
            OUT << ('names ' + latest.size() + '\n')
            def st = lagged.where('Seq > ' + s0).aggBy([AggCount('N'), AggPct(0.5, 'P50=LagMs'), AggPct(0.99, 'P99=LagMs'),
                    AggMax('Max=LagMs')])
            OUT << ('steady ' + read(st, 'N') + ' rows: lag p50 ' + read(st, 'P50') + ' ms, p99 ' + read(st, 'P99')
                    + ' ms, max ' + read(st, 'Max') + ' ms\n')
        } as io.deephaven.util.function.ThrowingRunnable)
    } catch (Throwable e) { OUT << ('failed: ' + e + '\n'); e.printStackTrace() }
    finally { OUT << 'done\n'; s.close() }
}
