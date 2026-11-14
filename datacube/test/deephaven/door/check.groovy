// THE STARTUP PROGRAM'S TABLES, CHECKED ON THE PINNED DEEPHAVEN (docs/DESK_DATA_ROADMAP_2026_10_29.md B1 and B2; a test's
// check, run after the program the planner generated, in the same application): each table the test names
// (DOOR_TABLES: a line a table, its id columns and its columns with the Deephaven types the store declares, tab-separated)
// read by its name from the program's scope -- its columns' types the declared ones (DH16's check) -- and, once the test
// says the world has stopped (DOOR_DONE: a line a table, the records and keys written), its rows and distinct ids written
// to DOOR_OUT when they reach what was written (or at 90 s), then "done".
import io.deephaven.engine.context.ExecutionContext

def env = System.getenv()
OUT = new File(env['DOOR_OUT'])
OUT.text = ''
def tables = [:]
new File(env['DOOR_TABLES']).eachLine { line ->
    def parts = line.split('\t')
    def t = binding.getVariable(parts[0])
    def wrong = []
    parts[2].split(',').each { c ->
        def nt = c.split(':')
        def col = t.getDefinition().getColumn(nt[0])
        if (col == null) wrong << (nt[0] + ' missing')
        else if (col.getDataType().getName() != nt[1]) wrong << (nt[0] + ' held as ' + col.getDataType().getName() + ', declared ' + nt[1])
    }
    if (t.getDefinition().getColumns().size() != parts[2].split(',').size()) wrong << ('columns ' + t.getDefinition().getColumnNames())
    tables[parts[0]] = [table: t, ids: parts[1].split(',') as String[], wrong: wrong]
}
OUT << ('checking ' + tables.size() + ' tables\n')
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
            tables.each { name, r ->
                def lock = r.table.getUpdateGraph().sharedLock()
                lock.doLocked({
                    counts[name] = [rows: r.table.size(), distinct: r.table.selectDistinct(r.ids).size()]
                } as io.deephaven.util.function.ThrowingRunnable)
            }
            def reached = written.every { name, w ->
                def c = counts[name]
                c != null && (w.keys != null ? c.distinct >= w.keys : c.rows >= w.records)
            }
            if (reached || System.currentTimeMillis() - started > 90000) break
            Thread.sleep(1000)
        }
        counts.each { name, c ->
            OUT << (name + '\t' + c.rows + '\t' + c.distinct + '\t' + (tables[name].wrong.isEmpty() ? 'typed' : tables[name].wrong.join('; ')) + '\n')
        }
    } catch (Throwable e) {
        OUT << ('failed: ' + e + '\n')
        e.printStackTrace()
    } finally {
        OUT << 'done\n'
        s.close()
    }
}
