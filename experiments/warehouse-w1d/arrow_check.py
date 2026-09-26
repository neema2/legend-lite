"""W1d: read every stream ArrowProbe wrote with pyarrow (a standard Arrow reader); print type and values."""
import sys, pathlib, pyarrow.ipc as ipc
for f in sorted(pathlib.Path(sys.argv[1]).glob("*.arrows")):
    try:
        t = ipc.open_stream(f.read_bytes()).read_all()
        if f.stem == "big":
            print(f"{f.stem:14} {t.num_rows} rows, schema {[str(x.type) for x in t.schema]}")
        else:
            print(f"{f.stem:14} {str(t.schema.field(0).type):40} {t.column(0).to_pylist()}")
    except Exception as e:
        print(f"{f.stem:14} UNREADABLE: {e}")
