# Experiment 6, every corpus pass: rerun each pass's exact Bazel command by hand with a swapped prelude first on the
# classpath, outputs redirected; then compare every output with the Bazel baseline.
import json, sys, os, subprocess, shlex, time, shutil
B, H, P, override = sys.argv[1:5]
ER = subprocess.run(['bazel', 'info', 'execution_root'], cwd=B, capture_output=True, text=True).stdout.strip()
BB = subprocess.run(['bazel', 'info', 'bazel-bin'], cwd=B, capture_output=True, text=True).stdout.strip()
lanes = ['duckdb', 'h2', 'warehouse']
results = []
for lane in lanes:
    for kind in ('host', 'database'):
        tgt = f'judge_{kind}_{lane}'
        q = subprocess.run(['bazel', 'aquery', f'mnemonic("Corpus.*", //spec:{tgt})', '--output=jsonproto'], cwd=B, capture_output=True, text=True).stdout
        args = json.loads(q)['actions'][0]['arguments']
        out = f'{H}/e6_lanes/{os.path.basename(override)}/{tgt}'
        shutil.rmtree(out, ignore_errors=True); os.makedirs(f'{out}/tmp')
        new = []
        for i, x in enumerate(args):
            for k in ('judge_host_', 'judge_database_'):
                pass
            if x.startswith('-Djava.io.tmpdir='): x = f'-Djava.io.tmpdir={out}/tmp'
            elif x.startswith('-Xmx'): x = '-Xmx4g'
            elif i > 0 and args[i - 1] == '-cp': x = f'{override}:' + x
            x = x.replace(f'bazel-out/darwin_arm64-fastbuild/bin/spec/{tgt}', out)
            if kind == 'database':
                x = x.replace(f'bazel-out/darwin_arm64-fastbuild/bin/spec/judge_host_{lane}', f'{H}/e6_lanes/{os.path.basename(override)}/judge_host_{lane}')
            new.append(x)
        t0 = time.time()
        r = subprocess.run(new, cwd=ER, capture_output=True, text=True)
        secs = time.time() - t0
        base = f'{BB}/spec/{tgt}'
        diffs = []
        for f in sorted(os.listdir(base)):
            if f.endswith('.log') or f.endswith('.params') or not os.path.isfile(f'{base}/{f}'): continue
            a = sorted(l for l in open(f'{base}/{f}', errors='replace') if not l.startswith('#'))
            b = sorted(l for l in open(f'{out}/{f}', errors='replace') if not l.startswith('#')) if os.path.exists(f'{out}/{f}') else None
            if a != b: diffs.append(f if b is not None else f + ' (missing)')
        verdict = open(f'{out}/verdict.txt').read().strip() if os.path.exists(f'{out}/verdict.txt') else '?'
        results.append((tgt, round(secs), verdict, diffs))
        print(f"{tgt:28} {round(secs):4}s verdict={verdict} exit={r.returncode} differing outputs: {diffs or 'none'}", flush=True)
