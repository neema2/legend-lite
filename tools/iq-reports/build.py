"""The realistic report suite's cases (the IQ scale program, 2026-10-25): writes the parity root's schema.yaml and
lite/reports_<group>.yaml from gen_schema.py's output and queries_*.py, and a corpus suite (schema.yaml, queries.jsonl)
for IqCorpusTest's coverage run over the same schema. Run from this directory:
  python3 gen_schema.py schema.yaml && python3 build.py ../../core/src/test/resources/iq-reports <suite dir>"""
import json, os, re, shutil, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import queries_a, queries_b, queries_c, queries_d, queries_e, queries_f, queries_g, queries_h, queries_i, queries_j, queries_k, queries_l, queries_m, queries_n, queries_o, queries_p, queries_q, queries_r, queries_s, queries_t, queries_u, queries_v, queries_w
root, suite = sys.argv[1], sys.argv[2]
os.makedirs(os.path.join(root, "lite"), exist_ok=True)
os.makedirs(suite, exist_ok=True)
shutil.copy("schema.yaml", os.path.join(root, "schema.yaml"))
shutil.copy("schema.yaml", os.path.join(suite, "schema.yaml"))
seen = set(); n = 0; groups = set()
with open(os.path.join(suite, "queries.jsonl"), "w") as qj:
    for group, cases in queries_a.CASES + queries_b.CASES + queries_c.CASES + queries_d.CASES + queries_e.CASES + queries_f.CASES + queries_g.CASES + queries_h.CASES + queries_i.CASES + queries_j.CASES + queries_k.CASES + queries_l.CASES + queries_m.CASES + queries_n.CASES + queries_o.CASES + queries_p.CASES + queries_q.CASES + queries_r.CASES + queries_s.CASES + queries_t.CASES + queries_u.CASES + queries_v.CASES + queries_w.CASES:
        # one file a group: a group named twice would overwrite the first one's cases
        assert group not in groups, group
        groups.add(group)
        with open(os.path.join(root, "lite", f"reports_{group}.yaml"), "w") as f:
            f.write(f"# The IQ scale program's realistic reports (2026-10-25): {group}, as analysts write them, over the retail\n"
                    "# schema (iq-reports/schema.yaml), judged by the live IQ (iq17-default). tools/iq-oracle's README.md has the keys.\n"
                    "tests:\n")
            for cid, sql in cases:
                one = re.sub(r"\s+", " ", sql).strip()
                assert '"' not in one and cid not in seen, cid
                seen.add(cid); n += 1
                # (in YAML's double quotes a backslash is written \\: part P's REGEXP patterns)
                quoted = one.replace("\\", "\\\\")
                f.write(f"  - id: {cid}\n    sql: \"{quoted}\"\n    profile: iq17-default\n")
                qj.write(json.dumps({"id": f"reports_{group}/{cid}", "sql": one}) + "\n")
print(n)
