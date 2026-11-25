#!/usr/bin/env bash
# Credentials for a CLOUD DATABASE LANE in a Claude Code cloud session (AGENTS.md, "The fast setup"; the properties
# file is docs/CLOUD_BACKENDS_PCT_2026_10_11.md section 4).
#
#   tools/cloud-lane-credentials.sh databricks|snowflake
#
# Writes ~/.legendlite/cloud-pct.properties at mode 0600, the file LEGENDLITE_PCT_CLOUD_PROPERTIES names. It holds no
# secret itself: it reads the AWS key the session's environment sets (DB_AWS_ACCESS_KEY_ID, DB_AWS_SECRET_ACCESS_KEY),
# fetches the database's secret from AWS Secrets Manager, and prints key NAMES only. Never paste a secret into a
# session, a message or a file in the repository.
#
# Each database's part was written and run by its own lane on 2026-11-25 (the first GetSecretValue answered 200 for
# Databricks and for Snowflake). BigQuery is added when its lane has run its own.
set -euo pipefail
case "${1:-}" in
  databricks) ;;
  snowflake) exec "$0" --snowflake ;;
  --snowflake) ;;
  *) echo "usage: $0 databricks|snowflake" >&2; exit 2 ;;
esac
: "${DB_AWS_ACCESS_KEY_ID:?the session environment sets it}" "${DB_AWS_SECRET_ACCESS_KEY:?the session environment sets it}"
if [ "$1" = --snowflake ]; then
  # Secret: Secrets Manager us-east-1, SecretId "snowflake.INTEGRATION_USER1", keys encrypted_private_key (PKCS#8,
  # stored without its PEM armour) and private_key_encryption_password. The key-password driver property that works is
  # private_key_file_pwd.
  REGION="${REGION:-us-east-1}"; SECRET_ID="${SECRET_ID:-snowflake.INTEGRATION_USER1}"; OUT="$HOME/.legendlite"
  umask 077; mkdir -p "$OUT"; chmod 700 "$OUT"; tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  # the key pair goes to curl as a config on stdin, never the command line
  printf 'user = "%s:%s"\n' "$DB_AWS_ACCESS_KEY_ID" "$DB_AWS_SECRET_ACCESS_KEY" | \
  curl -sS --fail-with-body -K - --aws-sigv4 "aws:amz:${REGION}:secretsmanager" \
    -H 'Content-Type: application/x-amz-json-1.1' -H 'X-Amz-Target: secretsmanager.GetSecretValue' \
    --data "{\"SecretId\":\"${SECRET_ID}\"}" "https://secretsmanager.${REGION}.amazonaws.com/" -o "$tmp/resp.json"
  python3 -I - "$tmp/resp.json" "$OUT" <<'PY'
import json, sys, os, textwrap
resp, out = sys.argv[1], sys.argv[2]
secret = json.loads(json.load(open(resp))["SecretString"])
print("secret keys:", sorted(secret))
key = "".join(secret["encrypted_private_key"].split())
if "BEGIN" not in key:
    key = "-----BEGIN ENCRYPTED PRIVATE KEY-----\n" + "\n".join(textwrap.wrap(key, 64)) + "\n-----END ENCRYPTED PRIVATE KEY-----\n"
kf = os.path.join(out, "snowflake_key.p8")
with open(os.open(kf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f: f.write(key)
esc = lambda s: s.replace("\\", "\\\\").replace(":", "\\:").replace("=", "\\=")
props = {
 "snowflake.url": "jdbc:snowflake://ki79827.us-east-2.aws.snowflakecomputing.com/?warehouse=INTEGRATION_WH1&db=INTEGRATION_DB1&role=INTEGRATION_ROLE1",
 "snowflake.property.user": "INTEGRATION_USER1",
 "snowflake.property.private_key_file": kf,
 "snowflake.property.private_key_file_pwd": secret["private_key_encryption_password"],
 "snowflake.account": "ki79827", "snowflake.warehouse": "INTEGRATION_WH1", "snowflake.region": "us-east-2",
 "snowflake.database": "INTEGRATION_DB1", "snowflake.system.property.schema": "INTEGRATION_SCHEMA1"}
pf = os.path.join(out, "cloud-pct.properties")
with open(os.open(pf, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
    for k, v in props.items(): f.write(f"{k}={esc(v)}\n")
print("wrote property keys:", list(props))
PY
  echo "export LEGENDLITE_PCT_CLOUD_PROPERTIES=\$HOME/.legendlite/cloud-pct.properties"
  exit 0
fi
REGION=${REGION:-us-east-1}
HOST=dbc-f0687849-717f.cloud.databricks.com
HTTP_PATH=/sql/1.0/warehouses/c56852187940e5a3
umask 077
mkdir -p "$HOME/.legendlite"; chmod 700 "$HOME/.legendlite"
CFG=$(mktemp "$HOME/.legendlite/curlcfg.XXXXXX"); OUT=$(mktemp "$HOME/.legendlite/secret.XXXXXX")
trap 'rm -f "$CFG" "$OUT"' EXIT
# the key pair goes to curl through a config file (mode 0600), never the command line
printf 'user = "%s:%s"\n' "$DB_AWS_ACCESS_KEY_ID" "$DB_AWS_SECRET_ACCESS_KEY" > "$CFG"
code=$(curl -sS -K "$CFG" --aws-sigv4 "aws:amz:$REGION:secretsmanager" \
  -H 'Content-Type: application/x-amz-json-1.1' -H 'X-Amz-Target: secretsmanager.GetSecretValue' \
  --data '{"SecretId":"databricks"}' -o "$OUT" -w '%{http_code}' "https://secretsmanager.$REGION.amazonaws.com/")
if [ "$code" != 200 ]; then
  echo "GetSecretValue answered HTTP $code: $(python3 -I -c 'import json,sys;d=json.load(open(sys.argv[1]));print(d.get("__type"),d.get("message",d.get("Message")))' "$OUT" 2>/dev/null)" >&2
  exit 1
fi
python3 -I - "$OUT" "$HOME/.legendlite/cloud-pct.properties" "$HOST" "$HTTP_PATH" <<'PY'
import json, sys, os
src, dst, host, path = sys.argv[1:5]
secret = json.loads(json.load(open(src))["SecretString"])
print("secret keys:", sorted(secret))
token = secret["integration_test.apitoken"]
def esc(v): return v.replace("\\", "\\\\").replace(":", "\\:").replace("=", "\\=")
lines = [
  f"databricks.url=jdbc:databricks://{host}:443/default;transportMode=http;ssl=1;AuthMech=3;httpPath={path}",
  "databricks.property.UID=token",
  f"databricks.property.PWD={esc(token)}",
  f"databricks.hostname={host}",
  f"databricks.httpPath={path}",
]
fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as f: f.write("\n".join(lines) + "\n")
os.chmod(dst, 0o600)
print("wrote", dst, "keys:", [l.split("=",1)[0] for l in lines])
PY
echo "export LEGENDLITE_PCT_CLOUD_PROPERTIES=\$HOME/.legendlite/cloud-pct.properties"
