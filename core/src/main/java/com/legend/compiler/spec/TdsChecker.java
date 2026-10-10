package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;

import java.util.ArrayList;
import java.util.List;

/**
 * The inline TDS literal {@code #TDS col:Type, … / rows #} (engine
 * {@code TdsChecker}). The parser hands the verbatim grid as
 * {@code tds('TDS', raw)}; the call is validated against the registered
 * {@code tds(String[1], String[1])} signature (never bypassed), then the header
 * is parsed into the REAL column schema &mdash; explicit {@code col:Type}
 * annotations, or inference from the first data row for unannotated columns
 * (engine's rule) &mdash; replacing the signature's {@code Relation<Any>}
 * placeholder. An unknown annotated type fails loudly (engine dropped the
 * silent default-to-String because it masked Variant bugs).
 */
final class TdsChecker {

    private TdsChecker() {
    }

    static TypedSpec check(Typer t, AppliedFunction af, Env env) {
        if (af.parameters().size() != 2 || !(af.parameters().get(1) instanceof CString rawLit)) {
            throw new TypeInferenceException("malformed TDS literal: expected tds('TDS', body)");
        }
        // Validate against the registered native signature — never ignored.
        // (lite-INTERNAL vocabulary: resolve by exact identity, the bare
        // name is not in the user namespace)
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(com.legend.builtin.Pure.Lite.TDS),
                List.of(ExprType.one(Type.Primitive.STRING), ExprType.one(Type.Primitive.STRING)));

        String content = rawLit.value().strip();
        if (content.startsWith("#TDS")) {
            content = content.substring(4);
        }
        if (content.endsWith("#")) {
            content = content.substring(0, content.length() - 1);
        }
        String[] lines = content.strip().split("\n");
        if (lines.length == 0 || lines[0].strip().isEmpty()) {
            throw new TypeInferenceException("a TDS literal needs a header row of column names");
        }

        // Header: name[:Type] per cell.
        List<String> names = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        for (String cell : splitCells(lines[0])) {
            String c = cell.strip();
            int colon = c.indexOf(':');
            String name = colon > 0 ? c.substring(0, colon).strip() : c;
            // ENGINE presentation (pureToSQLQuery.pure:2985): a
            // separator-bearing pivot-shaped header presents with its
            // literal quotes as part of the NAME — splitCells stripped
            // the source quotes; re-present so a TDS-literal expectation
            // names pivot columns exactly like the pivot output does
            // (witness testPivot_MultipleSingle's assertTdsEquivalent)
            name = com.legend.compiler.element.type.Type.RelationType
                    .presentPivotName(name);
            if (names.contains(name)) {
                throw new SchemaInvariantException("duplicate column '" + name + "' in TDS header");
            }
            names.add(name);
            // a real-pure header may carry a multiplicity suffix
            // (grp:Integer[1] — tdsEquivalent.pure spellings); the TDS
            // cell type is the bare name
            types.add(colon > 0 ? annotatedType(c.substring(colon + 1)
                    .strip().replaceAll("\\[[^]]*\\]$", "")) : null);
        }

        // Data rows (raw cells; carried for lowering).
        List<List<String>> rows = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].strip().isEmpty()) {
                continue;
            }
            List<String> row = splitCells(lines[i]);
            while (row.size() < names.size()) {
                row.add("");
            }
            rows.add(row);
        }

        List<Type.Column> columns = new ArrayList<>(names.size());
        for (int c = 0; c < names.size(); c++) {
            Type type = types.get(c) != null ? types.get(c) : inferredType(rows, c);
            // HONEST multiplicity: a column whose DATA carries null cells
            // is [0..1] — typing it [1] hid the null-safe equality arms
            // (engine nullSafeEqualsOperation: [0..1]==[0..1] joins/
            // filters are IS NOT DISTINCT FROM; witness testJoinOnNullKey,
            // where pure joins null keys to null keys)
            boolean nullable = false;
            for (List<String> row : rows) {
                String cell = c < row.size() ? row.get(c).strip() : "";
                if (cell.isEmpty() || cell.equals("null")) {
                    nullable = true;
                    break;
                }
            }
            columns.add(new Type.Column(names.get(c), type,
                    nullable ? Multiplicity.Bounded.ZERO_ONE
                            : Multiplicity.Bounded.ONE));
        }
        // the literal IS a TDS<T> (upstream: TDS<T> extends Relation<T>, a relation
        // carrier) — newTDSRelationAccessor(tds:TDS<T>[1]) takes it as declared
        return new TypedTds(rows,
                new ExprType(new Type.GenericType(
                        com.legend.compiler.element.type.PlatformTypes.TDS_RELATION_CLASS,
                        java.util.List.of(new Type.RelationType(columns))),
                        sig.output().multiplicity()));
    }

    /**
     * Split one grid line into cells: commas inside single-quoted cells do not
     * split (engine {@code splitCsvLine}); cells are stripped and unquoted.
     */
    private static List<String> splitCells(String line) {
        List<String> cells = new ArrayList<>();
        StringBuilder cell = new StringBuilder();
        // Either quote kind protects commas — variant cells carry JSON in
        // DOUBLE quotes (1, "[1,2,3]"); the opener must close the cell.
        char quote = 0;
        for (int i = 0; i < line.length(); i++) {
            char ch = line.charAt(i);
            // A backslash inside a quoted cell ESCAPES the next character
            // (JSON payloads carry \" — it must neither close the cell nor
            // survive as a literal backslash; audit).
            if (quote != 0 && ch == '\\' && i + 1 < line.length()) {
                cell.append(line.charAt(++i));
                continue;
            }
            // CSV-style DOUBLED quotes inside a quoted cell are one literal
            // quote ({""boolean"":true} carries JSON keys; audit follow-up).
            if (quote != 0 && ch == quote && i + 1 < line.length()
                    && line.charAt(i + 1) == quote) {
                cell.append(ch);
                i++;
                continue;
            }
            if ((ch == '\'' || ch == '"') && (quote == 0 || quote == ch)) {
                quote = quote == 0 ? ch : 0;
            } else if (ch == ',' && quote == 0) {
                cells.add(unquote(cell.toString().strip()));
                cell.setLength(0);
                continue;
            }
            cell.append(ch);
        }
        cells.add(unquote(cell.toString().strip()));
        return cells;
    }

    private static String unquote(String cell) {
        boolean single = cell.length() >= 2 && cell.startsWith("'") && cell.endsWith("'");
        boolean dbl = cell.length() >= 2 && cell.startsWith("\"") && cell.endsWith("\"");
        return single || dbl ? cell.substring(1, cell.length() - 1) : cell;
    }

    /** An explicit {@code col:Type} annotation (engine {@code mapTdsColumnType}); unknown types fail loudly. */
    private static Type annotatedType(String typeName) {
        return switch (typeName) {
            case "Integer" -> Type.Primitive.INTEGER;
            case "Float", "Number" -> Type.Primitive.FLOAT;
            case "Decimal" -> new Type.PrecisionDecimal(Type.PrecisionDecimal.MAX_PRECISION, 0);
            case "Boolean" -> Type.Primitive.BOOLEAN;
            case "String" -> Type.Primitive.STRING;
            case "StrictDate" -> Type.Primitive.STRICT_DATE;
            // Pure Date is the ABSTRACT supertype — a Date column holds
            // date-times too (ledger cluster 40; the INFERRED path
            // already returns DATE for zone-suffixed cells — annotated
            // directly contradicted it, truncating 21:00:00.123 to
            // midnight through DuckDB's lenient DATE parse)
            case "Date" -> Type.Primitive.DATE;
            case "DateTime" -> Type.Primitive.DATE_TIME;
            case "Variant", "meta::pure::metamodel::variant::Variant" ->
                    new Type.ClassType(com.legend.compiler.element.type.PlatformTypes.VARIANT);
            // FQN-spelled primitives (meta::pure::precisePrimitives::Int)
            // resolve through the one primitive FQN table — same aliases the
            // type annotations (@Int) use.
            default -> Type.Primitive.findByFqn(typeName)
                    .map(t -> (Type) t)
                    .orElseThrow(() -> new TypeInferenceException(
                            "TDS column type '" + typeName + "' is not a known primitive"));
        };
    }

    /** Infer an unannotated column's type from its first non-empty cell; an empty column is String. */
    static Type inferredType(List<List<String>> rows, int col) {
        for (List<String> row : rows) {
            String v = row.get(col);
            // 'null' cells are NEUTRAL — they fit every type, so a later
            // JSON-shaped cell may still make the column Variant (the
            // "null" / "[1,2,3]" mixed-payload PCT shape).
            if (v.isEmpty() || v.equals("null") || v.equals("TDSNull")) {
                continue;
            }
            if (v.matches("[+-]?\\d+")) {
                // NUMERIC columns widen over the WHOLE column (Deephaven's
                // int-unless-a-double-appears): 21, 41.14, 71 is Float. A
                // dotless scientific cell (1e5) is a DOUBLE to Deephaven's
                // parser too — before this widened, the column stayed
                // Integer and lowering's Long.parseLong("1e5") crashed
                // (text-surgery audit §1.1 #7).
                for (List<String> later : rows) {
                    String w = later.get(col);
                    if (w.matches("[+-]?\\d*\\.\\d+([eE][+-]?\\d+)?")
                            || w.matches("[+-]?\\d+[eE][+-]?\\d+")) {
                        return Type.Primitive.FLOAT;
                    }
                }
                return Type.Primitive.INTEGER;
            }
            if (v.matches("[+-]?\\d*\\.\\d+([eE][+-]?\\d+)?")
                    || v.matches("[+-]?\\d+[eE][+-]?\\d+")) {
                return Type.Primitive.FLOAT;
            }
            // pure DECIMAL-suffix cells (21d, 41.0d)
            if (v.matches("[+-]?\\d+(\\.\\d+)?[dD]")) {
                return new Type.PrecisionDecimal(Type.PrecisionDecimal.MAX_PRECISION, 0);
            }
            if (v.equals("true") || v.equals("false")) {
                return Type.Primitive.BOOLEAN;
            }
            // ZONE-SUFFIXED full timestamps infer as the abstract Date —
            // real pure's TDS inference is Deephaven CSV's, whose
            // DATETIME_AS_LONG parser requires seconds AND a zone
            // (2024-01-29T00:32:34.000000000+0000); no-zone or partial-time
            // strings stay String, date-only strings stay String, and the
            // mapped M3 type is Date, not DateTime (TDSExtension.convertType).
            // (+05:30 colon-offset spellings stay String DELIBERATELY until
            // the lowering preserves offsets — accepting them here before
            // audit §1.1 #3 lands would mint mis-lowered instants)
            if (v.matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?([+-]\\d{4}|Z)")) {
                return Type.Primitive.DATE;
            }
            // VALID-JSON array/object cells infer VARIANT. DELIBERATE,
            // LEDGERED divergence: Deephaven (real pure's inference) says
            // String — but the PCT wire drops the fixtures' explicit
            // payload:Variant annotations (toPureGrammar serialization), so
            // inference is the only signal left. Gating on a strict JSON
            // parse keeps bracket-shaped STRING data ('[tag]') a String.
            if (((v.startsWith("[") && v.endsWith("]"))
                    || (v.startsWith("{") && v.endsWith("}")))
                    && isValidJson(v)) {
                return new Type.ClassType(
                        com.legend.compiler.element.type.PlatformTypes.VARIANT);
            }
            return Type.Primitive.STRING;
        }
        return Type.Primitive.STRING;
    }

    /** F3.1b: the parser that decides the TYPE is the parser that reads
     * the VALUE — the platform reader (sql/Json) replaces the private
     * RFC-8259 validator (~145 lines) so the two can never disagree
     * about what is JSON; a VARIANT decided here always parses at read
     * time. Acceptance set = the READER's; its leniency residual vs
     * DuckDB stays ledgered in sql/Json's header. Probe-verified: the
     * gate's pinned case ('[tag]' bracket-shaped STRING data) still
     * refuses, and the reader's leading-zero/trailing-dot number
     * leniencies match what this validator already accepted. */
    private static boolean isValidJson(String s) {
        try {
            com.legend.sql.Json.parse(s);
            return true;
        } catch (RuntimeException e) {
            // DESIGNED total catch (BROAD_CATCH_COUNTS pin): any parse
            // failure means not-JSON — the reader's throw set (ISE,
            // SIOOBE, NFE on garbage) is its own business
            return false;
        }
    }
}
