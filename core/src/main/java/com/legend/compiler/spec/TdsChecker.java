package com.legend.compiler.spec;

import com.legend.compiler.element.type.ExprType;
import com.legend.builtin.Pure;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.parser.spec.AppliedFunction;
import com.legend.parser.spec.CString;

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
        InferenceKernel.Resolution sig = t.kernel().resolveOverload(
                t.model().findFunction(CoreFn.TDS.parseName()),
                List.of(ExprType.one(Type.Primitive.STRING), ExprType.one(Type.Primitive.STRING)));

        String content = rawLit.value().strip();
        if (content.startsWith("#TDS")) {
            content = content.substring(4);
        }
        if (content.endsWith("#")) {
            content = content.substring(0, content.length() - 1);
        }
        String[] lines = content.strip().split("\n");
        if (lines.length == 0 || lines[0].isBlank()) {
            throw new TypeInferenceException("a TDS literal needs a header row of column names");
        }

        // Header: name[:Type] per cell.
        List<String> names = new ArrayList<>();
        List<Type> types = new ArrayList<>();
        for (String cell : splitCells(lines[0])) {
            String c = cell.strip();
            int colon = c.indexOf(':');
            String name = colon > 0 ? c.substring(0, colon).strip() : c;
            if (names.contains(name)) {
                throw new TypeInferenceException("duplicate column '" + name + "' in TDS header");
            }
            names.add(name);
            types.add(colon > 0 ? annotatedType(c.substring(colon + 1).strip()) : null);
        }

        // Data rows (raw cells; carried for lowering).
        List<List<String>> rows = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            if (lines[i].isBlank()) {
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
            columns.add(new Type.Column(names.get(c), type, Multiplicity.Bounded.ONE));
        }
        return new TypedTds(rows,
                new ExprType(new Type.RelationType(columns), sig.output().multiplicity()));
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
            case "Date", "StrictDate" -> Type.Primitive.STRICT_DATE;
            case "DateTime" -> Type.Primitive.DATE_TIME;
            case "Variant", "meta::pure::metamodel::variant::Variant" ->
                    new Type.ClassType(Pure.VARIANT.qualifiedName());
            default -> throw new TypeInferenceException(
                    "TDS column type '" + typeName + "' is not a known primitive");
        };
    }

    /** Infer an unannotated column's type from its first non-empty cell; an empty column is String. */
    private static Type inferredType(List<List<String>> rows, int col) {
        for (List<String> row : rows) {
            String v = row.get(col);
            if (v.isEmpty()) {
                continue;
            }
            if (v.matches("[+-]?\\d+")) {
                return Type.Primitive.INTEGER;
            }
            if (v.matches("[+-]?\\d*\\.\\d+([eE][+-]?\\d+)?")) {
                return Type.Primitive.FLOAT;
            }
            if (v.equals("true") || v.equals("false")) {
                return Type.Primitive.BOOLEAN;
            }
            return Type.Primitive.STRING;
        }
        return Type.Primitive.STRING;
    }
}
