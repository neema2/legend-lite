package com.gs.legend.plan.lowering.scalar;

import com.gs.legend.compiler.typed.TypedCBoolean;
import com.gs.legend.compiler.typed.TypedCByteArray;
import com.gs.legend.compiler.typed.TypedCDateTime;
import com.gs.legend.compiler.typed.TypedCDecimal;
import com.gs.legend.compiler.typed.TypedCFloat;
import com.gs.legend.compiler.typed.TypedCInteger;
import com.gs.legend.compiler.typed.TypedCLatestDate;
import com.gs.legend.compiler.typed.TypedCString;
import com.gs.legend.compiler.typed.TypedCStrictDate;
import com.gs.legend.compiler.typed.TypedCStrictTime;
import com.gs.legend.compiler.typed.TypedEnumValue;
import com.gs.legend.plan.lowering.LoweringContext;
import com.gs.legend.sqlgen.SqlExpr;

/**
 * All typed literals. Each rule maps the typed HIR literal to its {@link SqlExpr}
 * counterpart; dialect-specific formatting is deferred to {@link SqlExpr#toSql}.
 *
 * <p>Date/time literals carry the raw textual value from the AST including the
 * leading {@code %} prefix in Pure syntax (e.g., {@code %2024-01-15}); the lowered
 * {@link SqlExpr.DateLiteral} strips it so downstream dialect formatters receive
 * clean ISO text.
 */
public final class LiteralLowering {
    private LiteralLowering() {}

    public static SqlExpr lower(TypedCInteger n, LoweringContext ctx) {
        return new SqlExpr.NumericLiteral(n.value());
    }

    public static SqlExpr lower(TypedCFloat n, LoweringContext ctx) {
        return new SqlExpr.NumericLiteral(n.value());
    }

    public static SqlExpr lower(TypedCDecimal n, LoweringContext ctx) {
        return new SqlExpr.DecimalLiteral(n.value());
    }

    public static SqlExpr lower(TypedCString n, LoweringContext ctx) {
        return new SqlExpr.StringLiteral(n.value());
    }

    public static SqlExpr lower(TypedCBoolean n, LoweringContext ctx) {
        return new SqlExpr.BoolLiteral(n.value());
    }

    public static SqlExpr lower(TypedCDateTime n, LoweringContext ctx) {
        return new SqlExpr.TimestampLiteral(stripPercent(n.value()));
    }

    public static SqlExpr lower(TypedCStrictDate n, LoweringContext ctx) {
        return new SqlExpr.DateLiteral(stripPercent(n.value()));
    }

    public static SqlExpr lower(TypedCStrictTime n, LoweringContext ctx) {
        return new SqlExpr.TimeLiteral(stripPercent(n.value()));
    }

    public static SqlExpr lower(TypedCLatestDate n, LoweringContext ctx) {
        // %latest — the unbounded-upper temporal sentinel. Rendered as a timestamp
        // literal in dialect-specific form. Upstream passes that want an explicit
        // "open upper bound" semantic translate this to dialect.latestDate() themselves;
        // here we emit a conservative far-future timestamp the same way the legacy did.
        return new SqlExpr.TimestampLiteral("9999-12-31T23:59:59");
    }

    public static SqlExpr lower(TypedCByteArray n, LoweringContext ctx) {
        // Byte-array literals are uncommon; render as a hex-encoded string literal so
        // the downstream dialect can CAST to its blob type if needed.
        StringBuilder hex = new StringBuilder(n.value().length * 2);
        for (byte b : n.value()) {
            hex.append(Character.forDigit((b >> 4) & 0xF, 16));
            hex.append(Character.forDigit(b & 0xF, 16));
        }
        return new SqlExpr.StringLiteral(hex.toString());
    }

    public static SqlExpr lower(TypedEnumValue n, LoweringContext ctx) {
        // Enum values lower to the bare member name as a string literal. Mapping
        // substitutions (enum -> physical code) are applied during property-access
        // lowering in Stage 3 via StoreResolution; here we keep the logical name.
        return new SqlExpr.StringLiteral(n.member());
    }

    private static String stripPercent(String raw) {
        return raw != null && raw.startsWith("%") ? raw.substring(1) : raw;
    }
}
