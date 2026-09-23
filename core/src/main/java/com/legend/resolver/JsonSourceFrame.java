// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.resolver;

import com.legend.compiler.element.ModelContext;
import com.legend.compiler.element.TypedFunction;
import com.legend.compiler.element.type.ExprType;
import com.legend.compiler.element.type.Multiplicity;
import com.legend.compiler.element.type.PlatformTypes;
import com.legend.compiler.element.type.Type;
import com.legend.compiler.spec.typed.TypedCast;
import com.legend.compiler.spec.typed.TypedCString;
import com.legend.compiler.spec.typed.TypedNativeCall;
import com.legend.compiler.spec.typed.TypedPropertyAccess;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedTds;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE JSON SOURCE FRAME (XStore leg §1, re-platformed by JAVA_EVICTION_PLAN
 * E3): a {@code JsonModelConnection(class=C, url='data:application/json,…')}
 * realizes as a one-Variant-column VALUES relation — one row per payload
 * object, each cell the object's RAW JSON TEXT. Java only does SCISSORS:
 * {@link #objectTexts} cuts the payload into per-object spans lexically
 * (a string-aware brace scan — no JSON value ever materializes in Java);
 * the DATABASE does all reading (every property binds as a typed variant
 * extraction over the {@code data} cell: registered {@code get} + the
 * {@code to(@T)} cast + {@code toOne}). The CLASS DECLARATION is the
 * schema (model-driven DDL discipline); class-typed properties contribute
 * nothing (reads through them keep their own walls).
 */
final class JsonSourceFrame {

    /** The hidden VALUES row-identity column (never a binding). */
    static final String FRAME_ORDINAL = "u_frame_ord__";

    private JsonSourceFrame() {
    }

    /** {@code ${var}} URL-template substitution from the let env (the
     * engine binds url parameters from in-scope query lets); a var whose
     * let is not a string LITERAL stays verbatim — its row is textual and
     * downstream filters treat it as data (loud divergence, never crash). */
    static Map<String, String> substituteUrlParams(Map<String, String> urls,
            Map<String, TypedSpec> letBindings) {
        Map<String, String> out = new LinkedHashMap<>();
        for (var e : urls.entrySet()) {
            java.util.regex.Matcher m = java.util.regex.Pattern
                    .compile("\\$\\{(\\w+)\\}").matcher(e.getValue());
            // StringBuffer, not StringBuilder: the StringBuilder overloads
            // of Matcher.appendReplacement/appendTail are Java 9 additions
            // that TeaVM's class library does not carry, and the planner
            // has to survive an ahead-of-time compile to WebAssembly. The
            // synchronisation costs nothing on a local that never escapes.
            StringBuffer sb = new StringBuffer();
            while (m.find()) {
                TypedSpec b = letBindings.get(m.group(1));
                m.appendReplacement(sb,
                        java.util.regex.Matcher.quoteReplacement(
                                b instanceof TypedCString cs
                                        ? cs.value() : m.group(0)));
            }
            m.appendTail(sb);
            out.put(e.getKey(), sb.toString());
        }
        return out;
    }

    /** The payload cut into per-object TEXT spans, LEXICALLY (no JSON
     * value ever materializes in Java — the DB parses the cells): an
     * array yields its top-level objects; a single object yields itself;
     * the engine's concatenated row-stream spelling ({@code {..}{..}} /
     * newline-separated) yields one span per object. A string-aware
     * top-level brace scan, loud on anything else. */
    static List<String> objectTexts(String payload, String classFqn) {
        String t = payload.strip();
        List<String> out = new ArrayList<>();
        if (t.isEmpty()) {
            return out;         // an empty payload is a zero-row frame
        }
        boolean array = t.startsWith("[");
        String body = t;
        if (array) {
            if (!t.endsWith("]")) {
                throw new NotImplementedException("JSON source for '"
                        + classFqn + "' carries an unterminated array");
            }
            body = t.substring(1, t.length() - 1);
        }
        int i = 0;
        int n = body.length();
        while (i < n) {
            char ch = body.charAt(i);
            if (Character.isWhitespace(ch) || (array && ch == ',')) {
                i++;
                continue;       // inter-object gap (stream or array form)
            }
            if (ch != '{') {
                throw new NotImplementedException("JSON source for '"
                        + classFqn + "' is neither an object nor an array"
                        + " of objects");
            }
            int start = i;
            int depth = 0;
            boolean inString = false;
            boolean escaped = false;
            for (; i < n; i++) {
                char c = body.charAt(i);
                if (inString) {
                    if (escaped) {
                        escaped = false;
                    } else if (c == '\\') {
                        escaped = true;
                    } else if (c == '"') {
                        inString = false;
                    }
                } else if (c == '"') {
                    inString = true;
                } else if (c == '{') {
                    depth++;
                } else if (c == '}') {
                    depth--;
                    if (depth == 0) {
                        i++;
                        break;
                    }
                }
            }
            if (depth != 0 || inString) {
                throw new NotImplementedException("JSON source for '"
                        + classFqn + "' carries a truncated object stream");
            }
            out.add(body.substring(start, i));
        }
        return out;
    }

    static ClassSource sourceUrlFrame(ModelContext ctx, String mappingFqn,
            String classFqn, String url) {
        String prefix = "data:application/json,";
        if (!url.startsWith(prefix)) {
            throw new NotImplementedException("JsonModelConnection url for '"
                    + classFqn + "' is not a data:application/json literal —"
                    + " remote/parameterized sources are not supported yet");
        }
        List<String> objects = objectTexts(url.substring(prefix.length()),
                classFqn);
        var cls = ctx.findClass(classFqn).orElseThrow(() ->
                new IllegalStateException("resolver bug: JSON-sourced class '"
                        + classFqn + "' unknown to the model"));
        Type variant = new Type.ClassType(
                com.legend.compiler.element.type.PlatformTypes.VARIANT);
        var one = Multiplicity.Bounded.ONE;
        var zeroOne = Multiplicity.Bounded.ZERO_ONE;
        // one Variant cell per row: the object's RAW TEXT, quote-wrapped
        // for the grid (Scalars.tdsCell's variant arm strips one outer
        // quote pair and emits CAST('…' AS JSON) — the DB parses); plus
        // the HIDDEN ROW ORDINAL, the VALUES row identity: two sets
        // composed over the SAME frame correlate on it (mixed-union
        // per-member children, XSTORE_LEG design). Not a class property:
        // excluded from bindings, so no serialize leaf or query read
        // ever sees it.
        List<List<String>> rows = new ArrayList<>(objects.size());
        for (int i = 0; i < objects.size(); i++) {
            rows.add(List.of("\"" + objects.get(i) + "\"",
                    String.valueOf(i)));
        }
        Type.RelationType rowType = new Type.RelationType(List.of(
                new Type.Column("data", variant, one),
                new Type.Column(FRAME_ORDINAL, Type.Primitive.INTEGER, one)));
        ExprType rowInfo = new ExprType(rowType, one);
        TypedSpec pipeline = new TypedTds(rows,
                new ExprType(Type.relation(rowType), one));
        String rowVar = "src_json";
        TypedSpec data = new TypedPropertyAccess(
                new TypedVariable(rowVar, rowInfo), "data",
                new ExprType(variant, one));
        // the STRING-key overload (real get.pure has two 2-arg forms:
        // String key and Integer index — audit slice 2 registered both)
        TypedFunction getFn = ctx.findFunction(com.legend.builtin.NativeFn.ResolverForm.VARIANT_GET.fqn())
                .stream()
                .filter(f -> f.parameters().size() == 2
                        && f.parameters().get(1).type()
                                == com.legend.compiler.element.type
                                        .Type.Primitive.STRING)
                .findFirst().orElseThrow(() -> new IllegalStateException(
                        "resolver bug: the String-key variant get overload"
                        + " is not in the catalog"));
        TypedFunction toOneFn = fn(ctx,
                com.legend.builtin.Pure.Lite.TRUST_ONE, 1);
        Map<String, TypedSpec> bindings = new LinkedHashMap<>();
        for (var p : cls.properties()) {
            // Variant IS a column carrier; other class-typed properties
            // contribute nothing (reads through them keep their own walls)
            if (Type.asClassType(p.type()) instanceof Type.ClassType ct
                    && !PlatformTypes.isVariant(ct)) {
                continue;
            }
            TypedSpec v = new TypedNativeCall(getFn, List.of(data,
                    new TypedCString(p.name(),
                            new ExprType(Type.Primitive.STRING, one))),
                    new ExprType(variant, zeroOne));
            if (!PlatformTypes.isVariant(p.type())) {
                // the platform's own to(@T) seam: -> becomes ->> + CAST
                v = new TypedCast(v, p.type(),
                        new ExprType(p.type(), zeroOne), false);
            }
            if (one.equals(p.multiplicity())) {
                // conform to the declared [1] BY EMISSION (toOne erases
                // value-wise in SQL — an absent key stays a NULL cell)
                v = new TypedNativeCall(toOneFn, List.of(v),
                        new ExprType(p.type(), one));
            }
            bindings.put(p.name(), v);
        }
        if (bindings.isEmpty()) {
            throw new NotImplementedException("JSON-sourced class '" + classFqn
                    + "' declares no scalar properties — nothing to realize");
        }
        return new ClassSource(mappingFqn, classFqn, "json", pipeline,
                rowVar, bindings, rowType, classFqn);
    }

    private static TypedFunction fn(ModelContext ctx, String fqn, int arity) {
        List<TypedFunction> fns = ctx.findFunction(fqn).stream()
                .filter(f -> f.parameters().size() == arity).toList();
        if (fns.size() != 1) {
            throw new IllegalStateException("resolver bug: expected exactly"
                    + " one " + arity + "-arg '" + fqn + "' in the catalog,"
                    + " found " + fns.size());
        }
        return fns.get(0);
    }

    /** ONE from()-scope entry (StoreResolver.fromContext): the re-scoped
     * Context; the runtime rides ALONGSIDE an explicit mapping (a
     * self-sourced M2M's upstream dispatch needs the candidate set). */
    static StoreResolver.Context fromContext(
            com.legend.compiler.spec.typed.TypedFrom fr,
            StoreResolver.Context outer, ClassSources sources,
            java.util.Map<String, com.legend.compiler.spec.typed.TypedSpec>
                    letBindings) {
        if (!fr.jsonSources().isEmpty()) {
            sources.setJsonSources(substituteUrlParams(
                    fr.jsonSources(), letBindings));
        }
        return scoped(fr, outer).withExecutedExtent(fr.executedExtent())
                .withExtentFrame(fr.extentFrame());
    }

    private static StoreResolver.Context scoped(
            com.legend.compiler.spec.typed.TypedFrom fr,
            StoreResolver.Context outer) {
        // a from() that declares no chain of its own INHERITS the enclosing
        // one (a plan execution re-evaluated as a value under a chained
        // runtime)
        var bound = fr.context().inheritingChain(outer.chainMappings());
        if (fr.mapping().isPresent()) {
            return new StoreResolver.Context(fr.mapping().get().fullPath(),
                    fr.runtime().map(r -> r.fullPath())
                            .orElse(outer.runtimeFqn()),
                    bound.chainMappings(), fr.jsonSources(), null);
        }
        if (fr.runtime().isPresent()) {
            return new StoreResolver.Context(null,
                    fr.runtime().get().fullPath(),
                    bound.chainMappings(), fr.jsonSources(), null);
        }
        if (!fr.chainMappings().isEmpty() || !fr.jsonSources().isEmpty()) {
            // INSTANCE-runtime from() (no mapping ref, no runtime ref)
            // still carries the chain channel — dropping it sent
            // query-side withChainedMappings chains to the ambient
            // candidate list (slice-1 job 1)
            return new StoreResolver.Context(outer.explicitMapping(),
                    outer.runtimeFqn(),
                    fr.chainMappings(), fr.jsonSources(), null);
        }
        return outer;
    }
}
