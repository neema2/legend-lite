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
import com.legend.compiler.spec.typed.TypedSourceUrl;
import com.legend.compiler.spec.typed.TypedSpec;
import com.legend.compiler.spec.typed.TypedVariable;
import com.legend.error.NotImplementedException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE JSON SOURCE FRAME (XStore leg §1, re-platformed by JAVA_EVICTION_PLAN
 * E3; onto the load by B2a, 2026-11-11, docs/DATA_DELIVERY_DESIGN_2026_11_10.md
 * §5): a {@code JsonModelConnection(class=C, url='data:application/json,…')}
 * realizes as the payload's {@code data:} source relation -- a session table
 * the plan's load makes, one row per record holding its raw JSON text and its
 * place ({@link TypedSourceUrl#ORDINAL}); the planner cuts the payload into its
 * records and loads them ({@code planner.DataSources}). The DATABASE does all
 * reading (every property binds as a typed variant extraction over the
 * {@code data} cell: registered {@code get} + the {@code to(@T)} cast +
 * {@code toOne}). The CLASS DECLARATION is the schema (model-driven DDL
 * discipline); class-typed properties contribute nothing (reads through them
 * keep their own walls).
 */
final class JsonSourceFrame {

    /** The hidden row-identity column (never a binding). */
    static final String FRAME_ORDINAL = com.legend.compiler.spec.typed.TypedSourceUrl.ORDINAL;

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

    /** {@code classFqn}'s frame over the JSON connection's {@code url}: the
     *  payload's {@code data:} source relation, each class property a typed
     *  read of a record's {@code data} cell. */
    static ClassSource frame(ModelContext ctx, String mappingFqn,
            String classFqn, String url) {
        String prefix = "data:application/json,";
        if (!url.startsWith(prefix)) {
            throw new NotImplementedException("JsonModelConnection url for '"
                    + classFqn + "' is not a data:application/json literal —"
                    + " remote/parameterized sources are not supported yet");
        }
        var cls = ctx.findClass(classFqn).orElseThrow(() ->
                new IllegalStateException("resolver bug: JSON-sourced class '"
                        + classFqn + "' unknown to the model"));
        Type variant = new Type.ClassType(
                com.legend.compiler.element.type.PlatformTypes.VARIANT);
        var one = Multiplicity.Bounded.ONE;
        var zeroOne = Multiplicity.Bounded.ZERO_ONE;
        // one row per record: its RAW TEXT, a Variant cell the database parses,
        // and the HIDDEN ROW ORDINAL, its place in the payload: two sets
        // composed over the SAME frame read one table and correlate on it
        // (mixed-union per-member children, XSTORE_LEG design). Not a class
        // property: excluded from bindings, so no serialize leaf or query
        // read ever sees it. The payload's records are the planner's cut
        // (DataSources), loaded by the plan: never rows in the statement.
        Type.RelationType rowType = new Type.RelationType(List.of(
                new Type.Column("data", variant, one),
                new Type.Column(FRAME_ORDINAL, Type.Primitive.INTEGER, one)));
        ExprType rowInfo = new ExprType(rowType, one);
        TypedSpec pipeline = new TypedSourceUrl(url,
                new ExprType(Type.relation(rowType), one), true);
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
