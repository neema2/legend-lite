package com.legend.compiler.spec.typed;

import com.legend.compiler.element.type.ExprType;

import java.util.List;
import java.util.Optional;

/**
 * An execution-context binding {@code ->from(runtime)} / {@code ->from(mapping,
 * runtime)} (engine {@code TypedFrom}) &mdash; a type passthrough
 * ({@code Relation<T>[1]} / {@code T[*]}) that carries the bound
 * {@link ExecutionContext} for the back-end. The context is a VALUE read once
 * by the special form's rule (docs/EXECUTION_CONTEXT_DESIGN_2026_09_06.md);
 * the accessors below are its fields.
 *
 * @param source  the value being bound to an execution context
 * @param context the bound execution context
 * @param executedExtent EXECUTED EXTENT (batch 78): this envelope stands for
 *                the VALUES of an executed {@code execute()} frame — the
 *                instances the engine materialized — so a read over it ranges
 *                over the extent's rows. Set by the result-envelope splice
 *                only; a query's own from() never carries it.
 * @param info    the source type unchanged
 */
public record TypedFrom(TypedSpec source, ExecutionContext context,
                        boolean executedExtent, @com.legend.base.Nullable String extentFrame,
                        ExprType info) implements TypedSpec {

    public TypedFrom(TypedSpec source, ExecutionContext context, ExprType info) {
        this(source, context, false, null, info);
    }

    /** The same envelope (context, executed-extent fact, extent frame) over
     * another source; a rebuild never drops the frame a reader ranges over. */
    public TypedFrom withSource(TypedSpec src, ExprType info) {
        return new TypedFrom(src, context, executedExtent, extentFrame, info);
    }

    /** The same envelope as the EXECUTED EXTENT of a planned class frame
     * (lean ladder rung 12): the class's root table is the frame's CTE. */
    public TypedFrom withExtentFrame(String frame) {
        return new TypedFrom(source, context, true, frame, info);
    }

    /** References only (a wrapper envelope). */
    public TypedFrom(TypedSpec source, Optional<TypedPackageableRef> mapping,
                     Optional<TypedPackageableRef> runtime, ExprType info) {
        this(source, ExecutionContext.of(mapping, runtime), false, null, info);
    }

    /** The same envelope flagged as an executed frame's extent. */
    public TypedFrom withExecutedExtent() {
        return new TypedFrom(source, context, true, extentFrame, info);
    }

    /** The same envelope under another context. */
    public TypedFrom withContext(ExecutionContext c) {
        return new TypedFrom(source, c, executedExtent, extentFrame, info);
    }

    public Optional<TypedPackageableRef> mapping() {
        return context.mapping();
    }

    public Optional<TypedPackageableRef> runtime() {
        return context.runtime();
    }

    public List<String> chainMappings() {
        return context.chainMappings();
    }

    public java.util.Map<String, String> jsonSources() {
        return context.jsonSources();
    }

    public List<String> sqlSetups() {
        return context.sqlSetups();
    }

    public List<ExecutionContext.CsvSetup> csvSetups() {
        return context.csvSetups();
    }

    public @com.legend.base.Nullable String connectionName() {
        return context.connectionName();
    }

    @Override
    public List<TypedSpec> children() {
        List<TypedSpec> out = new java.util.ArrayList<>();
        out.add(source);
        context.mapping().ifPresent(out::add);
        context.runtime().ifPresent(out::add);
        return out;
    }

    @Override
    public TypedSpec withChildren(java.util.List<TypedSpec> kids) {
        int n = 1 + (context.mapping().isPresent() ? 1 : 0)
                + (context.runtime().isPresent() ? 1 : 0);
        TypedSpec.expectChildren(kids, n, "TypedFrom");
        int i = 1;
        Optional<TypedPackageableRef> m = context.mapping().isPresent()
                ? Optional.of((TypedPackageableRef) kids.get(i++))
                : Optional.empty();
        Optional<TypedPackageableRef> r = context.runtime().isPresent()
                ? Optional.of((TypedPackageableRef) kids.get(i))
                : Optional.empty();
        return new TypedFrom(kids.get(0), context.withMapping(m).withRuntime(r),
                executedExtent, extentFrame, info);
    }
    @Override
    public TypedSpec withInfo(ExprType info) {
        return new TypedFrom(source, context, executedExtent, extentFrame, info);
    }
}
