// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.lineage;

import com.legend.compiler.element.ModelContext;
import com.legend.model.DatabaseDefinition;
import com.legend.protocol.spec.AppliedFunction;
import com.legend.protocol.spec.CString;
import com.legend.protocol.spec.ColSpec;
import com.legend.protocol.spec.ColSpecArray;
import com.legend.protocol.spec.PackageableElementPtr;
import com.legend.protocol.spec.ValueSpecification;

import java.util.ArrayList;
import java.util.List;

/**
 * The engine's relation-function PRIMARY-KEY auto-inference (#78 — the
 * pkInferenceTests matrix IS the behavioral spec; the engine's own
 * implementation is M3-reflective and never ported): a parse-space walk
 * from a {@code #>{db.TABLE}#} accessor through the relation-op chain.
 *
 * <ul>
 *   <li>bare accessor &rarr; the store table's PK column names</li>
 *   <li>filter/limit/drop/slice/sort/extend/from &rarr; pass-through</li>
 *   <li>rename(~old, ~new) &rarr; PK columns rename along</li>
 *   <li>select(~cols) &rarr; PK kept whole or EMPTY; select() passes</li>
 *   <li>distinct(~cols) &rarr; those columns; distinct() passes</li>
 *   <li>groupBy &rarr; the KEY columns</li>
 *   <li>join &rarr; both sides' PKs concatenated</li>
 *   <li>aggregate/concatenate/pivot/non-accessor leaf &rarr; EMPTY
 *       (the engine's conservative arms)</li>
 *   <li>a call to a corpus relation FUNCTION composes through its
 *       parsed body</li>
 * </ul>
 */
public final class PkInference {

    private PkInference() {
    }

    public static List<String> infer(ModelContext ctx,
            ValueSpecification v) {
        if (v instanceof AppliedFunction af) {
            String simple = af.function()
                    .substring(af.function().lastIndexOf(':') + 1);
            List<ValueSpecification> ps = af.parameters();
            switch (simple) {
                case "tableReference" -> {
                    return tablePk(ctx, ps);
                }
                case "filter", "limit", "drop", "slice", "sort", "extend",
                        "from" -> {
                    return infer(ctx, ps.get(0));
                }
                case "select" -> {
                    if (ps.size() == 1) {
                        return infer(ctx, ps.get(0));
                    }
                    List<String> pk = infer(ctx, ps.get(0));
                    List<String> kept = colNames(ps.get(1));
                    return kept.containsAll(pk) ? pk : List.of();
                }
                case "rename" -> {
                    List<String> pk =
                            new ArrayList<>(infer(ctx, ps.get(0)));
                    if (ps.size() >= 3) {
                        List<String> from = colNames(ps.get(1));
                        List<String> to = colNames(ps.get(2));
                        if (from.size() == 1 && to.size() == 1) {
                            pk.replaceAll(c -> c.equals(from.get(0))
                                    ? to.get(0) : c);
                        }
                    }
                    return pk;
                }
                case "distinct" -> {
                    return ps.size() == 1 ? infer(ctx, ps.get(0))
                            : colNames(ps.get(1));
                }
                case "groupBy" -> {
                    return ps.size() >= 2 ? colNames(ps.get(1))
                            : List.of();
                }
                case "join" -> {
                    List<String> out =
                            new ArrayList<>(infer(ctx, ps.get(0)));
                    out.addAll(infer(ctx, ps.get(1)));
                    return out;
                }
                case "aggregate", "concatenate", "pivot", "getAll" -> {
                    return List.of();
                }
                default -> {
                    // composition: a corpus relation function's body
                    var fd = ctx.findFunctionDefinition(af.function());
                    if (fd.isEmpty() && !af.referents().isEmpty()) {
                        for (String c : af.referents()) {
                            fd = ctx.findFunctionDefinition(c);
                            if (fd.isPresent()) {
                                break;
                            }
                        }
                    }
                    if (fd.isPresent() && !fd.get().body().isEmpty()) {
                        return infer(ctx, fd.get().body()
                                .get(fd.get().body().size() - 1));
                    }
                    return List.of();
                }
            }
        }
        return List.of();
    }

    private static List<String> tablePk(ModelContext ctx,
            List<ValueSpecification> ps) {
        if (ps.size() < 2 || !(ps.get(0) instanceof PackageableElementPtr db)
                || !(ps.get(ps.size() - 1) instanceof CString t)) {
            return List.of();
        }
        String table = t.value();
        int dot = table.lastIndexOf('.');
        if (dot >= 0) {
            table = table.substring(dot + 1);
        }
        var td = ctx.findTableDefinition(db.fullPath(), table);
        if (td.isEmpty()) {
            return List.of();
        }
        List<String> pk = new ArrayList<>();
        for (DatabaseDefinition.ColumnDefinition c : td.get().columns()) {
            if (c.primaryKey()) {
                pk.add(c.name());
            }
        }
        return pk;
    }

    private static List<String> colNames(ValueSpecification v) {
        List<String> out = new ArrayList<>();
        if (v instanceof ColSpec cs) {
            out.add(cs.name());
        } else if (v instanceof ColSpecArray ca) {
            for (ColSpec cs : ca.colSpecs()) {
                out.add(cs.name());
            }
        }
        return out;
    }
}
