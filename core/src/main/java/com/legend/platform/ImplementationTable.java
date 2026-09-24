// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.NativeFn;
import com.legend.builtin.Pure;
import com.legend.builtin.Subsumed;
import com.legend.compiler.spec.CoreFn;
import com.legend.compiler.spec.WalledBodies;
import com.legend.lowering.RegistryKeys;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * WHAT EXECUTES EVERY DECLARATION: one {@link Implementation} per
 * {@link FunctionId} of a {@link DeclarationTable}, total by construction
 * (platform architecture untangle, step 2).
 *
 * <p>Built ONLY from explicit registrations, each read exactly: a lowering
 * registry's key is matched to the catalog definition that generated it (whole
 * key, never parsed); a family member's overloads ARE definitions; a form, a
 * wall, a walled body and a subsumed program name exact FQNs, and apply to
 * every declaration there. A declaration no registration names takes the
 * default of its kind — {@code Body} when it has a body, {@code Unimplemented}
 * when it is a native. The FQN-level suppressions the compiler applies today
 * (the PCT rule, the platform-owned list) are deliberately NOT inputs: they are
 * what this table replaces, so where they act, today's behaviour and this table
 * differ — the shadow diff (step 3) reports exactly those places.
 *
 * <p>A registration that names nothing the table declares is DANGLING; two
 * registrations that cannot both hold (a refusal and an implementation on one
 * id, two forms on one id) are CONFLICTS. Both are reported, never resolved
 * silently.
 */
public final class ImplementationTable {

    private final Map<FunctionId, Implementation> rows;
    private final List<String> dangling;
    private final List<String> conflicts;
    private final List<String> memberWalls;

    private ImplementationTable(Map<FunctionId, Implementation> rows, List<String> dangling,
            List<String> conflicts, List<String> memberWalls) {
        this.rows = Collections.unmodifiableMap(rows);
        this.dangling = List.copyOf(dangling);
        this.conflicts = List.copyOf(conflicts);
        this.memberWalls = List.copyOf(memberWalls);
    }

    /** The table over {@code declarations}, from today's registrations. */
    public static ImplementationTable build(DeclarationTable declarations) {
        List<String> dangling = new ArrayList<>();
        List<String> conflicts = new ArrayList<>();
        List<String> memberWalls = new ArrayList<>();

        // the catalog definition behind each lowering-registry key: the key IS
        // that definition's signatureKey(), so the match is the whole key
        Map<String, FunctionId> catalogKey = new LinkedHashMap<>();
        for (NativeFunctionDefinition n : Pure.all()) {
            catalogKey.put(n.signatureKey(), FunctionId.of(n));
        }
        Map<FunctionId, Set<Implementation.Position>> positions = new LinkedHashMap<>();
        registerKeys(RegistryKeys.scalarRules(), Implementation.Position.SCALAR, catalogKey, declarations,
                positions, dangling);
        registerKeys(RegistryKeys.reducers(), Implementation.Position.AGGREGATE, catalogKey, declarations,
                positions, dangling);
        registerKeys(RegistryKeys.windowFunctions(), Implementation.Position.WINDOW, catalogKey, declarations,
                positions, dangling);
        registerKeys(RegistryKeys.windowAggregates(), Implementation.Position.WINDOW_AGGREGATE, catalogKey,
                declarations, positions, dangling);

        // the implementer families: each member's overloads ARE definitions
        Map<FunctionId, List<String>> families = new LinkedHashMap<>();
        for (var family : NativeFn.families().entrySet()) {
            for (NativeFn.Member m : family.getValue()) {
                for (NativeFunctionDefinition o : m.overloads()) {
                    FunctionId id = FunctionId.of(o);
                    if (declarations.get(id) == null) {
                        dangling.add("NativeFn." + family.getKey() + " " + id);
                        continue;
                    }
                    List<String> fs = families.computeIfAbsent(id, k -> new ArrayList<>());
                    if (!fs.contains(family.getKey())) {
                        fs.add(family.getKey());
                    }
                }
            }
        }

        // the language forms: every overload at each FQN a form owns
        Map<FunctionId, CoreFn> forms = new LinkedHashMap<>();
        for (CoreFn form : CoreFn.values()) {
            for (String fqn : form.ownedFqns()) {
                List<Function> at = declarations.at(fqn);
                if (at.isEmpty()) {
                    dangling.add("CoreFn." + form.name() + " " + fqn);
                }
                for (Function f : at) {
                    CoreFn prior = forms.put(FunctionId.of(f), form);
                    if (prior != null && prior != form) {
                        conflicts.add(FunctionId.of(f) + ": forms " + prior + " and " + form);
                    }
                }
            }
        }

        // the refusals, each with its reason
        Map<FunctionId, Implementation.Refused> refused = new LinkedHashMap<>();
        for (String fqn : Pure.walledNativeFqns()) {
            refuse(fqn, new Implementation.Refused(Implementation.Reason.CANNOT_IMPLEMENT,
                    java.util.Objects.requireNonNull(Pure.walledNativeReason(fqn))), "Pure.WALLED_NATIVES",
                    declarations, refused, dangling);
        }
        for (var wall : WalledBodies.reasons().entrySet()) {
            if (declarations.at(wall.getKey()).isEmpty()) {
                // a walled CLASS MEMBER body (a lifted derived property or
                // constraint), not a function declaration: reported apart
                memberWalls.add(wall.getKey());
                continue;
            }
            Implementation.Reason reason = switch (wall.getValue().kind()) {
                case ENGINE_MACHINERY -> Implementation.Reason.ENGINE_MACHINERY;
                case CANNOT_IMPLEMENT -> Implementation.Reason.CANNOT_IMPLEMENT;
            };
            refuse(wall.getKey(), new Implementation.Refused(reason, wall.getValue().why()), "WalledBodies",
                    declarations, refused, dangling);
        }
        for (Subsumed s : Subsumed.values()) {
            refuse(s.fqn(), new Implementation.Refused(Implementation.Reason.MOOT,
                    "subsumed: its value is never needed on this platform"), "Subsumed",
                    declarations, refused, dangling);
        }

        // one row per declaration
        Map<FunctionId, Implementation> rows = new LinkedHashMap<>();
        for (FunctionId id : declarations.ids()) {
            Set<Implementation.Position> ps = positions.getOrDefault(id, Set.of());
            List<String> fs = families.getOrDefault(id, List.of());
            CoreFn form = forms.get(id);
            Implementation.Refused refusal = refused.get(id);
            Implementation row;
            if (form != null) {
                if (refusal != null) {
                    conflicts.add(id + ": form " + form + " and a refusal (" + refusal.reason() + ")");
                }
                row = new Implementation.Form(form, ps, fs);
            } else if (refusal != null) {
                if (!ps.isEmpty() || !fs.isEmpty()) {
                    conflicts.add(id + ": refused (" + refusal.reason() + ") and implemented " + ps + fs);
                }
                row = refusal;
            } else if (!ps.isEmpty() || !fs.isEmpty()) {
                row = new Implementation.Intrinsic(ps, fs);
            } else if (declarations.get(id) instanceof FunctionDefinition) {
                row = new Implementation.Body();
            } else {
                row = new Implementation.Unimplemented();
            }
            rows.put(id, row);
        }
        return new ImplementationTable(rows, dangling, conflicts, memberWalls);
    }

    private static void registerKeys(Set<String> keys, Implementation.Position position,
            Map<String, FunctionId> catalogKey, DeclarationTable declarations,
            Map<FunctionId, Set<Implementation.Position>> positions, List<String> dangling) {
        for (String key : keys) {
            FunctionId id = catalogKey.get(key);
            if (id == null || declarations.get(id) == null) {
                dangling.add(position + " " + key);
                continue;
            }
            positions.computeIfAbsent(id, k -> EnumSet.noneOf(Implementation.Position.class)).add(position);
        }
    }

    private static void refuse(String fqn, Implementation.Refused refusal, String source,
            DeclarationTable declarations, Map<FunctionId, Implementation.Refused> refused,
            List<String> dangling) {
        List<Function> at = declarations.at(fqn);
        if (at.isEmpty()) {
            dangling.add(source + " " + fqn);
        }
        for (Function f : at) {
            refused.put(FunctionId.of(f), refusal);
        }
    }

    /** The implementation of the declaration {@code id}, or null if the table does not declare it. */
    public @com.legend.Nullable Implementation of(FunctionId id) {
        return rows.get(id);
    }

    /** Every row, in declaration order. */
    public Map<FunctionId, Implementation> rows() {
        return rows;
    }

    /** Registrations naming nothing the table declares. */
    public List<String> dangling() {
        return dangling;
    }

    /** Registrations that cannot both hold. */
    public List<String> conflicts() {
        return conflicts;
    }

    /** Walled class-member bodies (lifted derived properties, constraints): walls that are not function rows. */
    public List<String> memberWalls() {
        return memberWalls;
    }
}
