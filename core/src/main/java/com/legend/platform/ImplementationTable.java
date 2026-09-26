// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.platform;

import com.legend.builtin.NativeFn;
import com.legend.model.ClassMember;
import com.legend.model.Function;
import com.legend.model.FunctionDefinition;
import com.legend.model.NativeFunctionDefinition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * WHAT EXECUTES EVERY DECLARATION: one {@link Implementation} per
 * {@link FunctionId} of a {@link DeclarationTable}, total by construction
 * (platform architecture untangle, step 2).
 *
 * <p>Built ONLY from explicit {@link Registrations}, each read exactly: a
 * lowering registry's key is matched to the catalog definition that generated
 * it (whole key, never parsed); a family member's overloads ARE definitions; a
 * form, a wall, a walled body and a subsumed program name exact FQNs, and apply
 * to every declaration there. A declaration no registration names takes the
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

    /** The table over {@code declarations}, from {@code registrations}. */
    public static ImplementationTable build(DeclarationTable declarations, Registrations registrations) {
        List<String> dangling = new ArrayList<>();
        List<String> conflicts = new ArrayList<>();
        List<String> memberWalls = new ArrayList<>();

        // the catalog definition behind each lowering-registry key: the key IS
        // that definition's signatureKey(), so the match is the whole key
        Map<String, FunctionId> catalogKey = new LinkedHashMap<>();
        for (NativeFunctionDefinition n : registrations.catalog()) {
            catalogKey.put(n.signatureKey(), FunctionId.of(n));
        }
        Map<FunctionId, Set<Implementation.Position>> positions = new LinkedHashMap<>();
        for (var e : registrations.loweringKeys().entrySet()) {
            for (String key : e.getValue()) {
                FunctionId id = catalogKey.get(key);
                if (id == null || declarations.get(id) == null) {
                    dangling.add(e.getKey() + " " + key);
                    continue;
                }
                positions.computeIfAbsent(id, k -> EnumSet.noneOf(Implementation.Position.class)).add(e.getKey());
            }
        }
        Map<FunctionId, Set<Feature>> overrides = new LinkedHashMap<>();
        for (var e : registrations.featureOverrides().entrySet()) {
            for (String key : e.getValue()) {
                FunctionId id = catalogKey.get(key);
                if (id == null || declarations.get(id) == null) {
                    dangling.add("feature " + e.getKey() + " " + key);
                    continue;
                }
                overrides.computeIfAbsent(id, k -> EnumSet.noneOf(Feature.class)).add(e.getKey());
            }
        }

        // the implementer families: each member's overloads ARE definitions
        Map<FunctionId, Set<Class<? extends NativeFn.Member>>> families = new LinkedHashMap<>();
        for (var family : registrations.families().entrySet()) {
            for (NativeFunctionDefinition o : family.getValue()) {
                FunctionId id = FunctionId.of(o);
                if (declarations.get(id) == null) {
                    dangling.add(family.getKey().getSimpleName() + " " + id);
                    continue;
                }
                families.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(family.getKey());
            }
        }

        // the class members a family implements: a lifted derived-property
        // declaration whose PROVENANCE names a registered member. Verified
        // against class declarations, not here — a table without lifted
        // functions (the catalog alone) simply has no such rows
        for (FunctionId id : declarations.ids()) {
            if (declarations.get(id) instanceof FunctionDefinition fd && fd.synthesizedFrom() != null
                    && fd.synthesizedFrom().hat() == com.legend.model.SynthHat.PROP) {
                ClassMember member = new ClassMember(fd.synthesizedFrom().ownerFqn(), fd.synthesizedFrom().memberName());
                for (var family : registrations.members().entrySet()) {
                    if (family.getValue().contains(member)) {
                        families.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(family.getKey());
                    }
                }
            }
        }

        // the language forms: every overload at each FQN a form owns
        Map<FunctionId, CoreFn> forms = new LinkedHashMap<>();
        for (var e : registrations.forms().entrySet()) {
            for (String fqn : e.getValue()) {
                List<Function> at = declarations.at(fqn);
                if (at.isEmpty()) {
                    dangling.add("CoreFn." + e.getKey().name() + " " + fqn);
                }
                for (Function f : at) {
                    CoreFn prior = forms.put(FunctionId.of(f), e.getKey());
                    if (prior != null && prior != e.getKey()) {
                        conflicts.add(FunctionId.of(f) + ": forms " + prior + " and " + e.getKey());
                    }
                }
            }
        }

        // the refusals, each with its reason
        Map<FunctionId, Implementation.Refused> refused = new LinkedHashMap<>();
        for (var e : registrations.walledNatives().entrySet()) {
            refuse(e.getKey(), new Implementation.Refused(Implementation.Reason.CANNOT_IMPLEMENT, e.getValue()),
                    "walled native", declarations, refused, dangling);
        }
        for (var wall : registrations.walledBodies().entrySet()) {
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
            refuse(wall.getKey(), new Implementation.Refused(reason, wall.getValue().why()), "walled body",
                    declarations, refused, dangling);
        }
        for (String fqn : registrations.subsumed()) {
            refuse(fqn, new Implementation.Refused(Implementation.Reason.MOOT,
                    "subsumed: its value is never needed on this platform"), "subsumed",
                    declarations, refused, dangling);
        }

        // one row per declaration
        Map<FunctionId, Implementation> rows = new LinkedHashMap<>();
        for (FunctionId id : declarations.ids()) {
            Set<Implementation.Position> ps = positions.getOrDefault(id, Set.of());
            Set<Feature> fo = overrides.getOrDefault(id, Set.of());
            Set<Class<? extends NativeFn.Member>> fs = families.getOrDefault(id, Set.of());
            CoreFn form = forms.get(id);
            Implementation.Refused refusal = refused.get(id);
            boolean implemented = !ps.isEmpty() || !fs.isEmpty();
            Implementation row;
            if (form != null) {
                if (refusal != null) {
                    conflicts.add(id + ": form " + form + " and a refusal (" + refusal.reason() + ")");
                }
                row = new Implementation.Form(form, ps, fs);
            } else if (refusal != null) {
                if (implemented) {
                    conflicts.add(id + ": refused (" + refusal.reason() + ") and implemented " + ps + " " + fs);
                }
                row = refusal;
            } else if (implemented) {
                row = new Implementation.Intrinsic(ps, fo, fs);
            } else if (declarations.get(id) instanceof FunctionDefinition) {
                row = new Implementation.Body();
            } else {
                row = new Implementation.Unimplemented();
            }
            rows.put(id, row);
        }
        return new ImplementationTable(rows, dangling, conflicts, memberWalls);
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
    public @com.legend.base.Nullable Implementation of(FunctionId id) {
        return rows.get(id);
    }

    /** The row for {@code declaration}, or — for one outside this table — the
     *  default of its kind: {@code Body} for a body, {@code Unimplemented} for a
     *  native. A lowering with no model behind it meets such declarations. */
    public Implementation rowOf(Function declaration) {
        Implementation row = of(FunctionId.of(declaration));
        if (row != null) {
            return row;
        }
        return declaration instanceof FunctionDefinition ? new Implementation.Body()
                : new Implementation.Unimplemented();
    }

    /** Whether the platform runs {@code declaration} by its own rule or form
     *  (never by the declaration's body); false for null or an undeclared one. */
    public boolean runsByRule(com.legend.model.@com.legend.base.Nullable Function declaration) {
        return declaration != null && Implementation.byRule(of(FunctionId.of(declaration)));
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
