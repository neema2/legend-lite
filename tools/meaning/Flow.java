// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import com.legend.tools.meaning.ClassFiles.ClassFile;
import com.legend.tools.meaning.ClassFiles.Constant;
import com.legend.tools.meaning.ClassFiles.Insn;
import com.legend.tools.meaning.ClassFiles.Method;
import com.legend.tools.meaning.ClassFiles.Ref;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/**
 * ONE METHOD'S DATA FLOW, APPROXIMATED (census B2, 2026-11-19): its instructions run over an abstract operand stack
 * whose values are either a constant (null, a number, a literal) or live, and either TAINTED -- computed from a
 * seeded answer (a method the caller names, or a parameter it marks) -- or not. It records which locals are loaded,
 * every call with each argument's value, every conditional branch that tests a tainted value, every tainted value
 * handed to a call, and whether a tainted value is returned.
 *
 * <p>Approximate by design, and conservative where it matters: straight-line code is exact; at a branch target or an
 * exception handler the stack is unknown, and a value popped from an unknown stack is live and untainted (a ternary's
 * merged value loses its taint). A local is tainted once any tainted value is stored to it (flow-insensitive, the
 * method run again until no local changes).
 */
public final class Flow {

    /** An abstract stack value: its size (1, or 2 for long and double), its constant's text (null when live), and
     *  whether it is tainted. */
    public record V(int size, String constant, boolean taint) {

        static final V LIVE = new V(1, null, false);

        boolean isAbsent() {
            return "null".equals(constant) || "0".equals(constant) || "false".equals(constant);
        }
    }

    /** A call: where it is, how it was invoked, what it calls, its receiver (null for a static or dynamic call) and its
     *  arguments' values in order. */
    public record Call(int pc, int line, int op, Ref target, V receiver, List<V> args) {
    }

    /** What one run of a method found. */
    public record Result(Set<Integer> loadedSlots, List<Call> calls, List<Integer> taintedBranchLines,
            List<Call> taintedCalls, boolean returnsTaint, List<Integer> taintedReturnLines) {
    }

    private Flow() {
    }

    /** Runs {@code m} of {@code c}: {@code seeded} names the calls whose result is tainted, {@code taintedParams} the
     *  parameters (by position) that are. */
    public static Result run(ClassFile c, Method m, Predicate<Ref> seeded, Set<Integer> taintedParams) {
        int[] slots = ClassFiles.paramSlots(m);
        Set<Integer> taintedLocals = new HashSet<>();
        for (int p : taintedParams) {
            if (p < slots.length) {
                taintedLocals.add(slots[p]);
            }
        }
        Result r = null;
        for (int pass = 0; pass < 4; pass++) {
            int before = taintedLocals.size();
            r = once(c, m, seeded, taintedLocals);
            if (taintedLocals.size() == before) {
                break;
            }
        }
        return r;
    }

    private static Result once(ClassFile c, Method m, Predicate<Ref> seeded, Set<Integer> taintedLocals) {
        Set<Integer> targets = new HashSet<>();
        for (Insn i : m.code()) {
            if ((i.op() >= 153 && i.op() <= 168) || i.op() == 198 || i.op() == 199 || i.op() == 200 || i.op() == 201) {
                targets.add(i.operand2());
            }
            if (i.targets() != null) {
                for (int t : i.targets()) {
                    targets.add(t);
                }
            }
        }
        Set<Integer> handlers = new HashSet<>(m.handlers());
        Set<Integer> loaded = new HashSet<>();
        List<Call> calls = new ArrayList<>();
        List<Integer> branches = new ArrayList<>();
        List<Call> taintedCalls = new ArrayList<>();
        List<Integer> taintedReturns = new ArrayList<>();
        boolean returnsTaint = false;
        Stack s = new Stack();
        for (Insn i : m.code()) {
            if (handlers.contains(i.pc())) {
                s.reset();
                s.push(V.LIVE);
            } else if (targets.contains(i.pc())) {
                s.reset();
            }
            int op = i.op();
            int line = m.lineAt(i.pc());
            switch (op) {
                case 0 -> { }
                case 1 -> s.push(new V(1, "null", false));
                case 2, 3, 4, 5, 6, 7, 8 -> s.push(new V(1, String.valueOf(op - 3), false));
                case 9, 10 -> s.push(new V(2, String.valueOf(op - 9), false));
                case 11, 12, 13 -> s.push(new V(1, (op - 11) + ".0", false));
                case 14, 15 -> s.push(new V(2, (op - 14) + ".0", false));
                case 16, 17 -> s.push(new V(1, String.valueOf(i.operand()), false));
                case 18, 19 -> s.push(new V(1, ldc(c, i.operand()), false));
                case 20 -> s.push(new V(2, ldc(c, i.operand()), false));
                case 21, 22, 23, 24, 25 -> load(s, loaded, taintedLocals, i.operand(), op == 22 || op == 24 ? 2 : 1);
                case 26, 27, 28, 29 -> load(s, loaded, taintedLocals, op - 26, 1);
                case 30, 31, 32, 33 -> load(s, loaded, taintedLocals, op - 30, 2);
                case 34, 35, 36, 37 -> load(s, loaded, taintedLocals, op - 34, 1);
                case 38, 39, 40, 41 -> load(s, loaded, taintedLocals, op - 38, 2);
                case 42, 43, 44, 45 -> load(s, loaded, taintedLocals, op - 42, 1);
                case 46, 47, 48, 49, 50, 51, 52, 53 -> {
                    V idx = s.pop();
                    V arr = s.pop();
                    s.push(new V(op == 47 || op == 49 ? 2 : 1, null, idx.taint() || arr.taint()));
                }
                case 54, 55, 56, 57, 58 -> store(s, taintedLocals, i.operand());
                case 59, 60, 61, 62 -> store(s, taintedLocals, op - 59);
                case 63, 64, 65, 66 -> store(s, taintedLocals, op - 63);
                case 67, 68, 69, 70 -> store(s, taintedLocals, op - 67);
                case 71, 72, 73, 74 -> store(s, taintedLocals, op - 71);
                case 75, 76, 77, 78 -> store(s, taintedLocals, op - 75);
                case 79, 80, 81, 82, 83, 84, 85, 86 -> {
                    s.pop();
                    s.pop();
                    s.pop();
                }
                case 87 -> s.pop();
                case 88 -> {
                    V v = s.pop();
                    if (v.size() == 1) {
                        s.pop();
                    }
                }
                case 89 -> {
                    V v = s.pop();
                    s.push(v);
                    s.push(v);
                }
                case 90 -> {
                    V v1 = s.pop();
                    V v2 = s.pop();
                    s.push(v1);
                    s.push(v2);
                    s.push(v1);
                }
                case 91 -> {
                    V v1 = s.pop();
                    V v2 = s.pop();
                    if (v2.size() == 2) {
                        s.push(v1);
                        s.push(v2);
                        s.push(v1);
                    } else {
                        V v3 = s.pop();
                        s.push(v1);
                        s.push(v3);
                        s.push(v2);
                        s.push(v1);
                    }
                }
                case 92 -> {
                    V v1 = s.pop();
                    if (v1.size() == 2) {
                        s.push(v1);
                        s.push(v1);
                    } else {
                        V v2 = s.pop();
                        s.push(v2);
                        s.push(v1);
                        s.push(v2);
                        s.push(v1);
                    }
                }
                case 93 -> {
                    V v1 = s.pop();
                    if (v1.size() == 2) {
                        V v2 = s.pop();
                        s.push(v1);
                        s.push(v2);
                        s.push(v1);
                    } else {
                        V v2 = s.pop();
                        V v3 = s.pop();
                        s.push(v2);
                        s.push(v1);
                        s.push(v3);
                        s.push(v2);
                        s.push(v1);
                    }
                }
                case 94 -> {
                    V v1 = s.pop();
                    if (v1.size() == 2) {
                        V v2 = s.pop();
                        if (v2.size() == 2) {
                            s.push(v1);
                            s.push(v2);
                            s.push(v1);
                        } else {
                            V v3 = s.pop();
                            s.push(v1);
                            s.push(v3);
                            s.push(v2);
                            s.push(v1);
                        }
                    } else {
                        V v2 = s.pop();
                        V v3 = s.pop();
                        if (v3.size() == 2) {
                            s.push(v2);
                            s.push(v1);
                            s.push(v3);
                            s.push(v2);
                            s.push(v1);
                        } else {
                            V v4 = s.pop();
                            s.push(v2);
                            s.push(v1);
                            s.push(v4);
                            s.push(v3);
                            s.push(v2);
                            s.push(v1);
                        }
                    }
                }
                case 95 -> {
                    V v1 = s.pop();
                    V v2 = s.pop();
                    s.push(v1);
                    s.push(v2);
                }
                default -> {
                    if (op >= 96 && op <= 115) {
                        binary(s, (op - 96) % 4 == 1 || (op - 96) % 4 == 3 ? 2 : 1);
                    } else if (op >= 116 && op <= 119) {
                        V v = s.pop();
                        s.push(new V(op == 117 || op == 119 ? 2 : 1, null, v.taint()));
                    } else if (op >= 120 && op <= 131) {
                        binary(s, op % 2 == 1 ? 2 : 1);
                    } else if (op == 132) {
                        loaded.add(i.operand());
                    } else if (op >= 133 && op <= 147) {
                        V v = s.pop();
                        int size = switch (op) {
                            case 133, 135, 138, 140, 141, 143 -> 2;
                            default -> 1;
                        };
                        s.push(new V(size, null, v.taint()));
                    } else if (op >= 148 && op <= 152) {
                        binary(s, 1);
                    } else if ((op >= 153 && op <= 158) || op == 198 || op == 199) {
                        if (s.pop().taint()) {
                            branches.add(line);
                        }
                    } else if (op >= 159 && op <= 166) {
                        V a = s.pop();
                        V b = s.pop();
                        if (a.taint() || b.taint()) {
                            branches.add(line);
                        }
                    } else if (op == 167 || op == 200) {
                        s.reset();
                    } else if (op == 168 || op == 201) {
                        s.push(V.LIVE);
                    } else if (op == 169) {
                        loaded.add(i.operand());
                    } else if (op == 170 || op == 171) {
                        if (s.pop().taint()) {
                            branches.add(line);
                        }
                        s.reset();
                    } else if (op >= 172 && op <= 176) {
                        if (s.pop().taint()) {
                            returnsTaint = true;
                            taintedReturns.add(line);
                        }
                        s.reset();
                    } else if (op == 177) {
                        s.reset();
                    } else if (op == 178) {
                        Ref f = c.ref(i.operand());
                        s.push(new V(ClassFiles.size(f.descriptor()), "static " + f.owner() + "." + f.name(), false));
                    } else if (op == 179) {
                        s.pop();
                    } else if (op == 180) {
                        Ref f = c.ref(i.operand());
                        V obj = s.pop();
                        s.push(new V(ClassFiles.size(f.descriptor()), null, obj.taint()));
                    } else if (op == 181) {
                        s.pop();
                        s.pop();
                    } else if (op >= 182 && op <= 186) {
                        Ref target = op == 186 ? c.indy(i.operand()) : c.ref(i.operand());
                        List<String> ps = ClassFiles.params(target.descriptor());
                        List<V> args = new ArrayList<>();
                        for (int k = ps.size() - 1; k >= 0; k--) {
                            args.add(0, s.pop());
                        }
                        V receiver = op == 184 || op == 186 ? null : s.pop();
                        Call call = new Call(i.pc(), line, op, target, receiver, List.copyOf(args));
                        calls.add(call);
                        // a tainted receiver taints the result (frame.equals(x), frame.isEmpty()); a tainted argument
                        // only through java.lang's value functions (String.valueOf, Objects.equals): a collection or a
                        // builder handed a tainted value is no decision, and a product method's parameter is followed
                        // by the caller (its parameters' taint, not its return)
                        boolean valueFunction = target.owner().startsWith("java/lang/")
                                || target.owner().equals("java/util/Objects");
                        boolean inTaint = (receiver != null && receiver.taint())
                                || (valueFunction && args.stream().anyMatch(V::taint));
                        if (args.stream().anyMatch(V::taint)) {
                            taintedCalls.add(call);
                        }
                        String ret = ClassFiles.returns(target.descriptor());
                        if (!ret.equals("V")) {
                            boolean seed = op != 186 && seeded.test(target);
                            s.push(new V(ClassFiles.size(ret), null, inTaint || seed));
                        }
                    } else if (op == 187) {
                        s.push(V.LIVE);
                    } else if (op == 188 || op == 189 || op == 190 || op == 193) {
                        V v = s.pop();
                        s.push(new V(1, null, v.taint()));
                    } else if (op == 191) {
                        s.pop();
                        s.reset();
                    } else if (op == 192) {
                        V v = s.pop();
                        s.push(v);
                    } else if (op == 194 || op == 195) {
                        s.pop();
                    } else if (op == 197) {
                        boolean t = false;
                        for (int k = 0; k < i.operand2(); k++) {
                            t |= s.pop().taint();
                        }
                        s.push(new V(1, null, t));
                    } else {
                        throw new IllegalStateException("opcode " + op + " at " + c.name() + "." + m.name() + ":" + i.pc());
                    }
                }
            }
        }
        return new Result(loaded, calls, branches, taintedCalls, returnsTaint, taintedReturns);
    }

    private static void load(Stack s, Set<Integer> loaded, Set<Integer> tainted, int slot, int size) {
        loaded.add(slot);
        s.push(new V(size, null, tainted.contains(slot)));
    }

    private static void store(Stack s, Set<Integer> tainted, int slot) {
        if (s.pop().taint()) {
            tainted.add(slot);
        }
    }

    private static void binary(Stack s, int size) {
        V a = s.pop();
        V b = s.pop();
        s.push(new V(size, null, a.taint() || b.taint()));
    }

    private static String ldc(ClassFile c, int i) {
        Constant k = c.pool()[i];
        return switch (k.tag()) {
            case 8 -> "\"" + c.utf8(k.a()) + "\"";
            case 3, 4, 5, 6 -> String.valueOf(k.value());
            case 7 -> "class " + c.className(i);
            default -> "constant#" + k.tag();
        };
    }

    /** The abstract operand stack; popped empty, it answers a live value (after a merge the stack is unknown). */
    private static final class Stack {

        private final ArrayList<V> values = new ArrayList<>();

        void push(V v) {
            values.add(v);
        }

        V pop() {
            return values.isEmpty() ? V.LIVE : values.remove(values.size() - 1);
        }

        void reset() {
            values.clear();
        }
    }
}
