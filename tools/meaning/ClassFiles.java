// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.tools.meaning;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * A CLASS FILE, READ (census B2, 2026-11-19): its constant pool, its supertypes, its fields and methods, each method's
 * code as decoded instructions with their lines, and its bootstrap methods (a lambda's implementation). JDK-only, no
 * bytecode library (core's ConstantPools reads the pool alone; the forms lens needs the code): the JVM class-file
 * format of JVMS 4, read for what the meaning census asks -- never written.
 */
public final class ClassFiles {

    private ClassFiles() {
    }

    /** A constant pool entry: its tag and its raw fields (indices, or the value it holds). */
    public record Constant(int tag, int a, int b, Object value) {
    }

    /** A member reference: its owner (internal name, {@code a/b/C}), its name and its descriptor. */
    public record Ref(String owner, String name, String descriptor) {

        /** {@code a.b.C#name(descriptor)} */
        public String key() {
            return owner.replace('/', '.') + "#" + name + descriptor;
        }
    }

    /** One instruction: its offset, its opcode, its first operand (a pool index, a local, a constant), its second
     *  (iinc's constant, invokeinterface's count, a branch's target), and its switch targets. */
    public record Insn(int pc, int op, int operand, int operand2, int[] targets) {
    }

    /** A method: access flags, name, descriptor, its code (empty for none), its lines by offset, its exception
     *  handlers' starts, its locals' size. */
    public record Method(int access, String name, String descriptor, List<Insn> code, TreeMap<Integer, Integer> lines,
            List<Integer> handlers, int maxLocals) {

        public boolean isStatic() {
            return (access & 0x0008) != 0;
        }

        public boolean isAbstract() {
            return (access & 0x0400) != 0 || code.isEmpty();
        }

        public boolean isBridge() {
            return (access & 0x0040) != 0;
        }

        public boolean isSynthetic() {
            return (access & 0x1000) != 0;
        }

        /** The line of the instruction at {@code pc}, or 0. */
        public int lineAt(int pc) {
            Map.Entry<Integer, Integer> e = lines.floorEntry(pc);
            return e == null ? 0 : e.getValue();
        }
    }

    /** A field: access flags, name, descriptor. */
    public record Field(int access, String name, String descriptor) {
    }

    /** A bootstrap method: its handle's reference and its arguments' pool indices. */
    public record Bootstrap(Ref handle, int handleKind, List<Integer> args) {
    }

    /** A class: its internal name, access, super, interfaces, source file, pool, methods and bootstrap methods. */
    public record ClassFile(String name, int access, String superName, List<String> interfaces, String sourceFile,
            Constant[] pool, List<Field> fields, List<Method> methods, List<Bootstrap> bootstraps) {

        public boolean isInterface() {
            return (access & 0x0200) != 0;
        }

        /** The UTF-8 text at pool index {@code i}. */
        public String utf8(int i) {
            return (String) pool[i].value();
        }

        /** The internal class name a CONSTANT_Class at {@code i} names. */
        public String className(int i) {
            return utf8(pool[i].a());
        }

        /** The member a Fieldref, Methodref or InterfaceMethodref at {@code i} names. */
        public Ref ref(int i) {
            Constant c = pool[i];
            Constant nat = pool[c.b()];
            return new Ref(className(c.a()), utf8(nat.a()), utf8(nat.b()));
        }

        /** An invokedynamic's NameAndType at pool index {@code i}: its name and descriptor, and its bootstrap. */
        public Ref indy(int i) {
            Constant c = pool[i];
            Constant nat = pool[c.b()];
            return new Ref("#" + c.a(), utf8(nat.a()), utf8(nat.b()));
        }

        /** The string constants of the pool (each CONSTANT_String's text). */
        public List<String> strings() {
            List<String> out = new ArrayList<>();
            for (Constant c : pool) {
                if (c != null && c.tag() == 8) {
                    out.add(utf8(c.a()));
                }
            }
            return out;
        }

        /** Every UTF-8 entry of the pool (names, descriptors and string constants alike). */
        public List<String> utf8s() {
            List<String> out = new ArrayList<>();
            for (Constant c : pool) {
                if (c != null && c.tag() == 1) {
                    out.add((String) c.value());
                }
            }
            return out;
        }

        /** The binary name: {@code a.b.C$D}. */
        public String binaryName() {
            return name.replace('/', '.');
        }
    }

    /** Reads one class file. */
    public static ClassFile read(byte[] bytes) {
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes))) {
            if (in.readInt() != 0xCAFEBABE) {
                throw new IllegalArgumentException("not a class file");
            }
            in.readUnsignedShort();
            in.readUnsignedShort();
            int count = in.readUnsignedShort();
            Constant[] pool = new Constant[count];
            for (int i = 1; i < count; i++) {
                int tag = in.readUnsignedByte();
                switch (tag) {
                    case 1 -> pool[i] = new Constant(tag, 0, 0, in.readUTF());
                    case 3 -> pool[i] = new Constant(tag, 0, 0, in.readInt());
                    case 4 -> pool[i] = new Constant(tag, 0, 0, in.readFloat());
                    case 5 -> {
                        pool[i] = new Constant(tag, 0, 0, in.readLong());
                        i++;
                    }
                    case 6 -> {
                        pool[i] = new Constant(tag, 0, 0, in.readDouble());
                        i++;
                    }
                    case 7, 8, 16, 19, 20 -> pool[i] = new Constant(tag, in.readUnsignedShort(), 0, null);
                    case 9, 10, 11, 12, 17, 18 -> pool[i] = new Constant(tag, in.readUnsignedShort(),
                            in.readUnsignedShort(), null);
                    case 15 -> pool[i] = new Constant(tag, in.readUnsignedByte(), in.readUnsignedShort(), null);
                    default -> throw new IllegalArgumentException("constant pool tag " + tag + " at " + i);
                }
            }
            int access = in.readUnsignedShort();
            int thisClass = in.readUnsignedShort();
            int superClass = in.readUnsignedShort();
            String name = utf8(pool, pool[thisClass].a());
            String superName = superClass == 0 ? null : utf8(pool, pool[superClass].a());
            int ifaces = in.readUnsignedShort();
            List<String> interfaces = new ArrayList<>();
            for (int k = 0; k < ifaces; k++) {
                interfaces.add(utf8(pool, pool[in.readUnsignedShort()].a()));
            }
            int fieldCount = in.readUnsignedShort();
            List<Field> fields = new ArrayList<>();
            for (int k = 0; k < fieldCount; k++) {
                int facc = in.readUnsignedShort();
                String fname = utf8(pool, in.readUnsignedShort());
                String fdesc = utf8(pool, in.readUnsignedShort());
                fields.add(new Field(facc, fname, fdesc));
                skipAttributes(in);
            }
            int methodCount = in.readUnsignedShort();
            List<Method> methods = new ArrayList<>();
            for (int k = 0; k < methodCount; k++) {
                int macc = in.readUnsignedShort();
                String mname = utf8(pool, in.readUnsignedShort());
                String mdesc = utf8(pool, in.readUnsignedShort());
                int attrs = in.readUnsignedShort();
                List<Insn> code = List.of();
                TreeMap<Integer, Integer> lines = new TreeMap<>();
                List<Integer> handlers = new ArrayList<>();
                int maxLocals = 0;
                for (int a = 0; a < attrs; a++) {
                    String an = utf8(pool, in.readUnsignedShort());
                    int len = in.readInt();
                    if (an.equals("Code")) {
                        in.readUnsignedShort();
                        maxLocals = in.readUnsignedShort();
                        byte[] bytesOfCode = new byte[in.readInt()];
                        in.readFully(bytesOfCode);
                        code = decode(bytesOfCode);
                        int ex = in.readUnsignedShort();
                        for (int e = 0; e < ex; e++) {
                            in.readUnsignedShort();
                            in.readUnsignedShort();
                            handlers.add(in.readUnsignedShort());
                            in.readUnsignedShort();
                        }
                        int cattrs = in.readUnsignedShort();
                        for (int c = 0; c < cattrs; c++) {
                            String cn = utf8(pool, in.readUnsignedShort());
                            int clen = in.readInt();
                            if (cn.equals("LineNumberTable")) {
                                int n = in.readUnsignedShort();
                                for (int l = 0; l < n; l++) {
                                    lines.put(in.readUnsignedShort(), in.readUnsignedShort());
                                }
                            } else {
                                in.skipNBytes(clen);
                            }
                        }
                    } else {
                        in.skipNBytes(len);
                    }
                }
                methods.add(new Method(macc, mname, mdesc, code, lines, handlers, maxLocals));
            }
            String sourceFile = null;
            List<Bootstrap> bootstraps = new ArrayList<>();
            int cattrs = in.readUnsignedShort();
            for (int a = 0; a < cattrs; a++) {
                String an = utf8(pool, in.readUnsignedShort());
                int len = in.readInt();
                if (an.equals("SourceFile")) {
                    sourceFile = utf8(pool, in.readUnsignedShort());
                } else if (an.equals("BootstrapMethods")) {
                    int n = in.readUnsignedShort();
                    for (int b = 0; b < n; b++) {
                        int handle = in.readUnsignedShort();
                        int argc = in.readUnsignedShort();
                        List<Integer> args = new ArrayList<>();
                        for (int x = 0; x < argc; x++) {
                            args.add(in.readUnsignedShort());
                        }
                        Constant h = pool[handle];
                        Constant target = pool[h.b()];
                        Constant nat = pool[target.b()];
                        Ref ref = new Ref(utf8(pool, pool[target.a()].a()), utf8(pool, nat.a()), utf8(pool, nat.b()));
                        bootstraps.add(new Bootstrap(ref, h.a(), args));
                    }
                } else {
                    in.skipNBytes(len);
                }
            }
            return new ClassFile(name, access, superName, interfaces, sourceFile, pool, fields, methods, bootstraps);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String utf8(Constant[] pool, int i) {
        return (String) pool[i].value();
    }

    private static void skipAttributes(DataInputStream in) throws IOException {
        int n = in.readUnsignedShort();
        for (int a = 0; a < n; a++) {
            in.readUnsignedShort();
            in.skipNBytes(in.readInt());
        }
    }

    /** Every class of {@code jars} (a multi-release jar's versioned classes left out: the base one is read). */
    public static Map<String, ClassFile> ofJars(List<Path> jars) {
        Map<String, ClassFile> out = new LinkedHashMap<>();
        for (Path jar : jars) {
            try (JarFile f = new JarFile(jar.toFile())) {
                Enumeration<JarEntry> entries = f.entries();
                while (entries.hasMoreElements()) {
                    JarEntry e = entries.nextElement();
                    String n = e.getName();
                    if (!n.endsWith(".class") || n.endsWith("module-info.class") || n.startsWith("META-INF/")) {
                        continue;
                    }
                    try (var in = f.getInputStream(e)) {
                        ClassFile c = read(in.readAllBytes());
                        out.putIfAbsent(c.name(), c);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
    }

    /** Every resource of {@code jars} that is no class file, directory or manifest, by "jar!/path", as UTF-8 text (a
     *  resource that is no text left out). */
    public static Map<String, String> resourcesOfJars(List<Path> jars) {
        Map<String, String> out = new TreeMap<>();
        for (Path jar : jars) {
            try (JarFile f = new JarFile(jar.toFile())) {
                Enumeration<JarEntry> entries = f.entries();
                while (entries.hasMoreElements()) {
                    JarEntry e = entries.nextElement();
                    String n = e.getName();
                    if (e.isDirectory() || n.endsWith(".class") || n.startsWith("META-INF/MANIFEST")) {
                        continue;
                    }
                    try (var in = f.getInputStream(e)) {
                        byte[] b = in.readAllBytes();
                        try {
                            out.put(jar.getFileName() + "!/" + n, java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                                    .decode(java.nio.ByteBuffer.wrap(b)).toString());
                        } catch (java.nio.charset.CharacterCodingException notText) {
                            // a binary resource: no text to read
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return out;
    }

    // ---------------------------------------------------------------------------------------------------------------
    // The instructions (JVMS 6.5)

    /** The opcode's operand bytes, for the fixed-length ones; -1 for the variable (switches, wide). */
    private static final int[] OPERANDS = new int[256];

    static {
        java.util.Arrays.fill(OPERANDS, 0);
        for (int op : new int[] {16, 18, 21, 22, 23, 24, 25, 54, 55, 56, 57, 58, 169, 188}) {
            OPERANDS[op] = 1;
        }
        for (int op : new int[] {17, 19, 20, 132, 178, 179, 180, 181, 182, 183, 184, 187, 189, 192, 193}) {
            OPERANDS[op] = 2;
        }
        for (int op = 153; op <= 168; op++) {
            OPERANDS[op] = 2;
        }
        OPERANDS[198] = 2;
        OPERANDS[199] = 2;
        OPERANDS[197] = 3;
        OPERANDS[185] = 4;
        OPERANDS[186] = 4;
        OPERANDS[200] = 4;
        OPERANDS[201] = 4;
        OPERANDS[170] = -1;
        OPERANDS[171] = -1;
        OPERANDS[196] = -1;
    }

    static List<Insn> decode(byte[] code) {
        List<Insn> out = new ArrayList<>();
        int pc = 0;
        while (pc < code.length) {
            int op = code[pc] & 0xff;
            int at = pc;
            switch (op) {
                case 170 -> {
                    int p = (pc + 4) & ~3;
                    int def = s4(code, p);
                    int low = s4(code, p + 4);
                    int high = s4(code, p + 8);
                    int[] t = new int[high - low + 2];
                    t[0] = at + def;
                    for (int k = 0; k <= high - low; k++) {
                        t[k + 1] = at + s4(code, p + 12 + 4 * k);
                    }
                    out.add(new Insn(at, op, 0, 0, t));
                    pc = p + 12 + 4 * (high - low + 1);
                }
                case 171 -> {
                    int p = (pc + 4) & ~3;
                    int def = s4(code, p);
                    int n = s4(code, p + 4);
                    int[] t = new int[n + 1];
                    t[0] = at + def;
                    for (int k = 0; k < n; k++) {
                        t[k + 1] = at + s4(code, p + 8 + 8 * k + 4);
                    }
                    out.add(new Insn(at, op, 0, 0, t));
                    pc = p + 8 + 8 * n;
                }
                case 196 -> {
                    int inner = code[pc + 1] & 0xff;
                    int idx = u2(code, pc + 2);
                    if (inner == 132) {
                        out.add(new Insn(at, inner, idx, (short) u2(code, pc + 4), null));
                        pc += 6;
                    } else {
                        out.add(new Insn(at, inner, idx, 0, null));
                        pc += 4;
                    }
                }
                default -> {
                    int len = OPERANDS[op];
                    int a = 0;
                    int b = 0;
                    if (len == 1) {
                        a = op == 16 ? code[pc + 1] : code[pc + 1] & 0xff;
                    } else if (len == 2) {
                        if ((op >= 153 && op <= 168) || op == 198 || op == 199) {
                            b = at + (short) u2(code, pc + 1);
                        } else if (op == 132) {
                            a = code[pc + 1] & 0xff;
                            b = code[pc + 2];
                        } else if (op == 17) {
                            a = (short) u2(code, pc + 1);
                        } else {
                            a = u2(code, pc + 1);
                        }
                    } else if (len == 3) {
                        a = u2(code, pc + 1);
                        b = code[pc + 3] & 0xff;
                    } else if (len == 4) {
                        if (op == 200 || op == 201) {
                            b = at + s4(code, pc + 1);
                        } else {
                            a = u2(code, pc + 1);
                            b = code[pc + 3] & 0xff;
                        }
                    }
                    out.add(new Insn(at, op, a, b, null));
                    pc += 1 + len;
                }
            }
        }
        return out;
    }

    private static int u2(byte[] b, int i) {
        return ((b[i] & 0xff) << 8) | (b[i + 1] & 0xff);
    }

    private static int s4(byte[] b, int i) {
        return ((b[i] & 0xff) << 24) | ((b[i + 1] & 0xff) << 16) | ((b[i + 2] & 0xff) << 8) | (b[i + 3] & 0xff);
    }

    // ---------------------------------------------------------------------------------------------------------------
    // Descriptors

    /** The parameter types of a method descriptor, each as its descriptor ({@code I}, {@code Ljava/lang/String;}). */
    public static List<String> params(String descriptor) {
        List<String> out = new ArrayList<>();
        int i = 1;
        while (descriptor.charAt(i) != ')') {
            int start = i;
            while (descriptor.charAt(i) == '[') {
                i++;
            }
            if (descriptor.charAt(i) == 'L') {
                i = descriptor.indexOf(';', i) + 1;
            } else {
                i++;
            }
            out.add(descriptor.substring(start, i));
        }
        return out;
    }

    /** A method descriptor's return type. */
    public static String returns(String descriptor) {
        return descriptor.substring(descriptor.indexOf(')') + 1);
    }

    /** A type's size on the stack and in the locals: 2 for long and double, 0 for void, else 1. */
    public static int size(String type) {
        return type.equals("J") || type.equals("D") ? 2 : type.equals("V") ? 0 : 1;
    }

    /** The local slot of each parameter of {@code m} (its receiver at 0 when it has one). */
    public static int[] paramSlots(Method m) {
        List<String> ps = params(m.descriptor());
        int[] out = new int[ps.size()];
        int slot = m.isStatic() ? 0 : 1;
        for (int k = 0; k < ps.size(); k++) {
            out[k] = slot;
            slot += size(ps.get(k));
        }
        return out;
    }
}
