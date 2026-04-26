#!/usr/bin/env python3
"""
One-shot generator: reads BuiltinRegistry.java, extracts every
reg.registerSignature("name", "<pure signature>"), derives the mangled
Pure.X constant name per the locked naming scheme, and emits Pure.java.

Naming scheme (locked):
  <NAME>__<ARG1TYPE>_<ARG1MULT>__<ARG2TYPE>_<ARG2MULT>__...
  - Pure name: camelCase -> UPPER_SNAKE.
  - Arg type: simple-name only (qualified prefixes like
    meta::pure::metamodel::type:: stripped); generic args inside <...>
    dropped; leading underscore on internal types like _Window dropped;
    camelCase split to UPPER_SNAKE.
  - Multiplicity: [1]->1, [N]->N, [*]->MANY, [N..M]->N_M with MANY
    substituting for *; e.g. [0..1]->0_1, [1..*]->1_MANY.
  - Return type dropped (Pure overloads on args only).
  - Arg separator __ (double underscore); single _ within an arg block
    splits camelCase words.

On collision (extremely rare in practice — overloads usually differ in
outer type sequence already), append _v2, _v3, ... in encounter order.
"""

import re
import sys
from pathlib import Path

REGISTRY = Path("engine/src/main/java/com/gs/legend/compiler/BuiltinRegistry.java")
OUT = Path("engine/src/main/java/com/gs/legend/compiler/Pure.java")


def extract_signatures(text: str) -> list[tuple[int, str, str]]:
    """Return list of (line_no, name, raw_signature) for each
    reg.registerSignature("name", "<sig>") call in BuiltinRegistry.java.
    Handles multi-line calls."""
    # Normalize: collapse lines so multi-line registerSignature(...) becomes one logical line.
    # Strategy: find "reg.registerSignature(" anchors, then read until matching ");"
    out = []
    lines = text.split("\n")
    i = 0
    while i < len(lines):
        line = lines[i]
        m = re.search(r'reg\.registerSignature\(\s*"([^"]+)"\s*,', line)
        if m:
            line_no = i + 1
            name = m.group(1)
            # Concatenate continuation lines until we find ");"
            buf = line[m.end():]
            j = i
            while ");" not in buf:
                j += 1
                if j >= len(lines):
                    raise RuntimeError(f"Unterminated registerSignature at line {line_no}")
                buf += lines[j]
            # Extract the signature string (Java string literal between " and ");")
            sm = re.search(r'"((?:[^"\\]|\\.)*)"\s*\)\s*;', buf)
            if not sm:
                raise RuntimeError(f"Could not parse signature at line {line_no}: {buf[:200]}")
            raw_sig = sm.group(1)
            out.append((line_no, name, raw_sig))
            i = j + 1
        else:
            i += 1
    return out


def parse_signature(sig: str) -> tuple[str, list[tuple[str, str]]]:
    """Parse a Pure native signature minimally:
       'native function NAME<...>(p1:Type1[mult1], p2:Type2[mult2], ...):RetType[retMult];'
       Returns (name, [(outer_type_simple_name, raw_multiplicity), ...]).
       Skips return type (we don't emit it)."""
    s = sig.strip()
    # Strip leading "native function "
    s = re.sub(r'^native\s+function\s+', '', s)
    # Strip trailing ;
    s = s.rstrip(';').rstrip()

    # Function name = chars up to "<" or "("
    m = re.match(r'([A-Za-z_][A-Za-z0-9_]*)', s)
    if not m:
        raise RuntimeError(f"Could not parse function name: {sig[:80]}")
    fname = m.group(1)
    rest = s[m.end():]

    # Skip generic type params <T,Z,K,V> (balanced angle brackets)
    if rest.startswith('<'):
        depth = 0
        for k, ch in enumerate(rest):
            if ch == '<':
                depth += 1
            elif ch == '>':
                if k > 0 and rest[k-1] == '-':
                    continue
                depth -= 1
                if depth == 0:
                    rest = rest[k+1:]
                    break

    # Now expect (params):returnType
    if not rest.startswith('('):
        raise RuntimeError(f"Expected ( after fn header: {rest[:80]}")
    # Find matching closing paren — balanced over () <> {}
    # Balance ( ) but skip nested <{[ depths so a ')' inside generics doesn't pop us.
    depth_paren = 0
    depth_other = 0
    end = -1
    for k, ch in enumerate(rest):
        if ch in '<{[':
            depth_other += 1
        elif ch in '>}]':
            if ch == '>' and k > 0 and rest[k-1] == '-':
                continue
            depth_other -= 1
        elif ch == '(' and depth_other == 0:
            depth_paren += 1
        elif ch == ')' and depth_other == 0:
            depth_paren -= 1
            if depth_paren == 0:
                end = k
                break
    if end < 0:
        raise RuntimeError(f"Unbalanced ( in: {rest[:80]}")
    params_str = rest[1:end]

    params = split_params(params_str)
    parsed = [parse_param(p) for p in params]
    return fname, parsed


def split_params(s: str) -> list[str]:
    """Split a parameter list on commas at depth 0. Track depth over
    <>{}() but treat '->' (lambda arrow) as a single token, not a depth
    decrement on '>'."""
    out = []
    depth = 0
    buf = []
    for k, ch in enumerate(s):
        if ch in '<{(':
            depth += 1
            buf.append(ch)
        elif ch in '>})':
            if ch == '>' and k > 0 and s[k-1] == '-':
                buf.append(ch)  # part of '->', don't pop depth
            else:
                depth -= 1
                buf.append(ch)
        elif ch == ',' and depth == 0:
            out.append(''.join(buf).strip())
            buf = []
        else:
            buf.append(ch)
    last = ''.join(buf).strip()
    if last:
        out.append(last)
    return out


def parse_param(p: str) -> tuple[str, str]:
    """Parse one param: 'name:Type<...>[mult]' or 'name:Type[mult]'.
       Returns (outer_simple_type_name_normalized, raw_mult_inside_brackets)."""
    # Split on first ':'
    if ':' not in p:
        raise RuntimeError(f"Param without ':' : {p}")
    _, _, type_and_mult = p.partition(':')
    type_and_mult = type_and_mult.strip()

    # Last [...] is the multiplicity. Track depth over ALL bracket kinds
    # (<>{}()[]) so internal [N] inside Function<{T[1]->...}> don't confuse us.
    # The LAST '[' opened at depth 0 is the multiplicity opener.
    depth = 0
    last_open = -1
    for k, ch in enumerate(type_and_mult):
        if ch in '<{([':
            if ch == '[' and depth == 0:
                last_open = k
            depth += 1
        elif ch in '>})]':
            if ch == '>' and k > 0 and type_and_mult[k-1] == '-':
                continue  # '->' arrow, not a closing bracket
            depth -= 1
    if last_open < 0:
        raise RuntimeError(f"No multiplicity in: {p}")
    type_part = type_and_mult[:last_open]
    mult_part = type_and_mult[last_open+1:]
    if not mult_part.endswith(']'):
        raise RuntimeError(f"Bad mult in: {p}")
    mult = mult_part[:-1]
    return (extract_outer_type_name(type_part), mult)


def extract_outer_type_name(type_str: str) -> str:
    """Get the simple outer type name from a Pure type expression.
       'meta::pure::metamodel::relation::Relation<T>' -> 'Relation'
       '_Window<T>' -> 'Window' (leading _ dropped)
       'C' -> 'C'  (type variable)
       'meta::pure::metamodel::type::Integer' -> 'Integer'
    """
    s = type_str.strip()
    # Strip generics
    if '<' in s:
        s = s[:s.index('<')]
    # Strip qualifier path (anything before final '::')
    if '::' in s:
        s = s.rsplit('::', 1)[1]
    # Drop leading underscore for internal types (_Window, _Range, _Traversal)
    if s.startswith('_'):
        s = s[1:]
    return s


# ----- Mangling helpers --------------------------------------------------

def camel_to_upper_snake(s: str) -> str:
    """Convert camelCase / PascalCase to UPPER_SNAKE_CASE.
       'rowNumber' -> 'ROW_NUMBER'; 'StrictDate' -> 'STRICT_DATE';
       'XMLName' -> 'XML_NAME' (only one underscore between consecutive caps + lower).
       Already-uppercase single chars (type vars 'T', 'Z') stay as-is."""
    if not s:
        return s
    # Insert _ between [a-z0-9] and [A-Z], and between [A-Z] and [A-Z][a-z]
    s1 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s2 = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', s1)
    return s2.upper()


def mangle_mult(m: str) -> str:
    """[1]->1, [N]->N, [*]->MANY, [0..1]->0_1, [1..*]->1_MANY, [N..M]->N_M."""
    m = m.strip()
    if m == '*':
        return 'MANY'
    if '..' in m:
        lo, hi = m.split('..', 1)
        lo = lo.strip()
        hi = hi.strip()
        hi_part = 'MANY' if hi == '*' else hi
        return f"{lo}_{hi_part}"
    return m


def mangle(name: str, params: list[tuple[str, str]]) -> str:
    pname = camel_to_upper_snake(name)
    if not params:
        return pname
    blocks = []
    for type_name, mult in params:
        t = camel_to_upper_snake(type_name)
        m = mangle_mult(mult)
        blocks.append(f"{t}_{m}")
    return pname + "__" + "__".join(blocks)


# ----- Main --------------------------------------------------------------

def main():
    text = REGISTRY.read_text()
    sigs = extract_signatures(text)
    print(f"Found {len(sigs)} registerSignature calls", file=sys.stderr)

    entries = []  # list of (mangled_name, registered_name, raw_signature)
    seen = {}     # mangled_name -> count
    for line_no, regname, raw_sig in sigs:
        try:
            fname, params = parse_signature(raw_sig)
        except Exception as e:
            print(f"  ERROR line {line_no} ({regname}): {e}", file=sys.stderr)
            continue
        if fname != regname:
            # Some entries register with a different name than the signature declares;
            # use the registered name as the canonical one (matches BuiltinRegistry behavior).
            print(f"  NOTE line {line_no}: registerSignature name='{regname}' but signature has '{fname}'", file=sys.stderr)
        base = mangle(regname, params)
        # Collision handling: append _v2, _v3 if needed.
        if base in seen:
            seen[base] += 1
            mangled = f"{base}_v{seen[base]}"
        else:
            seen[base] = 1
            mangled = base
        entries.append((mangled, regname, raw_sig))

    # Emit Pure.java
    lines = []
    lines.append("// AUTO-GENERATED by scripts/generate_pure_constants.py")
    lines.append("// from BuiltinRegistry.java's registerSignature(...) calls.")
    lines.append("// Each constant is the typed NativeFunctionDef for one Pure native overload.")
    lines.append("// Naming scheme: <NAME>__<ARG1TYPE>_<ARG1MULT>__<ARG2TYPE>_<ARG2MULT>__...")
    lines.append("// Multiplicity: [1]->1, [N]->N, [*]->MANY, [0..1]->0_1, [1..*]->1_MANY, [N..M]->N_M.")
    lines.append("// Return type omitted (Pure overloads on args only).")
    lines.append("//")
    lines.append("// DO NOT edit this file by hand. To add a native, add a registerSignature")
    lines.append("// call in BuiltinRegistry.java (or move to Pure.java once migrated) and")
    lines.append("// re-run the generator.")
    lines.append("package com.gs.legend.compiler;")
    lines.append("")
    lines.append("import java.util.ArrayList;")
    lines.append("import java.util.Collections;")
    lines.append("import java.util.List;")
    lines.append("")
    lines.append("/**")
    lines.append(" * Typed identifiers for every Pure native overload. The single source of truth")
    lines.append(" * for Pure-name strings in the system: every consumer (lowering, checker,")
    lines.append(" * binding tables) references natives by these constants, not by string lookups.")
    lines.append(" *")
    lines.append(" * <p>Each constant is a {@link NativeFunctionDef} produced by parsing the")
    lines.append(" * signature exactly once at class-load time. Constants are populated in")
    lines.append(" * declaration order; {@link #all()} returns the full list for")
    lines.append(" * {@link BuiltinRegistry} to ingest.")
    lines.append(" */")
    lines.append("public final class Pure {")
    lines.append("    private Pure() {}")
    lines.append("")
    lines.append("    /** Definitions in declaration order. Populated lazily by signature(). */")
    lines.append("    private static final List<NativeFunctionDef> ALL = new ArrayList<>();")
    lines.append("")
    lines.append("    /** Snapshot of every Pure native def, for {@link BuiltinRegistry} to ingest at boot. */")
    lines.append("    static List<NativeFunctionDef> all() {")
    lines.append("        return Collections.unmodifiableList(ALL);")
    lines.append("    }")
    lines.append("")
    lines.append("    private static NativeFunctionDef signature(String pureSignature) {")
    lines.append("        NativeFunctionDef def = BuiltinRegistry.parseAndNormalize(pureSignature);")
    lines.append("        ALL.add(def);")
    lines.append("        return def;")
    lines.append("    }")
    lines.append("")
    for mangled, regname, raw_sig in entries:
        # Java string literal: escape backslash, double-quote.
        esc = raw_sig.replace("\\", "\\\\").replace("\"", "\\\"")
        lines.append(f"    public static final NativeFunctionDef {mangled} = signature(\"{esc}\");")
    lines.append("}")
    lines.append("")
    OUT.write_text("\n".join(lines))
    print(f"Wrote {OUT} with {len(entries)} constants", file=sys.stderr)


if __name__ == "__main__":
    main()
