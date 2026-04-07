#!/usr/bin/env python3
"""Add import statements to test Pure model text blocks."""
import re, glob, sys

for f in sorted(glob.glob('engine/src/test/java/com/gs/legend/test/*.java')):
    with open(f) as fh:
        content = fh.read()

    def add_imports(match):
        block = match.group(0)
        inner = match.group(1)

        # Find all packages used in element definitions
        pkgs = set()
        for m in re.finditer(
            r'(?:Class|Database|Mapping|Association|Enum|Service|Runtime|Function)'
            r'\s+(\w+(?:::\w+)*)::', inner):
            pkgs.add(m.group(1))

        if not pkgs:
            return block

        # Check which packages are already imported in this block
        existing = set()
        for m in re.finditer(r'import\s+(\S+)::\*;', inner):
            existing.add(m.group(1))

        new_pkgs = pkgs - existing
        if not new_pkgs:
            return block

        # Find indentation of the first non-empty line
        first = re.search(r'^([ \t]*)\S', inner, re.MULTILINE)
        indent = first.group(1) if first else ''

        import_block = '\n'.join(f'{indent}import {p}::*;' for p in sorted(new_pkgs))
        return '"""\n' + import_block + '\n' + inner + '"""'

    new_content = re.sub(r'"""(.*?)"""', add_imports, content, flags=re.DOTALL)

    if new_content != content:
        with open(f, 'w') as fh:
            fh.write(new_content)
        old_count = len(re.findall(r'import \w+(?:::\w+)*::\*;', content))
        new_count = len(re.findall(r'import \w+(?:::\w+)*::\*;', new_content))
        print(f'{f}: +{new_count - old_count} imports')

print('Done.')
