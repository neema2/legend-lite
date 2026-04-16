# legend_library rule: declares one output file per element,
# enabling per-element Bazel cache tracking via unused_inputs_list.

LegendElementsInfo = provider(
    doc = "Per-PackageableElement files for this project",
    fields = {"element_files": "list of individual element .json Files"},
)

def _legend_library_impl(ctx):
    srcs = ctx.files.srcs

    # Collect dep element files (individually declared)
    dep_element_files = []
    for d in ctx.attr.deps:
        dep_element_files.extend(d[LegendElementsInfo].element_files)

    # Declare one output file per element (names from module extension)
    output_files = []
    for fqn in ctx.attr.elements:
        filename = fqn.replace("::", "__") + ".json"
        output_files.append(ctx.actions.declare_file(
            ctx.label.name + "_elements/" + filename))

    unused = ctx.actions.declare_file(ctx.label.name + ".unused_inputs")

    # Write the compile script
    script = ctx.actions.declare_file(ctx.label.name + "_compile.sh")

    # Build the script content
    script_content = """#!/bin/bash
set -e
echo "=== LegendCompile: {label} ==="

# "Compile" each .pure source → extract content into element .json files
# (In real impl, this is the Java compiler. Here we simulate with file copies.)
OUT_DIR="{out_dir}"

# Parse each source file and create element JSON files
# Include a hash of the full source so output changes when source changes
for src in {srcs}; do
    src_hash=$(shasum "$src" | cut -c1-12)
    while IFS= read -r line; do
        for kw in Class Enum Association Mapping Function Database Runtime Connection Service; do
            if echo "$line" | grep -q "^$kw "; then
                fqn=$(echo "$line" | sed "s/^$kw //" | sed 's/[{{ (].*//' | tr -d '[:space:]')
                if echo "$fqn" | grep -q "::"; then
                    fname=$(echo "$fqn" | sed 's/::/__/g').json
                    echo '{{"fqn": "'$fqn'", "source": "'$src'", "hash": "'$src_hash'"}}' > "$OUT_DIR/$fname"
                    echo "  Produced: $fname"
                fi
            fi
        done
    done < "$src"
done

# Track which dep element files we actually used
# Simulate: read each dep element, check if any source references its FQN
UNUSED_FILE="{unused}"
> "$UNUSED_FILE"

for dep in {deps}; do
    dep_fqn=$(cat "$dep" 2>/dev/null | grep -o '"fqn": "[^"]*"' | head -1 | sed 's/"fqn": "//;s/"//')
    # Check if any source file references this FQN (simple grep)
    used=false
    for src in {srcs}; do
        if grep -q "$dep_fqn" "$src" 2>/dev/null; then
            used=true
            break
        fi
    done
    if [ "$used" = "false" ]; then
        echo "$dep" >> "$UNUSED_FILE"
    fi
done

echo "  Unused deps: $(wc -l < "$UNUSED_FILE" | tr -d ' ') files"
""".format(
        label = ctx.label,
        out_dir = output_files[0].dirname if output_files else "",
        srcs = " ".join([f.path for f in srcs]),
        deps = " ".join([f.path for f in dep_element_files]),
        unused = unused.path,
    )

    ctx.actions.write(output = script, content = script_content, is_executable = True)

    ctx.actions.run(
        executable = script,
        outputs = output_files + [unused],
        inputs = srcs + dep_element_files,
        tools = [script],
        unused_inputs_list = unused,
        mnemonic = "LegendCompile",
    )

    return [
        LegendElementsInfo(element_files = output_files),
        DefaultInfo(files = depset(output_files)),
    ]

legend_library = rule(
    implementation = _legend_library_impl,
    attrs = {
        "srcs": attr.label_list(allow_files = [".pure"]),
        "elements": attr.string_list(),
        "deps": attr.label_list(providers = [LegendElementsInfo]),
    },
)
