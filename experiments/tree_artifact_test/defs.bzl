# --- EXPERIMENT 1: Tree artifact (opaque directory) ---

def _producer_impl(ctx):
    out_dir = ctx.actions.declare_directory(ctx.label.name + "_elements")
    ctx.actions.run_shell(
        outputs = [out_dir],
        inputs = ctx.files.srcs,
        command = """
            mkdir -p {out}
            for f in {srcs}; do
                base=$(basename "$f" .txt)
                cp "$f" "{out}/$base.json"
            done
        """.format(
            out = out_dir.path,
            srcs = " ".join([f.path for f in ctx.files.srcs]),
        ),
        mnemonic = "ProduceElements",
    )
    return [DefaultInfo(files = depset([out_dir]))]

producer = rule(
    implementation = _producer_impl,
    attrs = {"srcs": attr.label_list(allow_files = True)},
)

# --- EXPERIMENT 2: Individual declared files (elements listed in BUILD) ---

ElementsInfo = provider(fields = ["files"])

def _individual_producer_impl(ctx):
    output_files = []
    for src in ctx.files.srcs:
        base = src.basename.replace(".txt", ".json")
        out_file = ctx.actions.declare_file(ctx.label.name + "_elements/" + base)
        output_files.append(out_file)

    # Single action that writes all individual files
    ctx.actions.run_shell(
        outputs = output_files,
        inputs = ctx.files.srcs,
        command = """
            for f in {srcs}; do
                base=$(basename "$f" .txt)
                cp "$f" "{out_dir}/$base.json"
            done
        """.format(
            srcs = " ".join([f.path for f in ctx.files.srcs]),
            out_dir = output_files[0].dirname,
        ),
        mnemonic = "ProduceIndividualElements",
    )
    return [
        DefaultInfo(files = depset(output_files)),
        ElementsInfo(files = output_files),
    ]

individual_producer = rule(
    implementation = _individual_producer_impl,
    attrs = {"srcs": attr.label_list(allow_files = True)},
)

# Rule that consumes a tree artifact, uses only some files,
# and lists unused ones in unused_inputs_list
def _consumer_impl(ctx):
    dep_dir = ctx.attr.dep[DefaultInfo].files.to_list()[0]  # the tree artifact

    out = ctx.actions.declare_file(ctx.label.name + "_output.txt")
    unused = ctx.actions.declare_file(ctx.label.name + ".unused_inputs")

    # Write the consumer script
    script = ctx.actions.declare_file(ctx.label.name + "_consumer.sh")
    ctx.actions.write(
        output = script,
        content = """#!/bin/bash
echo "=== CONSUMER RUNNING ==="
DEP_DIR="$1"
OUT="$2"
UNUSED="$3"

echo "Reading: $DEP_DIR/used_element.json"
cat "$DEP_DIR/used_element.json" > "$OUT"

# List all files in the tree artifact that we did NOT use
> "$UNUSED"
for f in "$DEP_DIR"/*.json; do
    base=$(basename "$f")
    if [ "$base" != "used_element.json" ]; then
        echo "$DEP_DIR/$base" >> "$UNUSED"
    fi
done

echo "--- unused list contents ---"
cat "$UNUSED"
""",
        is_executable = True,
    )

    ctx.actions.run(
        executable = script,
        arguments = [dep_dir.path, out.path, unused.path],
        outputs = [out, unused],
        inputs = [dep_dir],
        tools = [script],
        unused_inputs_list = unused,
        mnemonic = "ConsumeElements",
    )
    return [DefaultInfo(files = depset([out]))]

consumer = rule(
    implementation = _consumer_impl,
    attrs = {"dep": attr.label()},
)

# --- EXPERIMENT 2 consumer: takes individual files as inputs ---

def _individual_consumer_impl(ctx):
    dep_files = ctx.attr.dep[ElementsInfo].files  # list of individual File objects

    out = ctx.actions.declare_file(ctx.label.name + "_output.txt")
    unused = ctx.actions.declare_file(ctx.label.name + ".unused_inputs")

    # Find the "used" file and the "unused" files
    used_file = None
    unused_files = []
    for f in dep_files:
        if "used_element" == f.basename.replace(".json", ""):
            used_file = f
        else:
            unused_files.append(f)

    # Write the consumer script
    script = ctx.actions.declare_file(ctx.label.name + "_consumer.sh")
    ctx.actions.write(
        output = script,
        content = """#!/bin/bash
echo "=== INDIVIDUAL CONSUMER RUNNING ==="
USED="$1"
OUT="$2"
UNUSED_LIST="$3"
shift 3

echo "Reading: $USED"
cat "$USED" > "$OUT"

# Write unused file paths
> "$UNUSED_LIST"
for f in "$@"; do
    echo "$f" >> "$UNUSED_LIST"
done

echo "--- unused list contents ---"
cat "$UNUSED_LIST"
""",
        is_executable = True,
    )

    ctx.actions.run(
        executable = script,
        arguments = [used_file.path, out.path, unused.path] + [f.path for f in unused_files],
        outputs = [out, unused],
        inputs = dep_files,
        tools = [script],
        unused_inputs_list = unused,
        mnemonic = "ConsumeIndividualElements",
    )
    return [DefaultInfo(files = depset([out]))]

individual_consumer = rule(
    implementation = _individual_consumer_impl,
    attrs = {"dep": attr.label(providers = [ElementsInfo])},
)
