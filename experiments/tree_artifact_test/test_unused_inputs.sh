#!/bin/bash
# Test: does unused_inputs_list work with individual files inside tree artifacts?
#
# Expected behavior if it WORKS:
#   1. Build trading (consumer) — succeeds, runs ConsumeElements
#   2. Change unused_element_a.txt (which trading listed as unused)
#   3. Rebuild trading — should NOT re-run ConsumeElements (cached)
#   4. Change used_element.txt (which trading actually reads)
#   5. Rebuild trading — SHOULD re-run ConsumeElements
#
# If per-element tracking inside tree artifacts works, step 3 skips and step 5 rebuilds.
# If it doesn't work, step 3 also rebuilds (treating the whole directory as one unit).

set -e
cd "$(dirname "$0")"

echo "========================================="
echo "Step 1: Initial build"
echo "========================================="
bazel build //:trading 2>&1 | tail -5

echo ""
echo "========================================="
echo "Step 2: Rebuild (no changes) — should be cached"
echo "========================================="
bazel build //:trading 2>&1 | tail -5

echo ""
echo "========================================="
echo "Step 3: Change UNUSED element, rebuild"
echo "========================================="
echo "version2: CHANGED but NOT used by trading" > src/unused_element_a.txt
bazel build //:trading 2>&1 | tail -5

echo ""
echo "========================================="
echo "Step 4: Change USED element, rebuild"
echo "========================================="
echo "version2: CHANGED and IS used by trading" > src/used_element.txt
bazel build //:trading 2>&1 | tail -5

echo ""
echo "========================================="
echo "Step 5: Verify output reflects the change"
echo "========================================="
cat bazel-bin/trading_output.txt

echo ""
echo "========================================="
echo "DONE. Check above output:"
echo "  - Step 3: if 'ConsumeElements' did NOT run → per-element tracking WORKS"
echo "  - Step 3: if 'ConsumeElements' DID run → only per-project tracking"
echo "========================================="
