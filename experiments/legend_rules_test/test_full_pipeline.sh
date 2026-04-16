#!/bin/bash
# Full end-to-end test of the Legend Bazel pipeline:
#
# 1. Module extension scans .pure files, discovers element FQNs
# 2. legend_library declares individual output files per element
# 3. unused_inputs_list tracks per-element usage
# 4. Changing an unused dep element → cache hit (no rebuild)
# 5. Changing a used dep element → rebuild
# 6. Adding a new element to a .pure file → module extension detects it

set -e

echo "========================================="
echo "Step 1: Clean build of trading (depends on refdata)"
echo "========================================="
bazel clean 2>&1 | tail -2
bazel build //projects/trading 2>&1 | grep -E "LegendCompile|processes|Elapsed|ELEMENTS|element"
echo ""

echo "========================================="
echo "Step 2: No-change rebuild — should be fully cached"
echo "========================================="
bazel build //projects/trading 2>&1 | grep -E "LegendCompile|processes|Elapsed"
echo ""

echo "========================================="
echo "Step 3: Change UNUSED dep element (refdata::Region)"
echo "  Trading references refdata::Sector but NOT refdata::Region"
echo "  Expected: refdata rebuilds, trading is a CACHE HIT"
echo "========================================="
cat > projects/refdata/src/refdata/Region.pure << 'EOF'
Class refdata::Region
{
    name: String[1];
    isoCode: String[1];
    continent: String[0..1];
}
EOF
bazel build //projects/trading 2>&1 | grep -E "LegendCompile|processes|Elapsed|cache"
echo ""

echo "========================================="
echo "Step 4: Change USED dep element (refdata::Sector)"
echo "  Trading references refdata::Sector"
echo "  Expected: both refdata and trading rebuild"
echo "========================================="
cat > projects/refdata/src/refdata/Sector.pure << 'EOF'
Class refdata::Sector
{
    name: String[1];
    code: String[1];
    region: refdata::Region[0..1];
}

Enum refdata::SectorType
{
    EQUITY,
    FIXED_INCOME,
    COMMODITY,
    CRYPTO
}
EOF
bazel build //projects/trading 2>&1 | grep -E "LegendCompile|processes|Elapsed|cache"
echo ""

echo "========================================="
echo "Step 5: Add NEW element to refdata (refdata::Currency)"
echo "  Module extension should detect the new element!"
echo "========================================="
cat > projects/refdata/src/refdata/Currency.pure << 'EOF'
Class refdata::Currency
{
    code: String[1];
    name: String[1];
}
EOF
bazel build //projects/trading 2>&1 | grep -E "LegendCompile|processes|Elapsed|element|Currency"
echo ""

echo "========================================="
echo "Step 6: Verify all element files exist"
echo "========================================="
echo "Refdata elements:"
ls -1 bazel-bin/projects/refdata/refdata_elements/ 2>/dev/null || echo "(not found)"
echo ""
echo "Trading elements:"
ls -1 bazel-bin/projects/trading/trading_elements/ 2>/dev/null || echo "(not found)"
echo ""

echo "========================================="
echo "RESULTS SUMMARY:"
echo "  Step 2: cache hit = module extension + rule working"
echo "  Step 3: trading cache hit = per-element unused tracking WORKS"
echo "  Step 4: trading rebuild = used element detection WORKS"
echo "  Step 5: new file detected = module extension auto-discovers"
echo "========================================="
