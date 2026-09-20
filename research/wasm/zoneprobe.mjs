/**
 * The TIMEZONE half of the differential — see planner/ZoneMain.java.
 *
 *   node --experimental-wasm-exnref zoneprobe.mjs > target/zone-wasm.txt
 *   java -cp "$CORE:target/classes" planner.ZoneMain > target/zone-jvm.txt
 *   diff target/zone-jvm.txt target/zone-wasm.txt
 *
 * Keep the cases here in step with ZoneMain.CASES.
 */
import { load } from './target/wasm-runtime/org/teavm/backend/wasm/wasm-gc-module-runtime.js';

const HERE = new URL('.', import.meta.url).pathname;
const teavm = await load(HERE + 'target/wasm/classes.wasm', {
  stackDeobfuscator: { enabled: false },
  installImports(o) {
    o.teavmConsole = o.teavmConsole || {};
    o.teavmConsole.putcharStdout = () => {};
    o.teavmConsole.putcharStderr = () => {};
  },
});

const CASES = [
  ['2026-01-15T12:00:00', 'UTC'],
  ['2026-01-15T12:00:00', 'America/New_York'],    // EST, -5
  ['2026-07-15T12:00:00', 'America/New_York'],    // EDT, -4 — a DST rule
  ['2026-01-15T12:00:00', 'Asia/Tokyo'],          // +9, no DST
  ['2026-01-15T12:00:00', 'Asia/Kolkata'],        // +5:30, half-hour offset
  ['2026-01-15T12:00:00', 'Australia/Lord_Howe'], // +11, half-hour DST
  ['2026-01-15T12:00:00.123', 'Europe/London'],   // sub-second shape kept
  ['2026-01-15T12:00:00', 'Not/AZone'],           // must refuse, identically
];

for (const [iso, zone] of CASES) {
  console.log(`${iso}\t${zone}\t${teavm.exports.zoneProbe(iso, zone)}`);
}
