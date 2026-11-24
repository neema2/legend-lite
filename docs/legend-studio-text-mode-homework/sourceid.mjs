// Form mode's compile: lambdas carry Studio's sourceId; does the compile error return it?
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
const [dir, engineBase] = process.argv.slice(2);
const { load } = await import(pathToFileURL(resolve(dir, 'wasm-gc-module-runtime.js')).href);
const m = await load(resolve(dir, 'classes.wasm'), { stackDeobfuscator: { enabled: false }, installImports(o) { o.teavmConsole = { putcharStdout() {}, putcharStderr() {} }; } });
const lite = (path, q, body) => { const r = m.exports.pureV1OrError(`/api/pure/v1/${path}`, q, body).split('\n'); return { status: r[1], body: r.slice(3).join('\n') }; };
const eng = async (path, q, body, type) => { const r = await fetch(`${engineBase}/api/pure/v1/${path}${q ? '?' + q : ''}`, { method: 'POST', headers: { 'content-type': type }, body }); return { status: r.status, body: await r.text() }; };
const text = "Class model::A\n{\n  name: String[1];\n  upper() {$this.nme->toUpper()}: String[1];\n}\n";
const pmcd = JSON.parse((await eng('grammar/grammarToJson/model', 'returnSourceInformation=true', text, 'text/plain')).body);
const ID = 'model::A@derivedProperty@upper@u1';
const stamp = (n) => { if (Array.isArray(n)) n.forEach(stamp); else if (n && typeof n === 'object') { if (n.sourceInformation) n.sourceInformation.sourceId = ID; Object.values(n).forEach(stamp); } };
const cls = pmcd.elements.find((e) => e._type === 'class');
stamp(cls.qualifiedProperties[0].body);
const payload = JSON.stringify(pmcd);
const [e, l] = [await eng('compilation/compile', '', payload, 'application/json'), lite('compilation/compile', '', payload)];
const si = (b) => { try { return JSON.stringify(JSON.parse(b).sourceInformation); } catch { return b.slice(0, 120); } };
console.log('engine', e.status, si(e.body));
console.log('lite  ', l.status, si(l.body));
