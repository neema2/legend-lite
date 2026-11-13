// THE BRAND'S CASTS, READ BY TYPESCRIPT'S OWN CHECKER (the re-audit's S5, 2026-11-12: until then a regex census read
// the casts' text, and missed `x as Parameters<DuckDbEngine['run']>[0]`, `interface X extends Plan {}` and
// `Omit<Plan, ...>`, each a PlannedSql made of any text under a name the regex did not know). A plan's statement
// (PlannedSql) and a planner's answer (PlanText) are minted where a plan is read alone (engine-client/src/planned-sql.ts);
// every other way TypeScript lets a value take a type it was not given is read here by the type it RESOLVES to:
//
//   * a type assertion (`x as T`, `<T>x`) whose type carries either brand -- the brand itself, or an object, array,
//     promise, function or union holding one, however it is spelled (an alias, an interface extending a holder, a
//     mapped or indexed type: the checker resolves it);
//   * a type predicate (`x is T`, `asserts x is T`) whose type carries either brand: a guard is a cast the caller cannot
//     see;
//   * an assertion to a bare type parameter (`x as T`, T generic): it launders any type -- a caller instantiating T as a
//     PlannedSql mints one -- so each is counted, whatever T is.
//
// `as const` is not read: a literal's type is its own, and holds no brand it was not given. TypeScript 7's API (the
// unstable sync API, the pinned compiler's own checker over IPC) fails on some readonly tuple types; a type it cannot
// read is reported, never taken for one that carries nothing (no fallback, G7).

import { existsSync } from 'node:fs';
import { join } from 'node:path';
import { API, SignatureKind, TypeFlags, type Checker, type Type } from 'typescript/unstable/sync';
import { isAsExpression, isIdentifier, isTypeAliasDeclaration, isTypeAssertion, isTypePredicateNode, isTypeReferenceNode, type Node, type SourceFile, type TypeNode } from 'typescript/unstable/ast';

/** One cast the checker read: where it is (a path from the repository's root, a 1-based line, its offset), what kind,
 *  its type. */
export interface Cast {
  readonly file: string;
  readonly line: number;
  readonly at: number;
  readonly kind: 'brand' | 'generic' | 'predicate';
  readonly type: string;
}

/** What one project's reading found: its casts in scope, the files read, and any type the API could not read. */
export interface Read {
  readonly casts: readonly Cast[];
  readonly files: number;
  readonly unreadable: readonly string[];
}

/** The repository's top-level TypeScript directories: a file's path from the root starts at the first one in it. */
const TOP = ['engine-client', 'datacube', 'pure-protocol', 'query-store', 'sdlc-client', 'depot-client', 'model-changes',
  'legend-art', 'studio', 'query'];

/**
 * `file`'s path from the repository's root (`cwd`), or undefined where it is in none of its TypeScript directories: the
 * longest tail of the path the checker names it by that starts at one of them and is there under `cwd` (under Bazel the
 * checker may name a file by a path through the runfiles tree, which holds a package's name before the repository's).
 */
export function fromRoot(file: string, cwd: string): string | undefined {
  const normal = file.replace(/\\/g, '/');
  for (let at = normal.indexOf('/'); at >= 0; at = normal.indexOf('/', at + 1)) {
    const tail = normal.slice(at + 1);
    if (TOP.includes(tail.split('/')[0] ?? '') && existsSync(join(cwd, tail))) return tail;
  }
  return undefined;
}

/** The brands' own file, by the path the checker names it (a pattern: the census of planned-sql.ts's importers reads
 *  quoted paths as imports, and this names a file, imports none). */
const BRANDS = /(?:^|\/)engine-client\/src\/planned-sql\.ts$/;

/** Product code: a package's `src`, never a test, a demo or a tool. */
export const PRODUCT = (file: string): boolean => /^[^/]+\/src\//.test(file) && !file.includes('/node_modules/');

/** A type the checker cannot narrow further, or one that holds nothing: any, unknown, never. */
const OPAQUE = TypeFlags.Any | TypeFlags.Unknown | TypeFlags.Never;

/** A declaration in the language's own library or a package's: their members are read through type arguments. */
const LIBRARY = /\/node_modules\/|^bundled:|\/lib\.[^/]*\.d\.ts$/;

/**
 * The casts `config`'s program holds in the files `inScope` keeps: each project opened by the pinned compiler's own
 * checker (`cwd`, the directory its paths are from).
 */
export function readCasts(config: string, cwd: string, inScope: (file: string) => boolean): Read {
  const api = new API({ cwd });
  try {
    const snapshot = api.updateSnapshot({ openProjects: [config] });
    const project = snapshot.getProjects()[0];
    if (project === undefined) throw new Error(`no project opened for ${config}`);
    const { program, checker } = project;
    const brandFile = program.getSourceFileNames().find((f) => BRANDS.test(f.replace(/\\/g, '/')));
    const brands = brandsOf(checker, brandFile === undefined ? undefined : program.getSourceFile(brandFile));
    const unreadable: string[] = [];
    const carries = carrying(checker, brands, unreadable);
    const casts: Cast[] = [];
    let files = 0;
    for (const name of program.getSourceFileNames()) {
      const file = fromRoot(name, cwd);
      if (file === undefined || file.endsWith('.d.ts') || !inScope(file)) continue;
      const source = program.getSourceFile(name);
      if (source === undefined) throw new Error(`the program names ${name} and holds no source for it`);
      files++;
      const assertions: Node[] = [];
      const predicates: TypeNode[] = [];
      const visit = (n: Node): void => {
        // `as const`: a literal's own type, which holds no brand it was not given
        if ((isAsExpression(n) || isTypeAssertion(n))
          && !(isTypeReferenceNode(n.type) && isIdentifier(n.type.typeName) && n.type.typeName.text === 'const')) {
          assertions.push(n);
        }
        if (isTypePredicateNode(n) && n.type !== undefined) predicates.push(n.type);
        n.forEachChild(visit);
      };
      source.forEachChild(visit);
      const line = (n: Node): number => source.getLineAndCharacterOfPosition(n.end).line + 1;
      if (assertions.length > 0) {
        checker.getTypeAtLocation(assertions).forEach((t, i) => {
          const at = assertions[i]!;
          if (t === undefined) {
            unreadable.push(`${file}:${line(at)}: the assertion's type`);
          } else if (t.isTypeParameter()) {
            casts.push({ file, line: line(at), at: at.end, kind: 'generic', type: checker.typeToString(t) });
          } else if (carries(t, 0, `${file}:${line(at)}`)) {
            casts.push({ file, line: line(at), at: at.end, kind: 'brand', type: checker.typeToString(t) });
          }
        });
      }
      for (const p of predicates) {
        const t = checker.getTypeFromTypeNode(p);
        if (t === undefined) unreadable.push(`${file}:${line(p)}: the predicate's type`);
        else if (carries(t, 0, `${file}:${line(p)}`)) casts.push({ file, line: line(p), at: p.end, kind: 'predicate', type: checker.typeToString(t) });
      }
    }
    return { casts: casts.sort((a, b) => a.file.localeCompare(b.file) || a.at - b.at), files, unreadable };
  } finally {
    api.close();
  }
}

/** PlannedSql and PlanText, as planned-sql.ts declares them. */
function brandsOf(checker: Checker, file: SourceFile | undefined): readonly Type[] {
  if (file === undefined) throw new Error("the program holds no engine-client/src's brands' file: no brand to read casts by");
  const brands: Type[] = [];
  file.forEachChild((n) => {
    if (isTypeAliasDeclaration(n) && (n.name.text === 'PlannedSql' || n.name.text === 'PlanText')) {
      const symbol = checker.getSymbolAtLocation(n.name);
      if (symbol === undefined) throw new Error(`the brands' file's ${n.name.text} has no symbol`);
      brands.push(checker.getDeclaredTypeOfSymbol(symbol));
    }
  });
  if (brands.length !== 2) throw new Error(`the brands' file declares PlannedSql and PlanText; ${brands.length} found`);
  return brands;
}

/**
 * Whether a type carries either brand: it is one (assignable to it, and not opaque), or a union or intersection member
 * does, or a type argument, an index's value, a call's or a construction's return, or a property does (five levels
 * down; a library's generic through its type arguments alone). A property the API cannot read is recorded in `unreadable`.
 */
function carrying(checker: Checker, brands: readonly Type[], unreadable: string[]): (t: Type | undefined, depth: number, where: string) => boolean {
  const known = new Map<number, boolean>();
  const carries = (t: Type | undefined, depth: number, where: string): boolean => {
    if (t === undefined || (t.flags & OPAQUE) !== 0) return false;
    const was = known.get(t.id);
    if (was !== undefined) return was;
    // a cycle carries no more than its first visit finds
    known.set(t.id, false);
    let out = brands.some((b) => checker.isTypeAssignableTo(t, b));
    if (!out && (t.isUnionType() || t.isIntersectionType())) out = t.getTypes().some((u) => carries(u, depth, where));
    if (!out && t.isTypeParameter()) out = carries(checker.getBaseConstraintOfType(t), depth, where);
    if (!out && depth < 5 && t.isObjectType()) {
      if (t.isTypeReference()) out = checker.getTypeArguments(t).some((a) => carries(a, depth + 1, where));
      if (!out) out = checker.getIndexInfosOfType(t).some((i) => carries(i.valueType, depth + 1, where));
      if (!out) {
        out = [...checker.getSignaturesOfType(t, SignatureKind.Call), ...checker.getSignaturesOfType(t, SignatureKind.Construct)]
          .some((s) => carries(checker.getReturnTypeOfSignature(s), depth + 1, where));
      }
      // a library's generic (an array, a promise, a map) through its type arguments, read above; any other type -- a
      // library's mapped type (Omit, Pick, Partial) included -- through its properties
      const library = t.isTypeReference() && t.getSymbol()?.declarations.some((d) => LIBRARY.test(String(d.path))) === true;
      if (!out && !library) {
        out = checker.getPropertiesOfType(t).some((p) => {
          let type: Type | undefined;
          try {
            type = checker.getTypeOfSymbol(p);
          } catch (e) {
            unreadable.push(`${where}: ${p.name} of ${checker.typeToString(t)} (${e instanceof Error ? e.message.split('\n')[0] : String(e)})`);
            return false;
          }
          return carries(type, depth + 1, where);
        });
      }
    }
    known.set(t.id, out);
    return out;
  };
  return carries;
}

/** `casts` counted by file and kind, `file: kind` keyed, in order. */
export function counted(casts: readonly Cast[]): Record<string, number> {
  const out: Record<string, number> = {};
  for (const c of [...casts].sort((a, b) => `${a.file}:${a.kind}`.localeCompare(`${b.file}:${b.kind}`))) {
    const key = `${c.file}: ${c.kind}`;
    out[key] = (out[key] ?? 0) + 1;
  }
  return out;
}
