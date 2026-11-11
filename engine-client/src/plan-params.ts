// THE VALUES A PLAN'S STATEMENT BINDS THAT THE PLAN CARRIES (the B checkpoint's tenets audit, M3, 2026-11-11, the
// orchestrator's go; the user's ruling "make json work everywhere"): a data: source's payload -- one JSON array -- is the
// plan's own value, bound to the statement's `?` (its dialect's delivery: DuckDB splits it with json_each), named by its
// SHA-256 and held in the statement's content (the JSON pass). A host that runs a plan's TEXT (the tab's DuckDB, Python's,
// the tab's pure/v1 execute) reads the values here, once, where it reads the plan: each from the content by its digest,
// verified by SHA-256 (as PlanJson.contentOf and Java's PlanRunner.value do); any other slot -- a declared parameter, a
// key's cell, a file's path, a list -- is refused by name, as before. The text stays the text: a `?` where each binds
// ("Show SQL" shows the `?`, never the payload).

import { sha256Hex } from './sha256.ts';

/** A plan this host does not run as its text and values, refused by name. */
export class PlanParamsRefusal extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PlanParamsRefusal';
  }
}

/** A statement as the plan writes it (PlanJson `sql`): its slots and the content its payload slots name. */
interface Statement {
  readonly slots?: readonly { readonly parameter: string; readonly binding: { readonly _type: string; readonly digest?: string } }[];
  readonly content?: Readonly<Record<string, string>>;
}

/**
 * The values the answering statement of `plan` (PlanJson's text, parsed; or none: a text that runs alone) binds, in
 * its placeholders' order: each payload's text, its bytes checked against the digest its slot names. A slot of another
 * kind, a payload its content does not carry, or one whose bytes are not their digest is refused by name.
 */
export function planParams(plan: unknown): readonly string[] {
  if (plan === undefined || plan === null) return [];
  const root = (plan as { readonly root?: { readonly _type?: string; readonly sql?: Statement } }).root;
  if (root === undefined || root.sql === undefined) {
    throw new PlanParamsRefusal(`a text plan's statement is its root's (a typed result's), not a ${String(root?._type)}'s`);
  }
  return statementParams(root.sql);
}

/** The values `statement`'s slots bind (planParams' rule), in its placeholders' order. */
export function statementParams(statement: Statement): readonly string[] {
  const content = statement.content ?? {};
  return (statement.slots ?? []).map((slot) => {
    if (slot.binding._type !== 'payload') {
      throw new PlanParamsRefusal(`'${slot.parameter}' binds ${slot.binding._type === 'key' ? "a key's cell"
        : slot.binding._type === 'source' ? "a file source's path" : slot.binding._type === 'array' ? 'a list'
          : 'a value the run supplies'}, which a host running the plan's text does not bind`);
    }
    return payloadValue(content, slot.binding.digest ?? '');
  });
}

/** The payload `content` holds under `digest`, its bytes checked against it: one the content does not carry, or whose
 *  bytes are not their digest, refused by name. */
export function payloadValue(content: Readonly<Record<string, string>>, digest: string): string {
  const text = content[digest];
  if (text === undefined) throw new PlanParamsRefusal(`the plan's statement binds the payload ${digest}, which its content does not carry`);
  const actual = sha256Hex(text);
  if (actual !== digest) {
    throw new PlanParamsRefusal(`the data: source's payload is not the data it was planned against: its digest is ${actual}, where the plan names ${digest}`);
  }
  return text;
}
