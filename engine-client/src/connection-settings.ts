// A HOST'S HELD VALUES AND THE SETTINGS A LOAD MAKES ON A CONNECTION (B3's Q3, 2026-11-11, the user's ruling: "Yes do
// option 2"; docs/data-delivery-homework/B3_REMOTE_MOUNT_DESIGN_2026_11_11.md): a plan's bound statement names what the
// host holds -- a remote file's key, a bearer URL -- and never holds it; the host binds it (HostSecrets). A statement of
// the connection's (a session variable: one connection's, measured) is kept by the host and replayed on every connection
// it opens to that database (ConnectionSettings). A module of its own, apart from the plan runner, so the tab's
// connections keep and replay their settings without carrying the runner (plan-runner.ts re-exports it).

import type { PlanValue } from './plan-runner.ts';

/** One `?` of a bound statement: the plan's own value (never a credential), or a value the host holds by name. */
export type Bind =
  | { readonly _type: 'value'; readonly text: string }
  | { readonly _type: 'held'; readonly name: string; readonly field: string };
/** A setting of a connection a load made (a session variable): its statement and what it binds, never a value -- what a
 *  host replays on every connection it opens to that database (`replaySettings`, ConnectionSettings). */
export interface KeptSetting { readonly sql: string; readonly binds: readonly Bind[] }

/**
 * WHAT A HOST HOLDS BY NAME (the plan's held binds, B3's Q3): a value of its own store -- a remote file's key's field, a
 * bearer URL -- by the name and field the plan names it by; one it does not hold refused by name. The plan holds none.
 */
export interface HostSecrets {
  value(name: string, field: string): PlanValue;
}

/** `binds` as the values a statement's `?`s take: the plan's own text, or what `secrets` holds under the name; a bind of
 *  another kind, or a held one with no store, refused by name (`what` names its statement). */
export function boundValues(binds: readonly Bind[], secrets: HostSecrets | undefined, what: string): PlanValue[] {
  return binds.map((b) => {
    if (b._type === 'value') return (b as Extract<Bind, { _type: 'value' }>).text;
    // a bind read from a plan's JSON: its tag checked, as every tag the runner reads is
    if ((b as { readonly _type: string })._type !== 'held') throw new Error(`${what}'s bind is the plan's value or a held one, not ${(b as { readonly _type: string })._type}`);
    if (secrets === undefined) throw new Error(`${what} binds what a host holds, and this run was given no host's store`);
    const h = b as Extract<Bind, { _type: 'held' }>;
    return secrets.value(h.name, h.field);
  });
}

/** A load's connection settings run on another connection (`run`), each bound from `secrets`: the host's replay on a
 *  connection it opens after the load made them (a session variable is one connection's: measured, B3's Q3). */
export async function replaySettings(settings: readonly KeptSetting[],
  run: (sql: string, params: readonly PlanValue[]) => Promise<unknown>, secrets: HostSecrets): Promise<void> {
  for (const setting of settings) await run(setting.sql, boundValues(setting.binds, secrets, 'a connection setting'));
}

/** The shortest query-string parameter value redacted on its own: a shorter one (`X-Amz-Expires=3600`'s, a `1`) is no
 *  token, and taking it out of a message would take out the message's own words and numbers. The whole URL and the whole
 *  query string are redacted whatever their length. */
const SHORTEST_HELD_PARAMETER = 8;

/**
 * THE TEXTS A HELD VALUE MAY COME BACK AS (the B checkpoint's tenets audit, M2, 2026-11-11): a database repeats a value it
 * was bound in its own error -- an expired presigned link's read names the whole URL (measured, B3's Q3) -- and may quote
 * its query string, or one parameter, alone, percent-encoded or not. So a held URL is taken out as itself, its query
 * string, and each parameter's value (raw and decoded); any other held value as itself.
 */
export function heldTexts(value: string): string[] {
  const out = new Set<string>([value]);
  const query = value.indexOf('?');
  if (query >= 0) {
    const fragment = value.indexOf('#', query);
    const string = value.slice(query + 1, fragment < 0 ? undefined : fragment);
    out.add(string);
    for (const part of string.split('&')) {
      const raw = part.slice(part.indexOf('=') + 1);
      for (const v of [raw, decoded(raw)]) if (v.length >= SHORTEST_HELD_PARAMETER) out.add(v);
    }
  }
  // the longest first, so a part never leaves the rest of a longer one behind
  return [...out].filter((t) => t.length > 0).sort((a, b) => b.length - a.length);
}

const decoded = (text: string): string => {
  try {
    return decodeURIComponent(text.replace(/\+/g, ' '));
  } catch {
    // not percent-encoding as written: the raw text is redacted, and nothing else is read from it
    return text;
  }
};

/** `text` with every held value in `values` (each as heldTexts takes it apart) written `***`. */
export function redactHeld(text: string, values: Iterable<string>): string {
  let out = text;
  for (const value of values) for (const t of heldTexts(value)) out = out.split(t).join('***');
  return out;
}

/**
 * THE SETTINGS A HOST KEEPS FOR ITS CONNECTIONS (B3's Q3): each mount's connection settings, by the mount's name, with
 * the store that binds them -- replayed on every connection the host opens afterwards (the tab's `connect`), forgotten
 * when the mount is let go of.
 */
export class ConnectionSettings {
  readonly #kept = new Map<string, { readonly settings: readonly KeptSetting[]; readonly secrets: HostSecrets }>();

  /** `text` with every value the kept settings bind from a host's store taken out (redactHeld): what a database's error
   *  is written through before it is shown, logged or kept -- a bearer URL's read repeats the URL (M2). */
  redact(text: string): string {
    const held: string[] = [];
    for (const { settings, secrets } of this.#kept.values()) {
      for (const setting of settings) {
        for (const b of setting.binds) {
          if (b._type !== 'held') continue;
          const v = secrets.value(b.name, b.field);
          if (typeof v === 'string') held.push(v);
        }
      }
    }
    return held.length === 0 ? text : redactHeld(text, held);
  }

  keep(name: string, settings: readonly KeptSetting[], secrets: HostSecrets): void {
    if (settings.length === 0) this.#kept.delete(name);
    else this.#kept.set(name, { settings, secrets });
  }

  forget(name: string): void {
    this.#kept.delete(name);
  }

  /** Every kept setting run on a connection just opened. */
  async replay(run: (sql: string, params: readonly PlanValue[]) => Promise<unknown>): Promise<void> {
    for (const { settings, secrets } of this.#kept.values()) await replaySettings(settings, run, secrets);
  }
}

