// A HOST'S NAMED OPERATIONS on its own database (docs/ONE_PLATFORM_PROGRAM_2026_10_12.md D5): what the tab used to
// spell as SQL itself -- a file read into a table, a remote file mounted, a copy made, rows counted, an extension, a
// secret -- named here, with its data, and answered by legend-lite's planner (Boundary.hostOperations: the platform's
// HostOperations in Java, by the database's type) as the statements the database runs. The page spells no SQL; a
// database without an operation refuses it by name. Python's host writes the same JSON (compiler.host_operations). The
// operations are asked of the planner and their statements run by their door (doors/host-operations.ts, C5-T, 2026-11-15).

export type {
  CatalogColumn, HostName, HostOperation, HostSource, RemoteMountRequest, SecretField, SecretValue, SnapCopyRequest, SnapPull,
  SnapStreamRequest, SynthesizeKnob, SynthesizeRequest,
} from './requests.ts';
import type { SecretField, SecretValue } from './requests.ts';

/**
 * THE HOST'S SECRETS, BY NAME (B3, 2026-11-11, docs/TOTAL_WALL_START_HERE_2026_11_11.md §5.2: "credentials by name ...
 * a guard keeps any secret value out of the plan"): the values a person gave a source -- its keys, a bearer URL --
 * held in this host's memory under a name and bound into a planned statement where the plan names them (plan-runner.ts
 * HostSecrets) -- never written into an operation, a statement's text or a plan. A name this store does not hold, or a
 * field it holds no value for, is refused by name, the values never named.
 */
export class SecretStore {
  readonly #held = new Map<string, ReadonlyMap<SecretField, SecretValue>>();

  /** `values` held under `name`, replacing what it held. */
  hold(name: string, values: ReadonlyMap<SecretField, SecretValue>): void {
    this.#held.set(name, new Map(values));
  }

  /** The value held under `name`'s `field`: what a plan's held bind names (plan-runner.ts HostSecrets). */
  value(name: string, field: string): SecretValue {
    const values = this.#held.get(name);
    if (values === undefined) throw new Error(`'${name}' is not a secret this host holds`);
    const value = values.get(field as SecretField);
    if (value === undefined) throw new Error(`the secret '${name}' holds no ${field}`);
    return value;
  }
}
