// EACH COMMAND'S FIELDS, AS DATA (docs/DATACUBE_ONE_API_PLAN_2026_10_21.md D17): a command module lists each kind's
// fields by name -- what its reader takes, and what the Python builders generated from it write
// (datacube/tools/python-commands/emit.ts) -- and the compiler holds the list to the command's type, both ways: a field
// listed that the type has not, or one the type has that is not listed, is a type error naming it. A field a command may
// leave out ends in `?`.

/** A command type's field names, `?` ending an optional one: what `CommandFields` lists for it. */
type FieldName<C> = {
  [P in Exclude<keyof C, 'kind'>]-?: {} extends Pick<C, P> ? `${P & string}?` : P & string
}[Exclude<keyof C, 'kind'>];

/** Each kind of a command union, its fields by name. */
export type CommandFields<C extends { readonly kind: string }> = {
  readonly [K in C['kind']]: readonly FieldName<Extract<C, { readonly kind: K }>>[];
};

/** The fields of some kind of `C` its list in `F` leaves out (never, when every list is whole). */
export type MissingFields<C extends { readonly kind: string }, F extends CommandFields<C>> = {
  [K in C['kind']]: Exclude<FieldName<Extract<C, { readonly kind: K }>>, F[K][number]>
}[C['kind']];

/** A field list's names as a command carries them (no `?`). */
export function fieldNames(listed: readonly string[]): string[] {
  return listed.map((f) => (f.endsWith('?') ? f.slice(0, -1) : f));
}
