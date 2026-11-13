// JSON READ AS UNKNOWN (the total wall's C final audit, H5, 2026-11-12): what JSON.parse and a response's json() return
// is data no type vouches for, and `any` let it satisfy any type -- a plan's statement (PlannedSql) or a record holding
// one among them -- with no cast to see. Merged into the global declarations (as ts-reset does), these overloads come
// first, so a parsed value is `unknown` until a reader checks it, or a cast (which the guards count) says what it is.
// Types alone: nothing imports this file; each tsconfig that reads the product's TypeScript includes it.

declare global {
  interface JSON {
    parse(text: string, reviver?: (this: unknown, key: string, value: unknown) => unknown): unknown;
  }
  interface Body {
    json(): Promise<unknown>;
  }
  // and a value found to be an array is an array of unknowns, never of `any` (the re-audit's S6, 2026-11-12: the lib's
  // `arg is any[]` made each element any, with no cast to see)
  interface ArrayConstructor {
    isArray(arg: unknown): arg is unknown[];
  }
}

export {};
