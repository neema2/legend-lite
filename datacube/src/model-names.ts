// THE NAMES DataCube OFFERS for the model's elements (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.1), before a person
// changes them, and the rules a name the person gives is held to. Read by the windows alone (create-class.ts,
// model-actions.ts and theirs), so the page's startup carries none of it. Where a class's rows are -- the host's
// database, its place in the class's model (`Browser`, `Python`) -- is the host's to say (services.ts ModelWriting).

import { isIdentifier } from '../../pure-protocol/src/index.ts';

/**
 * The class a source's name suggests: its file's name without the extension, in upper camel case, singular --
 * `trades.csv` a `Trade`, `order_lines.parquet` an `OrderLine`. A suggestion only: the person names it.
 */
export function suggestedClassName(sourceName: string): string {
  const base = sourceName.replace(/\.[A-Za-z0-9]+$/, '');
  const words = base.split(/[^A-Za-z0-9]+/).filter((w) => w !== '');
  if (words.length > 0) words[words.length - 1] = singular(words[words.length - 1]!);
  const name = words.map((w) => w.charAt(0).toUpperCase() + (w === w.toUpperCase() ? w.slice(1).toLowerCase() : w.slice(1))).join('');
  if (name === '') return 'Item';
  return /^[0-9]/.test(name) ? `_${name}` : name;
}

/** An English plural's singular, for the common endings; anything else as it is. */
function singular(word: string): string {
  const lower = word.toLowerCase();
  if (/(ss|us|is)$/.test(lower)) return word;
  if (lower.endsWith('ies') && word.length > 3) return `${word.slice(0, -3)}${word.slice(-3) === 'IES' ? 'Y' : 'y'}`;
  if (/(xes|ches|shes|sses)$/.test(lower)) return word.slice(0, -2);
  if (lower.endsWith('s') && word.length > 1) return word.slice(0, -1);
  return word;
}

/**
 * The package a page's name suggests for its project's elements: its words in lower camel case (`Trades by desk`
 * `tradesByDesk`), a Pure name; `datacube` when the name has none.
 */
export function suggestedPackage(pageName: string): string {
  const words = pageName.split(/[^A-Za-z0-9]+/).filter((w) => w !== '');
  const name = words.map((w, i) => {
    const rest = w === w.toUpperCase() ? w.slice(1).toLowerCase() : w.slice(1);
    return (i === 0 ? w.charAt(0).toLowerCase() : w.charAt(0).toUpperCase()) + rest;
  }).join('');
  if (name === '') return 'datacube';
  const led = /^[0-9]/.test(name) ? `_${name}` : name;
  // a page called "True" suggests no package Pure reads as a literal (isPackage would refuse it): `true_`
  return isPureName(led) ? led : `${led}_`;
}

/** The data space a page's name suggests: its words in upper camel case (`Trades by desk` `TradesByDesk`). */
export function suggestedDataSpaceName(pageName: string): string {
  const pkg = suggestedPackage(pageName);
  return pkg.charAt(0).toUpperCase() + pkg.slice(1);
}

/** A Pure name: letters, digits and `_`, not led by a digit, and no word Pure reads as a literal (pure-protocol's rule). */
export function isPureName(name: string): boolean {
  return isIdentifier(name);
}

/** A package: Pure names joined by `::`. */
export function isPackage(name: string): boolean {
  return name.split('::').every(isPureName);
}
