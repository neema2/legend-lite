// Upstream's element-type icons (legend-art src/icon/TypeIcon.tsx, Studio's ElementIconUtils.tsx; census
// studio/docs/UPSTREAM_STUDIO_LOOK.md 3.3): the Raleway 900 letters for the model's core types, an icon for the
// rest, each in its type's colour (icons.css: `.color--<kind>`, upstream's _extensions.scss tokens).

import { icon } from './icon.ts';
import type { IconName } from './icons.ts';

/** The element kinds upstream draws, by the Pure keyword that declares each. */
export type ElementKind =
  | 'Class' | 'Enum' | 'Association' | 'Profile' | 'Measure' | 'function' | 'Mapping' | 'Runtime' | 'Database'
  | 'Service' | 'RelationalDatabaseConnection' | 'Data' | 'DataSpace' | 'Diagram' | 'FlatData' | 'GenerationSpecification'
  | 'FileGeneration';

/** [colour class, letter or icon, the icon's size when upstream overrides it] */
const TYPES: Record<ElementKind, readonly [string, string | IconName, string?]> = {
  Class: ['class', 'C'],
  Enum: ['enumeration', 'E'],
  Association: ['association', 'A'],
  Profile: ['profile', 'P'],
  Measure: ['measure', 'M'],
  function: ['function', 'function', '17px'],
  Mapping: ['mapping', 'map'],
  Runtime: ['runtime', 'businessTime'],
  Database: ['database', 'database'],
  Service: ['service', 'robot'],
  RelationalDatabaseConnection: ['connection', 'connection', '16px'],
  Data: ['data', 'dataFile'],
  DataSpace: ['data-space', 'unchecked'],
  Diagram: ['diagram', 'shapes'],
  FlatData: ['flat-data', 'layerGroup', '12px'],
  GenerationSpecification: ['generation-specification', 'G'],
  FileGeneration: ['file-generation', 'fileCode'],
};

const LETTERS = /^[A-Za-z]$/;

/**
 * `<div class="type-icon color--<kind>">`: the letter (Raleway 900) or the icon. An unknown kind is upstream's
 * QuestionSquareIcon, 15px in the disabled grey.
 */
export function typeIcon(kind: string | undefined): HTMLDivElement {
  const div = document.createElement('div');
  const t = kind !== undefined && Object.hasOwn(TYPES, kind) ? TYPES[kind as ElementKind] : undefined;
  if (t === undefined) {
    div.className = 'type-icon type-icon--unknown';
    div.append(icon('questionSquare', '15px'));
    return div;
  }
  const [color, glyph, size] = t;
  div.className = `type-icon color--${color}`;
  if (LETTERS.test(glyph)) {
    div.classList.add('type-icon--letter');
    div.textContent = glyph;
  } else {
    div.append(icon(glyph as IconName, size));
  }
  return div;
}
