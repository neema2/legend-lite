// THE MODEL'S WRITER, as DataCube asks it (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §15.6a): legend-lite's
// createClass, createAssociation and createDataSpace -- core's ModelChanges, each one call and atomic, asked of the host's
// writer (the tab's module, Python's native library). Their wire is the generic library's (model-changes/src/wire.ts,
// docs/ONE_PLATFORM_PROGRAM_2026_10_12.md B4), not DataCube's; DataCube adds only a planner over a project.

import type { ModelWriter as Writer } from '../../model-changes/src/wire.ts';
import type { Planner } from './planner-types.ts';

export type {
  AssociationAction, AssociationEnd, ClassAction, ColumnChoice, ColumnRole, DataSpaceAction, Joinable, MadeAssociation,
  MadeClass, MadeProperty, ModelAnswer, SampleRows, Source as GridSource, Suggestion,
} from '../../model-changes/src/wire.ts';

/** The model's writer as DataCube's model actions ask it: the library's three routes, and a planner over a project's
 *  files, where the facts an association's how-many is preset from are planned. */
export interface ModelWriter extends Writer {
  withModel(model: string, runtime: string): Planner;
}

/** Whether `x` writes the model. */
export function writesModels(x: object): x is ModelWriter {
  return 'createClass' in x && 'createAssociation' in x && 'createDataSpace' in x && 'withModel' in x;
}
