// DataCube, embedded: the ONE entry an app showing a cube imports (Query's results are a CubeApp). An
// embedding app reaches nothing else in datacube/src -- so DataCube can change its insides without
// breaking it, and what it promises is this list. Where queries run is engine-client/'s, not DataCube's.

export { CubeApp, type CubeAppOptions } from './app.ts';
export type { CubeView, Planner } from './cube.ts';
export { RemoteRun } from './runner.ts';
export type { CubeSnapshot } from './snapshot.ts';
export { sourceColumns } from './source-columns.ts';
