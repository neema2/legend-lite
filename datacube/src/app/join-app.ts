// A JOIN IN THE TAB, fetched as one (docs/DATACUBE_MODEL_DESIGN_2026_10_10.md §17): the Join… flow and a join's grid
// opened, behind one import -- each lazy module that shares the page's start-up code splits it into more chunks, so the
// join's code is one (test/bundle-budget.test.ts).

export { hostJoinFlow, joinFlow } from './join-flow.ts';
export { openedJoin } from './join-sources.ts';
