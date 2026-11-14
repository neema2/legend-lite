// THE MARKETS EXAMPLE OVER THE TRADING MODEL'S CLASSES as a saved page (examples/markets-classes.ts marketsClassesPage,
// page-document.ts pageToJson), written to stdout: what Python's test of it loads (python/tests/test_markets.py), the
// page the screen's Open window opens.

import { marketsClassesPage } from '../../src/examples/markets-classes.ts';
import { pageToJson } from '../../src/page-document.ts';

process.stdout.write(pageToJson(marketsClassesPage()));
