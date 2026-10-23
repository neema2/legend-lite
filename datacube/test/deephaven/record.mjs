// THE DEMO PAGES RECORDED TICKING (`bazel test //datacube:deephaven_record`, manual; a hand tool, not a check): the
// cube page with a desk open and the page of grids and charts, each eight seconds after it is live, through Chrome's
// own screencast -- each frame a JPEG, and `frames.txt` their times, under the test's outputs (<page>/), for ffmpeg:
//   ffmpeg -f concat -safe 0 -i frames.txt -vf fps=15 -pix_fmt yuv420p ticking.mp4
// (Playwright's own video needs an ffmpeg of its own this build does not fetch.)
import '../../../tools/browser/pinned-chromium.mjs';
import { mkdirSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { chromium } from 'playwright';
import { serveDemo, startDeephaven } from '../../demo/deephaven/site.mjs';

const outputs = process.env.TEST_UNDECLARED_OUTPUTS_DIR ?? '.';
const server = startDeephaven({ quiet: true });
const config = await server.next('on starting');
const site = await serveDemo(config);
const browser = await chromium.launch();
const size = { width: 1600, height: 900 };
try {
  for (const [name, path, global] of [['cube', '/', '__cube'], ['board', '/board.html', '__board']]) {
    const context = await browser.newContext({ ignoreHTTPSErrors: true, viewport: size });
    const page = await context.newPage();
    await page.goto(`${site.url}${path}`);
    await page.waitForFunction((g) => window[g]?.status === 'live', global, { timeout: 120_000 });
    if (name === 'cube') await page.evaluate(() => window.__cube.change('expand'));
    await page.waitForTimeout(2500);
    const dir = join(outputs, name);
    mkdirSync(dir, { recursive: true });
    const cdp = await context.newCDPSession(page);
    const frames = [];
    cdp.on('Page.screencastFrame', (f) => {
      frames.push({ data: f.data, at: f.metadata.timestamp });
      cdp.send('Page.screencastFrameAck', { sessionId: f.sessionId }).catch(() => {});
    });
    await cdp.send('Page.startScreencast', { format: 'jpeg', quality: 85, maxWidth: size.width, maxHeight: size.height });
    await page.waitForTimeout(8000);
    await cdp.send('Page.stopScreencast');
    const list = [];
    frames.forEach((f, i) => {
      const file = `f${String(i).padStart(4, '0')}.jpg`;
      writeFileSync(join(dir, file), Buffer.from(f.data, 'base64'));
      const next = frames[i + 1];
      list.push(`file '${file}'`, `duration ${next ? (next.at - f.at).toFixed(3) : '0.1'}`);
    });
    writeFileSync(join(dir, 'frames.txt'), `${list.join('\n')}\n`);
    console.log(`${name}: ${frames.length} frames over ${frames.length > 1 ? (frames.at(-1).at - frames[0].at).toFixed(1) : 0} s`);
    await context.close();
  }
} finally {
  await browser.close();
  site.close();
  await server.stop();
}
