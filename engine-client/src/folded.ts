// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

// THE PLANNER'S ANSWERS, READ ONCE (H4's P3, the user's CP-13 of 2026-11-19): every export of the planner module answers
// in the one text encoding both strings-only hosts speak (core's planner.Folded, written once in Java) -- `OK\n` and the
// answer, a refusal as `ERR\n` and lite's error `{kind, message, where?, raise?}` (lite's API v1 spec §3), an HTTP
// answer as `OK\n<status>\n<media type>\n<body>` -- and this module is the tab's one reader of it, and its one writer of
// a folded answer a host gives in the planner's place (a planner's worker answering from the host's routes). Until then
// three readers (wasm-grammar.ts's, DataCube's lite-planner.ts's, which kept the message alone, and the edge's) and three
// hand writers (DataCube's lite-routes.ts, lite-server.ts, python-host.ts). A record: it plans nothing and sends nothing.

import { liteError, LiteRefusal, type LiteError } from './lite-error.ts';

/** An export's folded answer read: its value, its refusal (lite's one shape, as the planner said it), or -- an answer in
 *  neither form -- what is wrong with it, said as a defect. The one reading every reader of the tab's is (`unfold`,
 *  DataCube's planner's). */
export type Unfolded =
  | { readonly answer: string }
  | { readonly refusal: LiteError }
  | { readonly malformed: string };

export function readFolded(answer: string): Unfolded {
  if (answer.startsWith('OK\n')) return { answer: answer.slice(3) };
  if (answer.startsWith('ERR\n')) {
    try {
      return { refusal: liteError(JSON.parse(answer.slice(4))) };
    } catch (e) {
      return { malformed: `the planner answered a refusal in no form it has (${(e as Error).message})` };
    }
  }
  return { malformed: `the planner answered in no form it has: ${JSON.stringify(answer.slice(0, 120))}` };
}

/** An export's folded answer as its value, or its refusal thrown as a LiteRefusal (lite's one shape, its kind, words,
 *  where and raise as the planner said them). An answer in neither form is a defect, said as one (kind internal). */
export function unfold(answer: string): string {
  const read = readFolded(answer);
  if ('answer' in read) return read.answer;
  if ('refusal' in read) throw new LiteRefusal(read.refusal);
  throw new LiteRefusal({ kind: 'internal', message: read.malformed });
}

/** An export's folded HTTP answer (planner.Folded.http): its status, media type and body; a refusal in its place
 *  thrown as `unfold` throws it, and an answer in no HTTP form a defect, said as one (kind internal). */
export function unfoldHttp(folded: string): { readonly status: number; readonly type: string; readonly body: string } {
  const answer = unfold(folded);
  const first = answer.indexOf('\n');
  const second = answer.indexOf('\n', first + 1);
  if (first < 0 || second < 0 || !/^[0-9]+$/.test(answer.slice(0, first))) {
    throw new LiteRefusal({ kind: 'internal', message: `the planner answered an HTTP call in no form it has: ${JSON.stringify(answer.slice(0, 120))}` });
  }
  return { status: Number(answer.slice(0, first)), type: answer.slice(first + 1, second), body: answer.slice(second + 1) };
}

/** `answer` folded as an export folds its answer: what a host answers in the planner module's place. */
export function folded(answer: string): string {
  return `OK\n${answer}`;
}

/** `error` folded as an export folds a refusal (lite's one shape, its keys in the planner's order): what a host
 *  answers in the planner module's place, read by `unfold` as the planner's own. */
export function foldedRefusal(error: LiteError): string {
  return `ERR\n${JSON.stringify({
    kind: error.kind,
    message: error.message,
    ...(error.where !== undefined ? { where: error.where } : {}),
    ...(error.raise !== undefined ? { raise: error.raise } : {}),
  })}`;
}
