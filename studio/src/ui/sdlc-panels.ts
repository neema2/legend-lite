// The side bar's SDLC panels: Review (a workspace's way onto the project line) and Project (its
// configuration and versions).

import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { clear, h } from './dom.ts';

export interface PanelContext {
  readonly client: SdlcClient;
  readonly project: string;
  readonly workspace: string;
}

export async function renderReview(root: HTMLElement, ctx: PanelContext & { hasChanges(): boolean; reload(): Promise<void> }): Promise<void> {
  clear(root);
  root.append(h('div', { class: 'side-head' }, h('span', {}, 'Review')),
    h('div', { class: 'side-empty' }, `Reviews of ${ctx.workspace} land here.`));
}

export async function renderProject(root: HTMLElement, ctx: PanelContext): Promise<void> {
  clear(root);
  const body = h('div', { class: 'side-body' });
  root.append(h('div', { class: 'side-head' }, h('span', {}, 'Project')), body);
  const config = await ctx.client.configuration({ project: ctx.project, workspace: ctx.workspace });
  body.append(
    row('Project', ctx.project),
    row('Group id', config.groupId),
    row('Artifact id', config.artifactId),
    row('Structure', String(config.projectStructureVersion.version)),
    row('Dependencies', config.projectDependencies.length === 0 ? 'none' : config.projectDependencies.map((d) => `${d.projectId}:${d.versionId}`).join(', ')));
}

export function row(label: string, value: string): HTMLElement {
  return h('div', { class: 'kv' }, h('span', { class: 'kv-key' }, label), h('span', { class: 'kv-value' }, value));
}
