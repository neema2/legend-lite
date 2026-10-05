// The side bar's SDLC panels, as upstream Studio's (census A §5, slice 2 §4): Review -- a workspace's way
// onto the project line (create, commit, close) -- and Project -- its versions (release major, minor or
// patch from the line's head) and its dependencies (published versions, picked from Depot).

import type { DepotClient } from '../../../depot-client/src/client.ts';
import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { versionText, type NewVersionType, type Review } from '../../../sdlc-client/src/wire.ts';
import type { Workspace } from '../model/workspace.ts';
import { clear, dialog, h, toast } from './dom.ts';
import { field } from './setup.ts';

export interface PanelContext {
  readonly client: SdlcClient;
  readonly depot: DepotClient;
  readonly project: string;
  readonly workspace: string;
  readonly ws: Workspace;
  /** Reads the workspace again (after its configuration changed). */
  reload(): Promise<void>;
  /** The workspace no longer exists (its review was committed). */
  gone(message: string): void;
}

const message = (e: unknown): string => (e instanceof Error ? e.message : String(e));

/** The workspace's open review, as upstream Studio finds it (slice 2 §4.4), or undefined. */
async function openReview(ctx: PanelContext): Promise<Review | undefined> {
  const reviews = await ctx.client.reviews(ctx.project, { state: 'OPEN' });
  return reviews.find((r) => r.workspaceId === ctx.workspace && r.workspaceType === 'USER');
}

export async function renderReview(root: HTMLElement, ctx: PanelContext): Promise<void> {
  clear(root);
  const body = h('div', { class: 'side-body', 'data-testid': 'review-panel' });
  root.append(h('div', { class: 'side-head' }, h('span', {}, 'Review')), body);
  const review = await openReview(ctx);
  if (!review) {
    const title = h('input', { class: 'input', placeholder: 'What this workspace changes', 'data-testid': 'review-title' });
    body.append(
      h('div', { class: 'hint' }, 'A review lands this workspace on the project line. Save your changes first.'),
      field('Title', title),
      h('button', { class: 'btn btn-primary', 'data-testid': 'create-review', onclick: async () => {
        if (ctx.ws.hasChanges()) return toast('Save your local changes first: a review takes what is saved.', 'error');
        if (title.value.trim() === '') return toast('A review needs a title.', 'error');
        try {
          await ctx.client.createReview(ctx.project, {
            workspaceId: ctx.workspace, workspaceType: 'USER', title: title.value.trim(),
            description: `review from Legend Studio for workspace ${ctx.workspace}`,
          });
          await renderReview(root, ctx);
        } catch (e) {
          toast(message(e), 'error');
        }
      } }, 'Create review'));
    return;
  }
  body.append(
    h('div', { class: 'review-title' }, `#${review.id} ${review.title}`),
    row('State', review.state),
    row('Author', review.author.name),
    row('Created', new Date(review.createdAt).toLocaleString()),
    h('div', { class: 'button-row' },
      h('button', { class: 'btn btn-primary', 'data-testid': 'commit-review', onclick: async () => {
        if (ctx.ws.hasChanges()) return toast('Save your local changes first: they are not in the review.', 'error');
        try {
          await ctx.client.commitReview(ctx.project, review.id, `${review.title} [review]`);
          ctx.gone(`Review #${review.id} committed: ${ctx.workspace} is now on the project line (and the workspace is closed).`);
        } catch (e) {
          toast(message(e), 'error');
        }
      } }, 'Commit'),
      h('button', { class: 'btn', 'data-testid': 'close-review', onclick: async () => {
        try {
          // upstream Studio's "Close review" calls reject (slice 2 §4.5)
          await ctx.client.reviewAction(ctx.project, review.id, 'reject');
          await renderReview(root, ctx);
        } catch (e) {
          toast(message(e), 'error');
        }
      } }, 'Close')));
}

export async function renderProject(root: HTMLElement, ctx: PanelContext): Promise<void> {
  clear(root);
  const body = h('div', { class: 'side-body', 'data-testid': 'project-panel' });
  root.append(h('div', { class: 'side-head' }, h('span', {}, 'Project')), body);
  const config = ctx.ws.configuration ?? await ctx.client.configuration({ project: ctx.project, workspace: ctx.workspace });
  body.append(row('Project', ctx.project), row('Group id', config.groupId), row('Artifact id', config.artifactId));

  // ---- dependencies (this workspace's project.json) ----
  const deps = h('div', { class: 'deps', 'data-testid': 'dependencies' });
  for (const d of config.projectDependencies) {
    deps.append(h('div', { class: 'dep' },
      h('span', { class: 'dep-name' }, `${d.projectId} : ${d.versionId}`),
      h('button', { class: 'btn btn-small', title: 'Remove', onclick: async () => {
        try {
          await ctx.client.updateConfiguration(ctx.project, ctx.workspace, { message: `remove dependency ${d.projectId}`, projectDependenciesToRemove: [d] });
          await ctx.reload();
        } catch (e) {
          toast(message(e), 'error');
        }
      } }, '−')));
  }
  if (config.projectDependencies.length === 0) deps.append(h('div', { class: 'hint' }, 'No dependencies.'));
  body.append(h('div', { class: 'section-title' }, 'Dependencies'), deps,
    h('button', { class: 'btn btn-small', 'data-testid': 'add-dependency', onclick: () => void addDependency(ctx) }, '+ Add dependency'));

  // ---- versions (the project line) ----
  const versionsEl = h('div', { class: 'deps', 'data-testid': 'versions' });
  body.append(h('div', { class: 'section-title' }, 'Versions'), versionsEl);
  const [versions, line] = await Promise.all([ctx.client.versions(ctx.project), ctx.client.revision({ project: ctx.project })]);
  for (const v of versions) {
    versionsEl.append(h('div', { class: 'dep' }, h('span', { class: 'dep-name version' }, versionText(v.id)), h('span', { class: 'hint' }, v.notes ?? '')));
  }
  if (versions.length === 0) versionsEl.append(h('div', { class: 'hint' }, 'No version yet.'));
  const released = versions[0]?.revisionId === line.id;
  const notes = h('input', { class: 'input', placeholder: 'Release notes', 'data-testid': 'release-notes' });
  const release = (type: NewVersionType) => async (): Promise<void> => {
    try {
      const v = await ctx.client.createVersion(ctx.project, { versionType: type, revisionId: line.id, notes: notes.value.trim() || null });
      toast(`Released ${versionText(v.id)}`, 'success');
      await renderProject(root, ctx);
    } catch (e) {
      toast(message(e), 'error');
    }
  };
  body.append(
    h('div', { class: 'hint' }, released
      ? 'The project line\'s head is already released.'
      : `Release the project line's head (${line.id.slice(0, 8)}): only a revision that compiles is released.`),
    field('Notes', notes),
    h('div', { class: 'button-row' },
      ...(['MAJOR', 'MINOR', 'PATCH'] as const).map((t) => h('button', {
        class: 'btn btn-small', 'data-testid': `release-${t.toLowerCase()}`, disabled: released, onclick: release(t),
      }, t[0] + t.slice(1).toLowerCase()))));
}

async function addDependency(ctx: PanelContext): Promise<void> {
  const projects = (await ctx.depot.projects()).filter((p) => p.projectId !== ctx.project && p.latestVersion !== null);
  if (projects.length === 0) {
    toast('No other project has a version yet: release one first.', 'error');
    return;
  }
  const project = h('select', { class: 'input', 'data-testid': 'dependency-project' }, ...projects.map((p) => h('option', { value: p.projectId }, p.projectId)));
  const version = h('select', { class: 'input', 'data-testid': 'dependency-version' });
  const fill = async (): Promise<void> => {
    const p = projects.find((x) => x.projectId === project.value)!;
    const versions = (await ctx.depot.versions(p.groupId, p.artifactId, false)).reverse();
    clear(version);
    for (const v of versions) version.append(h('option', { value: v }, v));
  };
  project.addEventListener('change', () => void fill());
  await fill();
  const chosen = await dialog('Add dependency', h('div', { class: 'form' }, field('Project', project), field('Version', version)),
    () => (version.value === '' ? 'Pick a version.' : { ok: { projectId: project.value, versionId: version.value } }), 'Add');
  if (!chosen) return;
  try {
    await ctx.client.updateConfiguration(ctx.project, ctx.workspace, { message: `depend on ${chosen.projectId}:${chosen.versionId}`, projectDependenciesToAdd: [chosen] });
    await ctx.reload();
  } catch (e) {
    toast(message(e), 'error');
  }
}

export function row(label: string, value: string): HTMLElement {
  return h('div', { class: 'kv' }, h('span', { class: 'kv-key' }, label), h('span', { class: 'kv-value' }, value));
}
