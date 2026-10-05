// The side bar's SDLC panels, as upstream Studio's (census A §5, slice 2 §4): Review -- a workspace's way
// onto the project line (create, commit, close) -- and Project -- its versions (release major, minor or
// patch from the line's head) and its dependencies (published versions, picked from Depot).

import type { DepotClient } from '../../../depot-client/src/client.ts';
import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import { versionText, type NewVersionType, type Review } from '../../../sdlc-client/src/wire.ts';
import type { Workspace } from '../model/workspace.ts';
import { icon } from '../../../legend-art/src/icon.ts';
import { changesBetween, type ElementChange } from './diff.ts';
import { ago, clear, dialog, h, headerAction, sideHead, subPanel, toast } from './dom.ts';
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
  /** Shows a change's diff (diff.ts). */
  diff(change: ElementChange): void;
}

const message = (e: unknown): string => (e instanceof Error ? e.message : String(e));

/** The workspace's open review, as upstream Studio finds it (slice 2 §4.4), or undefined. */
async function openReview(ctx: PanelContext): Promise<Review | undefined> {
  const reviews = await ctx.client.reviews(ctx.project, { state: 'OPEN' });
  return reviews.find((r) => r.workspaceId === ctx.workspace && r.workspaceType === 'USER');
}

/**
 * upstream's Workspace Review (census 8.2): the header with Close review; with no review, the title field and a
 * square accent + to create it; with one, its title, a square accent merge button to commit it, and
 * "created {N} ago".
 */
export async function renderReview(root: HTMLElement, ctx: PanelContext): Promise<void> {
  clear(root);
  const body = h('div', { class: 'side-bar__body', 'data-testid': 'review-panel' });
  const review = await openReview(ctx);
  const closeReview = review === undefined ? undefined : async (): Promise<void> => {
    try {
      // upstream Studio's "Close review" calls reject (slice 2 section 4.5)
      await ctx.client.reviewAction(ctx.project, review.id, 'reject');
      await renderReview(root, ctx);
    } catch (e) {
      toast(message(e), 'error');
    }
  };
  root.append(sideHead('Review', headerAction('times', 'Close review', closeReview && (() => void closeReview()), { 'data-testid': 'close-review' })), body);
  if (!review) {
    const title = h('input', { class: 'input', placeholder: 'Title', 'data-testid': 'review-title' });
    body.append(h('div', { class: 'workspace-review__title' }, title,
      h('button', { class: 'btn btn-primary btn-square', title: 'Create review', 'data-testid': 'create-review', onclick: async () => {
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
      } }, icon('plus'))));
    return;
  }
  body.append(
    h('div', { class: 'workspace-review__title' },
      h('div', { class: 'workspace-review__title__content', title: `Review #${review.id} by ${review.author.name}` }, `#${review.id} ${review.title}`),
      h('button', { class: 'btn btn-primary btn-square', title: 'Commit review', 'data-testid': 'commit-review', onclick: async () => {
        if (ctx.ws.hasChanges()) return toast('Save your local changes first: they are not in the review.', 'error');
        try {
          await ctx.client.commitReview(ctx.project, review.id, `${review.title} [review]`);
          ctx.gone(`Review #${review.id} committed: ${ctx.workspace} is now on the project line (and the workspace is closed).`);
        } catch (e) {
          toast(message(e), 'error');
        }
      } }, icon('gitMergeShort', '17px'))),
    h('div', { class: 'workspace-review__status' }, `created ${ago(review.createdAt)} ago`));
  // upstream's approvals: who has approved, and this user's Approve / Revoke approval
  const [{ approvedBy }, me] = await Promise.all([ctx.client.approval(ctx.project, review.id), ctx.client.currentUser()]);
  const mine = approvedBy.some((u) => u.userId === me.userId);
  const approve = async (): Promise<void> => {
    try {
      await ctx.client.reviewAction(ctx.project, review.id, mine ? 'revokeApproval' : 'approve');
      await renderReview(root, ctx);
    } catch (e) {
      toast(message(e), 'error');
    }
  };
  body.append(h('div', { class: 'workspace-review__approvals', 'data-testid': 'review-approvals' },
    h('span', {}, approvedBy.length ? `Approved by ${approvedBy.map((u) => u.name).join(', ')}` : 'Not approved yet'),
    h('button', { class: 'btn btn-small', 'data-testid': 'approve-review', onclick: () => void approve() }, mine ? 'Revoke approval' : 'Approve')));
  // upstream's review CHANGES (plan A7): what the review brings -- the workspace's saved text against where it was made
  // from (BASE) -- each opening its diff
  const where = { project: ctx.project, workspace: ctx.workspace };
  const [base, head] = await Promise.all([ctx.client.pure({ ...where, revision: 'BASE' }), ctx.client.pure(where)]);
  const changes = changesBetween(new Map(base.map((f) => [f.path, f.pureCode])), new Map(head.map((f) => [f.path, f.pureCode])));
  const LETTER = { CREATE: 'N', MODIFY: 'M', DELETE: 'D' } as const;
  body.append(subPanel('Changes', { info: 'What the review brings onto the project line', count: changes.length, testId: 'review-changes' },
    ...(changes.length ? changes.map((c) => h('div', {
      class: `side-bar__panel__item diff-item diff-item--${c.type.toLowerCase()}`, title: c.path, 'data-path': c.path, onclick: () => ctx.diff(c),
    }, h('div', { class: 'diff-item__name' }, c.path.split('::').pop() ?? c.path), h('div', { class: 'diff-item__path' }, c.path),
    h('div', { class: 'diff-item__type' }, LETTER[c.type]))) : [h('div', { class: 'side-bar__panel__empty' }, 'No changes')])));
}

type ProjectTab = 'overview' | 'release' | 'versions' | 'history';
/** The Project view's tab, kept while the view is drawn again. */
let projectTab: ProjectTab = 'overview';

/**
 * upstream's Project Overview (census 8.3): a vertical tab rail -- OVERVIEW, RELEASE, VERSIONS -- and the tab.
 * OVERVIEW holds the project's coordinates and, lite's own, its dependencies (upstream edits them in the project
 * configuration editor, a tab); RELEASE the notes and MAJOR (caution) / MINOR / PATCH, then the latest release;
 * VERSIONS every version with its notes.
 */
export async function renderProject(root: HTMLElement, ctx: PanelContext): Promise<void> {
  clear(root);
  const [config, versions, line] = await Promise.all([
    ctx.ws.configuration ?? ctx.client.configuration({ project: ctx.project, workspace: ctx.workspace }),
    ctx.client.versions(ctx.project),
    ctx.client.revision({ project: ctx.project }),
  ]);
  const content = h('div', { class: 'project-overview__content', 'data-testid': 'project-panel' });
  const tab = (t: ProjectTab, label: string): HTMLElement => h('button', {
    class: `project-overview__tab${projectTab === t ? ' project-overview__tab--active' : ''}`, title: label, 'data-project-tab': t,
    onclick: () => { projectTab = t; void renderProject(root, ctx); },
  }, t.toUpperCase());
  root.append(sideHead('Project'), h('div', { class: 'project-overview' },
    h('div', { class: 'project-overview__activity-bar' }, tab('overview', 'Overview'), tab('release', 'Release'), tab('versions', 'Versions'), tab('history', 'History')),
    content));

  if (projectTab === 'history') {
    await renderHistory(content, ctx);
    return;
  }

  if (projectTab === 'overview') {
    const deps = config.projectDependencies.map((d) => h('div', { class: 'side-bar__panel__item' },
      h('div', { class: 'side-bar__panel__item__label' }, `${d.projectId} : ${d.versionId}`),
      h('button', { class: 'side-bar__panel__item__action', title: 'Remove dependency', onclick: async () => {
        try {
          await ctx.client.updateConfiguration(ctx.project, ctx.workspace, { message: `remove dependency ${d.projectId}`, projectDependenciesToRemove: [d] });
          await ctx.reload();
        } catch (e) {
          toast(message(e), 'error');
        }
      } }, icon('times'))));
    content.append(
      h('div', { class: 'side-bar__form' }, row('Project', ctx.project), row('Group id', config.groupId), row('Artifact id', config.artifactId)),
      subPanel('Dependencies', { info: 'The published versions this workspace depends on (project.json)', count: deps.length, testId: 'dependencies' },
        ...(deps.length ? deps : [h('div', { class: 'side-bar__panel__empty' }, 'No dependencies')]),
        h('button', { class: 'btn btn-small side-bar__panel__add', 'data-testid': 'add-dependency', onclick: () => void addDependency(ctx) }, 'Add Dependency')));
    return;
  }

  if (projectTab === 'versions') {
    content.append(subPanel('Versions', { count: versions.length, testId: 'versions' },
      ...(versions.length ? versions.map((v) => h('div', { class: 'side-bar__panel__item', title: 'See version' },
        h('div', { class: 'side-bar__panel__item__label' }, versionText(v.id)),
        h('div', { class: 'side-bar__panel__item__note' }, v.notes ?? ''))) : [h('div', { class: 'side-bar__panel__empty' }, 'This project has no version')])));
    return;
  }

  // release
  const released = versions[0]?.revisionId === line.id;
  const notes = h('textarea', { class: 'input project-overview__release__notes', placeholder: 'Release notes', 'data-testid': 'release-notes' });
  const release = (type: NewVersionType) => async (): Promise<void> => {
    try {
      const v = await ctx.client.createVersion(ctx.project, { versionType: type, revisionId: line.id, notes: notes.value.trim() || null });
      toast(`Released ${versionText(v.id)}`, 'success');
      await renderProject(root, ctx);
    } catch (e) {
      toast(message(e), 'error');
    }
  };
  const tips: Record<NewVersionType, string> = {
    MAJOR: 'Create a major release which comes with backward-incompatible features',
    MINOR: 'Create a minor release which comes with backward-compatible features',
    PATCH: 'Create a patch release which comes with backward-compatible bug fixes',
  };
  const latest = versions[0];
  content.append(
    h('div', { class: 'project-overview__release' }, notes,
      h('div', { class: 'project-overview__release__actions' }, ...(['MAJOR', 'MINOR', 'PATCH'] as const).map((t) => h('button', {
        class: `btn btn-primary project-overview__release__btn${t === 'MAJOR' ? ' btn-caution' : ''}`, title: released ? 'The project line\'s head is already released' : tips[t],
        'data-testid': `release-${t.toLowerCase()}`, disabled: released, onclick: release(t),
      }, t)))),
    subPanel('Latest Release', { testId: 'latest-release' }, latest
      ? h('div', { class: 'side-bar__panel__item', title: 'See version' },
        h('div', { class: 'side-bar__panel__item__label' }, versionText(latest.id)),
        h('div', { class: 'side-bar__panel__item__note' }, latest.notes ?? ''))
      : h('div', { class: 'side-bar__panel__empty' }, 'This project has no release')));
}

/**
 * The workspace's history (plan A7, lite's revision browser): its revisions newest first -- message, author, when --
 * and a revision chosen, what it changed against the one before it, each opening its diff.
 */
async function renderHistory(content: HTMLElement, ctx: PanelContext): Promise<void> {
  const where = { project: ctx.project, workspace: ctx.workspace };
  const revisions = await ctx.client.revisions(where);
  const changesOf = h('div', { 'data-testid': 'revision-changes' });
  const LETTER = { CREATE: 'N', MODIFY: 'M', DELETE: 'D' } as const;
  const choose = async (i: number): Promise<void> => {
    for (const [n, el] of rows.entries()) el.classList.toggle('side-bar__panel__item--selected', n === i);
    const r = revisions[i]!;
    const before = revisions[i + 1];
    const [now, then] = await Promise.all([
      ctx.client.pure({ ...where, revision: r.id }),
      before ? ctx.client.pure({ ...where, revision: before.id }) : Promise.resolve([]),
    ]);
    const changes = changesBetween(new Map(then.map((f) => [f.path, f.pureCode])), new Map(now.map((f) => [f.path, f.pureCode])));
    clear(changesOf);
    changesOf.append(subPanel(`Changes in ${r.id.slice(0, 8)}`, { count: changes.length },
      ...(changes.length ? changes.map((c) => h('div', {
        class: `side-bar__panel__item diff-item diff-item--${c.type.toLowerCase()}`, title: c.path, 'data-path': c.path, onclick: () => ctx.diff(c),
      }, h('div', { class: 'diff-item__name' }, c.path.split('::').pop() ?? c.path), h('div', { class: 'diff-item__path' }, c.path),
      h('div', { class: 'diff-item__type' }, LETTER[c.type]))) : [h('div', { class: 'side-bar__panel__empty' }, 'No element changed')])));
  };
  const rows = revisions.map((r, i) => h('div', { class: 'side-bar__panel__item revision-item', title: r.id, 'data-revision': r.id, onclick: () => void choose(i).catch((e: unknown) => toast(message(e), 'error')) },
    h('div', { class: 'side-bar__panel__item__label' }, r.message),
    h('div', { class: 'side-bar__panel__item__note' }, `${r.authorName}, ${ago(r.committedTimestamp)} ago`)));
  content.append(subPanel('Revisions', { info: 'The workspace\'s history, newest first', count: revisions.length, testId: 'revisions' }, ...rows), changesOf);
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
