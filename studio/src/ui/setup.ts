// Workspace setup (upstream Studio's first screen): pick or create a project, then pick or create a
// workspace in it, then open the editor.

import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import type { Project } from '../../../sdlc-client/src/wire.ts';
import { icon } from '../../../legend-art/src/icon.ts';
import { clear, dialog, h, toast } from './dom.ts';
import { selector } from './selector.ts';
import { theme, toggleTheme } from './theme.ts';

export interface SetupContext {
  readonly client: SdlcClient;
  /** Where the projects live, as a person reads it ("this browser", a server URL). */
  readonly where: string;
  open(project: string, workspace: string): void;
  /** Publishes the demo projects (design S18), reporting each step. */
  loadDemo?(progress: (message: string) => void): Promise<void>;
}

/**
 * upstream's workspace setup page (census 1): the slim activity bar (the theme switch), and the big card --
 * "Welcome to Legend Studio", the project search and the workspace choice (each behind its icon cell), "Need to
 * create a new workspace?", Go, OR, Create New Project -- over the blue status bar. Lite's own: "Load demo
 * projects" beside the project heading, and where the projects live, in the status bar.
 */
export async function renderSetup(root: HTMLElement, ctx: SetupContext, selected?: string): Promise<void> {
  clear(root);
  let project: string | undefined;
  let workspace: string | undefined;

  const go = h('button', { class: 'btn btn-primary workspace-setup__go-btn', 'data-testid': 'go', disabled: true, onclick: () => {
    if (project && workspace) ctx.open(project, workspace);
  } }, h('span', { class: 'workspace-setup__go-btn__label' }, 'Go'), icon('longArrowRight'));
  const newWorkspaceLink = h('button', { class: 'workspace-setup__new-workspace-btn', 'data-testid': 'new-workspace', disabled: true,
    title: 'Create a workspace after choosing a project', onclick: () => void newWorkspace() }, 'Need to create a new workspace?');
  const workspaces = selector('workspace-selector', (w) => {
    workspace = w;
    go.disabled = !(project && workspace);
  });
  const projects = selector('project-selector', (p) => {
    project = p;
    workspace = undefined;
    go.disabled = true;
    newWorkspaceLink.disabled = project === undefined;
    void showWorkspaces();
  });

  const showWorkspaces = async (): Promise<void> => {
    if (!project) {
      workspaces.reset([], 'In order to choose a workspace, a project must be chosen', true);
      return;
    }
    workspaces.reset([], 'Loading workspaces...', true);
    const found = await ctx.client.workspaces(project);
    workspaces.reset(found.map((w) => ({ value: w.workspaceId, label: w.workspaceId, detail: w.userId ?? 'group' })),
      found.length ? 'Choose an existing workspace' : 'You have no workspaces. Please create one to proceed...', found.length === 0);
  };

  const showProjects = async (choose?: string): Promise<void> => {
    const found = await ctx.client.projects();
    projects.reset(found.map((p: Project) => ({ value: p.projectId, label: p.name, detail: p.projectId })), 'Search for project...');
    if (choose && found.some((p) => p.projectId === choose)) projects.choose(choose);
    else await showWorkspaces();
  };

  const newProject = async (): Promise<void> => {
    const name = h('input', { class: 'input', placeholder: 'Trading' });
    const groupId = h('input', { class: 'input', placeholder: 'org.example' });
    const artifactId = h('input', { class: 'input', placeholder: 'trading' });
    const description = h('input', { class: 'input', placeholder: 'optional' });
    const created = await dialog('Create project', h('div', { class: 'form' },
      field('Name', name), field('Group id', groupId), field('Artifact id', artifactId), field('Description', description)),
    () => (name.value.trim() === '' ? 'A project needs a name.' : { ok: {
      name: name.value.trim(), groupId: groupId.value.trim(), artifactId: artifactId.value.trim(), description: description.value,
    } }), 'Create');
    if (!created) return;
    try {
      const p = await ctx.client.createProject(created);
      await showProjects(p.projectId);
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'error');
    }
  };

  const newWorkspace = async (): Promise<void> => {
    const current = project;
    if (!current) return;
    const id = h('input', { class: 'input', placeholder: 'my-change' });
    const made = await dialog('Create workspace', h('div', { class: 'form' }, field('Workspace id', id)),
      () => (id.value.trim() === '' ? 'A workspace needs an id.' : { ok: id.value.trim() }), 'Create');
    if (!made) return;
    try {
      await ctx.client.createWorkspace(current, made);
      ctx.open(current, made);
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'error');
    }
  };

  const status = h('span', { class: 'workspace-setup__demo-status', 'data-testid': 'demo-status' });
  const loadDemo = async (): Promise<void> => {
    if (!ctx.loadDemo) return;
    try {
      await ctx.loadDemo((m) => { status.textContent = `Publishing ${m}…`; });
      status.textContent = 'Demo projects published.';
      await showProjects(project);
    } catch (e) {
      status.textContent = '';
      toast(e instanceof Error ? e.message : String(e), 'error');
    }
  };

  const dark = theme() === 'dark';
  const themeToggle = h('button', { class: 'activity-bar__item', title: dark ? 'Switch to light theme' : 'Switch to dark theme', 'data-testid': 'theme-toggle',
    onclick: () => { toggleTheme(); void renderSetup(root, ctx, project); } }, icon(dark ? 'sun' : 'moon', '20px'));
  const selectorRow = (glyph: 'search' | 'gitBranch', title: string, s: { el: HTMLElement }): HTMLElement =>
    h('div', { class: 'workspace-setup__selector__content' }, h('div', { class: 'workspace-setup__selector__icon', title }, icon(glyph)), s.el);

  root.append(h('div', { class: 'workspace-setup' },
    h('div', { class: 'workspace-setup__body' },
      h('div', { class: 'activity-bar' }, h('div', { class: 'activity-bar__items' }), themeToggle),
      h('div', { class: 'workspace-setup__content' },
        h('div', { class: 'workspace-setup__content__body' },
          h('div', { class: 'workspace-setup__content__main' },
            h('div', { class: 'workspace-setup__title' },
              h('div', { class: 'workspace-setup__logo' }, h('div', { class: 'workspace-setup__logo__icon' }, 'L')),
              h('div', { class: 'workspace-setup__title__header' }, 'Welcome to Legend Studio')),
            h('div', { class: 'workspace-setup__selectors' },
              h('div', { class: 'workspace-setup__selector' },
                h('div', { class: 'workspace-setup__selector__header' }, 'Search for an existing project',
                  ctx.loadDemo ? h('div', { class: 'workspace-setup__selector__header__aside' }, status,
                    h('button', { class: 'workspace-setup__text-btn', 'data-testid': 'load-demo', title: 'Publish the demo projects (types, party, instruments, trading) into these projects',
                      onclick: () => void loadDemo() }, 'Load demo projects')) : null),
                selectorRow('search', 'project', projects)),
              h('div', { class: 'workspace-setup__selector' },
                h('div', { class: 'workspace-setup__selector__header' }, 'Choose an existing workspace'),
                selectorRow('gitBranch', 'workspace', workspaces))),
            h('div', { class: 'workspace-setup__actions' },
              newWorkspaceLink,
              h('div', { class: 'workspace-setup__actions__button' }, go),
              h('div', { class: 'divider-with-text' }, h('div', { class: 'divider-with-text__line' }), h('div', { class: 'divider-with-text__text' }, 'OR'), h('div', { class: 'divider-with-text__line' })),
              h('div', { class: 'workspace-setup__actions__button' },
                h('button', { class: 'btn btn-primary workspace-setup__new-btn', 'data-testid': 'new-project', title: 'Create a Project', onclick: () => void newProject() }, 'Create New Project'))))))),
    h('div', { class: 'status-bar' }, h('div', { class: 'status-bar__left' }, `Projects in ${ctx.where}`), h('div', { class: 'status-bar__right' }))));
  await showProjects(selected);
}

export function field(label: string, input: HTMLElement): HTMLElement {
  return h('label', { class: 'field' }, h('span', { class: 'field-label' }, label), input);
}
