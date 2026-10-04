// Workspace setup (upstream Studio's first screen): pick or create a project, then pick or create a
// workspace in it, then open the editor.

import type { SdlcClient } from '../../../sdlc-client/src/client.ts';
import type { Project } from '../../../sdlc-client/src/wire.ts';
import { clear, dialog, h, toast } from './dom.ts';

export interface SetupContext {
  readonly client: SdlcClient;
  /** Where the projects live, as a person reads it ("this browser", a server URL). */
  readonly where: string;
  open(project: string, workspace: string): void;
}

export async function renderSetup(root: HTMLElement, ctx: SetupContext, selected?: string): Promise<void> {
  clear(root);
  const projectsList = h('div', { class: 'setup-list', 'data-testid': 'projects' });
  const workspacesList = h('div', { class: 'setup-list', 'data-testid': 'workspaces' });
  const workspacesTitle = h('div', { class: 'setup-col-title' }, 'Workspaces');
  let current = selected;

  const showWorkspaces = async (project: string): Promise<void> => {
    current = project;
    for (const el of projectsList.querySelectorAll('.setup-item')) el.classList.toggle('selected', el.getAttribute('data-id') === project);
    clear(workspacesList);
    workspacesTitle.textContent = `Workspaces of ${project}`;
    const workspaces = await ctx.client.workspaces(project);
    if (workspaces.length === 0) workspacesList.append(h('div', { class: 'setup-empty' }, 'No workspace yet: create one to start editing.'));
    for (const w of workspaces) {
      workspacesList.append(h('button', { class: 'setup-item', 'data-id': w.workspaceId, onclick: () => ctx.open(project, w.workspaceId) },
        h('span', { class: 'setup-item-name' }, w.workspaceId),
        h('span', { class: 'setup-item-sub' }, w.userId ?? 'group')));
    }
  };

  const showProjects = async (): Promise<void> => {
    clear(projectsList);
    const projects = await ctx.client.projects();
    if (projects.length === 0) projectsList.append(h('div', { class: 'setup-empty' }, 'No project yet: create one.'));
    for (const p of projects) projectsList.append(projectItem(p, () => void showWorkspaces(p.projectId)));
    if (current) await showWorkspaces(current);
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
      current = p.projectId;
      await showProjects();
    } catch (e) {
      toast(e instanceof Error ? e.message : String(e), 'error');
    }
  };

  const newWorkspace = async (): Promise<void> => {
    if (!current) {
      toast('Pick a project first.', 'error');
      return;
    }
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

  root.append(h('div', { class: 'setup' },
    h('div', { class: 'setup-header' },
      h('div', { class: 'brand' }, h('span', { class: 'brand-mark' }, 'L'), 'Legend Studio'),
      h('div', { class: 'setup-where' }, `Projects in ${ctx.where}`)),
    h('div', { class: 'setup-cols' },
      h('div', { class: 'setup-col' },
        h('div', { class: 'setup-col-head' }, h('div', { class: 'setup-col-title' }, 'Projects'),
          h('button', { class: 'btn btn-primary', onclick: () => void newProject(), 'data-testid': 'new-project' }, 'New project')),
        projectsList),
      h('div', { class: 'setup-col' },
        h('div', { class: 'setup-col-head' }, workspacesTitle,
          h('button', { class: 'btn btn-primary', onclick: () => void newWorkspace(), 'data-testid': 'new-workspace' }, 'New workspace')),
        workspacesList))));
  await showProjects();
}

function projectItem(p: Project, onclick: () => void): HTMLElement {
  return h('button', { class: 'setup-item', 'data-id': p.projectId, onclick },
    h('span', { class: 'setup-item-name' }, p.name),
    h('span', { class: 'setup-item-sub' }, p.projectId));
}

export function field(label: string, input: HTMLElement): HTMLElement {
  return h('label', { class: 'field' }, h('span', { class: 'field-label' }, label), input);
}
