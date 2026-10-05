# Legend Studio: upstream look-and-feel census for a lite rebuild

What upstream Legend Studio looks like, region by region, so that a plain DOM + CSS rebuild (no React) can match it.
The source is finos/legend-studio at commit `821c74c`, checked out at `.scratch/legend-studio`. Behaviour is covered in
`UPSTREAM_STUDIO_CENSUS.md`, Parts A and B, and is not repeated here.

**Path abbreviations used in citations** (all are relative to `.scratch/legend-studio/packages/`):

| Abbrev | Path |
|---|---|
| `LA` | `legend-art` (design system: `style/**`, `src/**`, `scss/_mixins.scss`) |
| `LS` | `legend-application-studio` (`style/components/**`, `src/components/**`, `src/stores/**`) |
| `LAPP` | `legend-application` (app chrome: notifications, alerts, theme registry) |
| `LL` | `legend-lego` (tab manager, code-editor CSS, activity-bar badge, doc link) |
| `LCE` | `legend-code-editor` (Monaco theme, Pure tokenizer, editor options) |
| `LSH` | `legend-shared` (string formatters) |
| `LQB` | `legend-query-builder` (one borrowed style: the "Advanced" dropdown) |

**Units.** `html { font-size: 62.5% }` (LA/style/normalize.scss:48), so **1rem = 10px** everywhere below. Sizes are given in
rem as written in the source, with px in parentheses where that helps.

**Token naming.** Studio's SCSS uses legend-art's *semantic* tokens (`--color-bg-app` etc.). These are the same names our
Query app already defines at the top of `query/src/ui/app.css`, and they have the **same dark values**. Studio also
reaches for a few tokens that Query does not have yet; they are listed in §0.3. Every hex value below is for the
**default dark theme** unless it says otherwise.

---

## 0. Global foundations

### 0.1 Themes: dark by default, with a light toggle

- **Studio is not dark-only.** The default is `default-dark` (LAPP/src/__lib__/LegendApplicationColorTheme.ts:27-38;
  LAPP/src/stores/LayoutService.ts:97-101, which falls back to DEFAULT_DARK). `Core_LegendApplicationPlugin` registers
  `default-light`, `legacy-light`, `hc-light` and `hc-dark` as well (LAPP/src/stores/Core_LegendApplicationPlugin.ts:59-66).
- Studio exposes **only dark ↔ default-light**, through a sun/moon button in the activity bar
  (`STUDIO_SUPPORTED_COLOR_THEMES`, LS/src/components/editor/ActivityBar.tsx:69-72 and 123-162). The choice is persisted
  as a setting.
- The theme class is added to `document.body` as `theme__default-dark` or `theme__default-light`
  (LayoutService.ts:146-156). A comment in the SCSS says `<html>`, but the code uses `body`.
- **Studio's light theme is `default-light`, not `legacy-light`.** Our Query light theme is `legacy-light`, which has
  different values. Studio's light values are in LA/style/base/themes/_theme-default-light.scss:33-104, and the file
  itself calls them an "initial pass … not through a design/contrast audit". The resolved values are in the table in §0.3.
- When the light theme is on, `TEMPORARY__isLightColorThemeEnabled` drops the `darkMode`/`--dark` modifier classes from
  modals and selectors (for example LS/src/components/workspace-setup/WorkspaceSetup.tsx:629-630). Since the modifiers
  are now aliases of the semantic tokens, the only visual difference is that the token values flip.
- The code editor follows the app theme: dark uses `default-dark`, light uses `github-light`
  (LCE/src/CodeEditorTheme.ts:76-81).

### 0.2 Fonts, base size, reset

| What | Value | Cite |
|---|---|---|
| UI font | `Roboto, sans-serif` on `html` | LA/style/normalize.scss:47 |
| Base size | `body { font-size: 1.4rem (14px); line-height: 1 }` | normalize.scss:67-71 |
| Reset | `* { margin:0; padding:0; border:none; font: inherit; font-weight: 400; vertical-align: baseline }`; `box-sizing: border-box` inherited | normalize.scss:17-30, 45-46 |
| Buttons | `button { background:none; cursor:pointer }`; `button[disabled] { cursor: not-allowed }` | normalize.scss:36-43 |
| Monospace | `'Roboto Mono', monospace`: code editor, progress messages, hotkey keys, tab path chip | LA/style/base/_common.scss:63; LS editor-group scss:72 |
| Condensed | `'Roboto Condensed'`: the setup-page title and the import-success banner | LS/style/components/_workspace-setup.scss:330; workspace-setup/_create-project-modal.scss:152 |
| Letter icons | `Raleway` 900, for the C/E/A/P/M/p/u type glyphs | LA/style/components/_icon.scss:19-27 |
| Weights loaded | Roboto 100/300/400/500/700/900; Roboto Mono 100-700; Roboto Condensed 300/400/700; Raleway 900 | LA/style/fonts.scss:18-49; LA/style/base/_fonts.scss:17-30 |

- **Scrollbars** (WebKit): 0.8rem (8px) wide and tall; transparent track and corner; thumb `--color-light-shade-100`
  `#ffffff2e` with a 0.2rem transparent border, `background-clip: content-box`, radius 0.4rem. In an inactive window the
  thumb is `#0000002e` (normalize.scss:115-141).
- **Focus.** `*:focus { outline: none }` globally (normalize.scss:32-34). The only focus rings are on
  `.btn--dark:focus` (`outline: 0.1rem solid --color-border-focus #08629e; outline-offset: 0.1rem`,
  LA/style/base/_button.scss:133-136) and `.btn--medium:focus` (an outline in border-default, :158-161). Inputs show
  focus by changing their border colour to `#08629e`.
- **No animations.** The MUI theme disables all transitions (`transitions.create = () => 'none'`,
  LA/src/utils/LegendStyleProvider.tsx:36-45), and Studio does not opt back in. Menus use `transitionDuration={0}`
  (LA/src/menu/BaseMenu.tsx:33, ContextMenu.tsx:114). Ripple is disabled (LegendStyleProvider.tsx:54-58).
- **Browser context menu is suppressed** app-wide. Only explicit `ContextMenu` regions open a menu
  (LAPP/src/components/ApplicationComponentFrameworkProvider.tsx:211-221).
- Tooltips are almost all native `title=` attributes. Where MUI tooltips appear, they use `--color-bg-elevated`
  `#252525`, primary text and 1.2rem (LA/style/reset/muiOverrides.scss:209-218).

### 0.3 Colour tokens (resolved)

Sources: semantic dark tokens in LA/style/base/themes/_semantic-tokens.scss:47-128 (`:root` defaults) and
_theme-default-dark.scss:33-92; the physical palette in LA/style/base/_variables.scss:17-271.

| Semantic token | Dark (default) | Default-light | Notes |
|---|---|---|---|
| `--color-bg-app` | `#1e1e1e` | `#f3f3f3` | editor area, panel group, modal--dark |
| `--color-bg-panel` | `#2d2d2d` | `#fff` | sidebar, tab strip, setup card |
| `--color-bg-panel-header` | `#353535` | `#efefef` | sub-panel headers, inactive tabs |
| `--color-bg-chrome` | `#3e3e3e` | `#e6e6e6` | activity bar |
| `--color-bg-elevated` | `#252525` | `#fff` | menus, notification, setup selector controls |
| `--color-bg-input` | `#2d2d2d` | `#fff` | |
| `--color-bg-hover` | `#3e3e3e` | `#efefef` | |
| `--color-bg-selected` | `#264f77` | `#deebff` | selected tree row, menu hover |
| `--color-bg-tag` | `#595959` | `#cecece` | counters, chips, disabled buttons |
| `--color-bg-overlay` | `#00000061` | same | |
| `--color-text-primary` | `#fafafa` | `#1e1e1e` | |
| `--color-text-secondary` | `#ddd` | `#595959` | |
| `--color-text-muted` | `#bbb` | `#737373` | icons, tree text |
| `--color-text-disabled` | `#737373` | `#9a9a9a` | |
| `--color-text-inverted` | `#1e1e1e` | `#fff` | text on bg-tag and yellow chips (**not in Query yet**) |
| `--color-text-on-accent` | `#f3f3f3` | `#f3f3f3` | |
| `--color-text-link` | `#007acc` | `#08629e` | |
| `--color-border-subtle` | `#2d2d2d` | `#efefef` | |
| `--color-border-default` | `#353535` | `#ddd` | |
| `--color-border-strong` | `#595959` | `#bbb` | splitter hover |
| `--color-border-focus` | `#08629e` | `#08629e` | |
| `--color-accent` | `#08629e` | `#08629e` | primary buttons, modal header |
| `--color-accent-hover` | `#007acc` | `#014a7b` | |
| `--color-accent-subtle` | `#7f7ab124` | `#deebff` | row hover |
| `--color-status-error` | `#f5584b` | `#be1100` | |
| `--color-status-error-bg` | `#ff00001a` | `#ffe6e3` | |
| `--color-status-warn` | `#fbbc05` | `#c58105` | |
| `--color-status-warn-bg` | `#342a18` | `#ffffe0` | |
| `--color-status-success` | `#34a853` | `#34a853` | |
| `--color-status-success-bg` | `#014321` | `#c9e9ca` | Query calls this `--color-status-success-hover` |
| `--color-status-info` | `#007acc` | `#08629e` | **status bar background** (not in Query yet) |
| `--color-state-hover-on-accent` | `#ffffff12` | `#00000014` | status-bar button hover (not in Query yet) |
| `--color-state-disabled-on-accent` | `#00000061` | `#00000061` | inactive status-bar toggles (not in Query yet) |
| `--color-active-indicator` | `#fbbc05` | `#08629e` | tab underlines |
| `--color-category-experimental` | `#7695ff` | same | "beta" sparkle badge (not in Query yet) |
| `--color-category-generation` | `#d14664` | same | generation group in the view-mode menu |
| `--color-shadow` | `#0000004f` | `#00000030` | |

Physical or extra tokens that Studio uses directly:

| Token | Hex | Used for | Cite |
|---|---|---|---|
| `--color-conflict` (`orange-150`) | `#ca663a` | status bar in conflict-resolution mode; conflict counters and buttons | LA/style/base/_variables.scss:168 |
| `--color-orange-200` | `#c95f03` | `.btn--conflict:hover` | _button.scss:188 |
| `--color-pink-400` / `-380` / `-300` | `#b33659` / `#c5375f` / `#d14664` | caution buttons and alerts | _button.scss:166-181; LAPP/style/components/_blocking-alert.scss:74-83 |
| `--color-dark-grey-250` | `#3e3e3e` | the 1px line on the panel-group splitter | LS/src/components/editor/Editor.tsx:262-269 |
| `--color-dark-grey-100` | `#2d2d2d` | splitter lines inside Local Changes | LS/src/components/editor/side-bar/LocalChanges.tsx:339 |
| `--color-blue-200` | `#08629e` | PanelLoadingIndicator bar | LA/style/components/_panel-loading-indicator.scss:38 |
| `--color-input-border` / `--hover` / `--focus` | `#ddd` / `#bbb` / `#477cc5` | non-dark `.input` and `.selector-input` (used in the light theme) | _variables.scss:158-161 |
| `--color-dark-shade-230` | `#0000003d` | app `.backdrop` | LAPP/style/components/_backdrop.scss:18 |

---

## 1. Workspace setup page (`/`)

DOM, from LS/src/components/workspace-setup/WorkspaceSetup.tsx:636-907:

```
div.app__page > div.workspace-setup
  div.workspace-setup__body                (flex row, height calc(100% - 2.2rem))
    div.activity-bar                       (menu ☰ + empty spacer + theme toggle; see §2.1)
    div.workspace-setup__content           (width calc(100% - 5rem), flex centre/centre)
      div.workspace-setup__content__body   (height 80%, width min(90vw,131rem), column)
        div.workspace-setup__content__main (the big card)
          div.workspace-setup__title > div.__logo > img.__logo__icon (src = `${baseAddress}favicon.ico`)
                                    + div.__title__header "Welcome to Legend Studio"
          RecentWorkspacesPanel            (only if there are recents)
          div.workspace-setup__selectors > __selectors__container > 2 × div.workspace-setup__selector
          div.workspace-setup__actions-combo > div.workspace-setup__actions
             button.__new-workspace-btn "Need to create a new workspace?"
             div.__actions__button > button.workspace-setup__go-btn.btn--dark "Go →"
             DividerWithText "OR"
             div.__actions__button--projects > button.__new-btn.btn--dark "Create New Project" [+ "Create Sandbox Project"]
        div.workspace-setup__content__cards (RuleEngagement / Showcase / FAQ / Production doc cards)
  div.editor__status-bar                   (empty left; right: assistant toggle only)
```

### 1.1 Page and card geometry (LS/style/components/_workspace-setup.scss)

- `.workspace-setup`: `background: --color-bg-app #1e1e1e`, full height (:128-131).
- `__body`: flex row, `height: calc(100% - 2.2rem)` (:315-320). The bottom 2.2rem (22px) is the blue status bar (§7).
- `__content__body`: `height: 80%; width: min(90vw, 131rem)`, flex column, `justify-content: flex-start` (:353-359).
- **Main card** `__content__main` (:361-372): `height: 78%`, flex column `space-evenly`,
  `background: --color-bg-panel #2d2d2d`, `border: 0.1rem solid #2d2d2d`, `border-radius: 1rem`,
  `box-shadow: 0 1px 5px 0 rgb(0 0 0 / 50%)`, `overflow: hidden`.
- **Title** (:322-336): `__title` is 24% tall; `__logo` is a centred flex box, `height: 36%`, `margin: 2.5vh 0 2.15vh`. The
  logo is the deployment's `favicon.ico`, shown at its natural size because `__logo__icon` has no CSS
  (WorkspaceSetup.tsx:404, 654-659). `__title__header`: **"Welcome to Legend Studio"**, `font-family: 'Roboto Condensed'`,
  700, `font-size: min(4vw, 4vh)`, `color: --color-text-secondary #ddd`, centred, `margin-bottom: 2.5vh`.
- Everything inside the card scales with the viewport (`vh`/`vw`), not rem. This is the one place in Studio where that is true.

### 1.2 Recent workspaces strip (LS/src/components/workspace-setup/RecentWorkspacesPanel.tsx:62-180; scss :719-889)

- Hidden when there are no recents. Shows at most 6 tiles (`MAX_TILES`, :29).
- Container: `max-height: 14vh; margin: 0 2vw 2vh`, flex column.
- Header: `HistoryIcon` (1.1rem) + **"Recent workspaces"**, 1.2rem / 500 / `--color-text-muted #bbb`, gap 0.5rem (:92-95; scss :734-746).
- Grid: `display: grid; grid-auto-flow: column; grid-auto-columns: 18rem; gap: 0.8rem; overflow-x: auto` (:749-760).
- **Tile** (a `<button>`, scss :762-791): column flex, `padding: 0.6rem 0.8rem`, `background: #2d2d2d`,
  `border: 0.1rem solid --color-border-default #353535`, `border-radius: 0.3rem`, text `#fafafa`,
  `transition: background-color/border-color 0.1s`.
  - Hover: background `#353535`, border `#595959`, and the remove button fades in.
  - Focus-visible: outline `0.1rem solid --color-blue-150 #0a73b9`, offset 0.1rem.
- Remove button: top-right at 0.2rem, 1.6rem square, opacity 0 until hover, `TimesIcon` at 0.9rem, colour muted (primary
  on hover, with a `#3e3e3e` background), tooltip **"Remove from recents"** (:132-140; scss :792-819).
- Row 1: `FolderIcon` (1rem, muted) + project name, 1.2rem / 500, ellipsis, `padding-right: 1.4rem` (scss :821-847).
- Row 2: `UsersIcon` (group) or `UserIcon` (user) + workspace id, 1.1rem, `#ddd` (:147-155).
- Meta row (pushed to the bottom): up to 2 project tags (0.95rem, line-height 1.3rem, `padding: 0 0.4rem`, radius
  0.2rem, `#595959` background, `#bbb` text, max-width 6rem) and a relative time (1rem, muted).
  The time reads `just now`, `Nm ago`, `Nh ago`, `Nd ago`, `Nw ago`, `Nmo ago` or `Ny ago` (:31-59).
- Tooltip: `"{project} / {ws}\n{description}"`, or `"Open {project} / {ws}"` when there is no description (:103-106).

### 1.3 Project and workspace selectors (WorkspaceSetup.tsx:665-797; scss :128-305, 484-606)

- Each `__selector` is 45% tall, column `space-between`, `margin: 0 2vw`.
- **Header row** (`__selector__header`): `font-size: min(2vw, 2vh)`, 500, `#ddd`, 33% tall.
  - Project header: **"Search for an existing project"**. On the right, when recents exist, a button **"Clear recents"**
    (1.2vh, 400, muted, underlined, primary on hover; tooltip "Clear recently-opened projects and workspaces").
  - Workspace header: **"Choose an existing workspace"**.
- **Control row** (`__selector__content`): 55% tall, flex.
  - Left icon cell: `width: 4vh`, background `--color-bg-elevated #252525`, `border-radius: 0.2rem 0 0 0.2rem`, svg at 1.5vh
    in `#737373`. The project cell holds `SearchIcon` (title "project"); the workspace cell holds `GitBranchIcon` (title "workspace").
  - Then a react-select (`CustomSelectorInput`, dark variant). Overrides inside `.workspace-setup`:
    - The control fills the row height, with background `#252525` instead of `#2d2d2d` (:154-159).
    - Placeholder font `min(1.25vw, 1.25vh)`, single value `min(1vw, 1.5rem)`, input `min(1.5vw, 1.5vh)` (:142-169).
    - **The dropdown indicator is a small accent-blue square**: `background: #08629e`, 2vh × 2vh, `margin-right: 1vh`,
      caret at 1.5vh (:181-191). The indicator separator is removed (:193-198).
    - Clear indicator: `TimesIcon` at 1.5vh, margin-right 1vh (:133-140).
    - "Locked" when a value is selected: transparent caret, default cursor (:171-179).
  - Placeholders (WorkspaceSetup.tsx:727, 786-794):
    - Project: **"Search for project..."**
    - Workspace: **"Loading workspaces..."**, then **"In order to choose a workspace, a project must be chosen"**, then
      **"Choose an existing workspace"**, or **"You have no workspaces. Please create one to proceed..."**
  - Menu option row height is `window.innerHeight * 0.03` (WorkspaceSetup.tsx:631-633), and the option label font is
    `min(1.5vw, 1.5vh)` (scss :72-74).
- **Project option label** (LS/src/components/workspace-setup/ProjectSelectorUtils.tsx:64-124; scss
  workspace-setup/_project-selector.scss):
  - Name.
  - When the selected project is configured, a split chip: **"view"** (4.2rem × 1.8rem, `#595959` background,
    `--color-text-inverted #1e1e1e` text, 1.2rem / 500, radius `0.2rem 0 0 0.2rem`) followed by a round-ended cell holding
    `ArrowCircleRightIcon` at 1.6rem (radius `0 50% 50% 0`). Hover turns both halves `#3e3e3e`.
  - When the project is not configured, a yellow chip: `ExclamationCircleIcon`, **"configure"**, then the arrow icon, all on
    `--color-status-warn #fbbc05` with `#1e1e1e` text. Its tooltip is "The project has not been configured properly. Click to see the review and commit it to get complete the configuration."
  - Inside `.workspace-setup` these chips are resized in vh (:204-265).
- **Workspace option label** (WorkspaceSelectorUtils.tsx:32-52): a `UsersIcon` or `UserIcon` cell (2rem), the workspace id,
  and, for patch workspaces, a right-hand chip **`patch/{version}`** (8rem × 1.8rem, `#595959` background, `#1e1e1e` text,
  1.2rem / 500).

### 1.4 Actions (WorkspaceSetup.tsx:800-865; scss :608-698)

- `__actions-combo` is 36% tall with `margin-top: 2vh`. `__actions` is 88% tall, a column justified to `flex-end`.
- **"Need to create a new workspace?"** (`__new-workspace-btn`): a text link in `--color-accent #08629e`,
  `font-size: min(1.5vw,1.5vh)`, `margin-left: 2vw; margin-bottom: 1.75vh`, left-aligned. Disabled colour `#737373`.
  Tooltip "Create a workspace after choosing a project".
- **Go button** (`.workspace-setup__go-btn.btn--dark`):
  - `width: max(20%, 22rem)`, `aspect-ratio: 255/61`, `padding: 0.6rem 1.2rem`, `background: #08629e`, radius 0.1rem.
  - Hover `#007acc`. Disabled: `#595959` background, `#737373` text.
  - Label **"Go"**, 700, `font-size: clamp(1.4rem, 12cqw, 3.2rem)`, followed by `LongArrowRightIcon` at 1.5vh with `margin-left: 1vh`.
  - The button is a container-query container (`container-type: inline-size`).
- **"OR" divider** (`DividerWithText`, LA/src/divider/Divider.tsx:19-31; LA/style/base/_divider.scss:19-33): two 3px lines in
  `#353535` with the text between them (1.5vh inside setup, `#ddd`, `padding: 0 10px`). Margins `1.5vh 2vw`.
- **"Create New Project"** (`__new-btn.btn--dark`): the same box as Go, `font-size: clamp(1.1rem, 7cqw, 2rem)`,
  `margin: 0 3%`, tooltip "Create a Project". There is an optional second button, **"Create Sandbox Project"**.

### 1.5 Documentation cards under the main card (WorkspaceSetup.tsx:117-318, 867-872; scss :374-482)

- A flex row, `space-evenly`, `margin-top: 3rem`, **hidden at 800px wide or less**.
- Each card is `flex: 0 0 calc((100% - 6rem)/3)` and `aspect-ratio: 3/2`.
- Card styling: `.workspace-setup__content__card` sets `background: #2d2d2d !important`, MUI Card, `border-radius: 1rem`
  (LA/style/reset/muiOverrides.scss:109-118).
  - Header: centred, `clamp(1.2rem, 5.5cqw, 2.4rem)`, 500, primary text.
  - Body: centred, `clamp(1.1rem, 4cqw, 2rem)`.
  - Action: an MUI `Button`, `--color-text-link #007acc`. MUI buttons default to `text-transform: uppercase`, so the
    labels render in capitals. That is an inference from MUI defaults; no Legend CSS overrides it. The action ends with `OpenIcon`.
- Built-in cards:
  - **"Showcase Projects"**: "Review showcase projects with sample project code and re-use existing code snippets to quickly build your model. Review Studio documentation." Action "Showcase explorer".
  - **"Documentation"**: "Review Studio documentation." Action "Review documentation".
  - The FAQ, Production and Rule-engagement cards appear only when the deployment configures those documentation entries.

### 1.6 Create Workspace dialog (LS/src/components/workspace-setup/CreateWorkspaceModal.tsx:126-225; scss workspace-setup/_create-project-modal.scss:19-58)

- MUI Dialog with the `search-modal__container` and `__inner-container` classes; the box is
  `.modal.modal--dark.workspace-setup__create-workspace-modal`. That gives `width: 75rem`,
  `border: 0.1rem solid #08629e`, `padding: 2rem`, `background: #1e1e1e`, `color: #ddd`.
- Title row: `div.modal__title` **"Create Workspace"** (1.8rem / 700) plus a `DocumentationLink`. The link is a
  `QuestionCircleIcon` in `#737373` that turns `#bbb` on hover, margin-left 0.5rem, tooltip "Click to see documentation"
  (LL/src/application/DocumentationLink.tsx:52-57; LL/style/application/_documentation-link.scss).
- A PanelLoadingIndicator (§9.6), then the form:
  - **"Workspace Name"**, placeholder **"MyWorkspace"**, full width. The error text is "Workspace with same name already exists " (note the trailing space).
  - **"Workspace Source"**: label 500 / primary / line-height 2rem, then a react-select whose default value is **"HEAD"**;
    patch options read `patch/{id}`.
  - **"Group Workspace"** boolean, prompt "Group workspaces can be accessed by all users in the project", **on by default** (CreateWorkspaceModal.tsx:52).
  - Submit: a right-aligned `button.btn.btn--dark` **"Create"** (PanelFormActions is `display:flex; justify-content:flex-end`).
- The form controls are described in §9.3 and §9.4.

### 1.7 Create Project dialog (LS/src/components/workspace-setup/CreateProjectModal.tsx:682-731, 155-338; scss :60-181)

- `.modal.modal--dark.workspace-setup__create-project-modal`: `width: 75rem`, accent border, `padding: 0`.
- Header **"Create Project"**: `font-size: 2rem; font-weight: bold; margin: 1.5rem 2rem`.
- **Tab strip**: `height: 2.8rem`, `background: --color-bg-elevated #252525`.
  - Tabs: `padding: 0 2rem`, `#bbb`, `border-right: 0.1rem solid #2d2d2d`.
  - The active tab gets a 0.2rem **yellow underline** `#fbbc05` through `::after`.
  - Tabs: **"Create New Project"** and **"Import Project"**, each followed by a `?` doc link.
- Content `padding: 2rem`. The Create tab's fields, in order:
  - **Project Name**, placeholder "MyProject".
  - **Description**, a textarea 8rem tall with placeholder "(optional)".
  - **Group ID**. Prompt: "The domain for artifacts generated as part of the project build pipeline and published to an artifact repository". The placeholder comes from config.
  - **Artifact ID**. Prompt: "The identifier (within the domain specified by group ID) for artifacts generated as part of the project build pipeline and published to an artifact repository". Placeholder "my-project".
  - **Tags**. Prompt: "List of annotations to categorize projects". It uses the list editor in §9.4, with "Add Value", "Save" and "Cancel".
  - Submit **"Create"**, `height: 3.6rem`.
- The Import tab adds **Project ID** (prompt "The ID of the project in the underlying version-control system", placeholder "1234"). Its button reads "Import", or "Review" after a successful import.
- After a successful import, a banner appears: `background: --color-status-success #34a853`, `#1e1e1e` text, Roboto Condensed 1.6rem, padding 0.5rem.
- When creation is unsupported, the dialog shows centred text "SDLC server does not support creating new projects" (500, `#737373`).

---

## 2. Editor shell

DOM, from LS/src/components/editor/Editor.tsx:224-297:

```
div.app__page > div.editor (flex column, 100%)
  div.editor__body (flex row, height calc(100% - 2.2rem))
    div.activity-bar                                    (5rem = 50px wide)
    div.editor__content-container (bg #1e1e1e, flex 1)
      div.editor__content
        reflex-container.vertical
          reflex-element  [SideBar]              initial 300px, snap 150 (LS/src/stores/editor/EditorStore.ts:217-221)
          reflex-splitter
          reflex-element  (minSize 300)
            reflex-container.horizontal
              reflex-element [EditorGroup | GrammarTextEditor | splash]
              reflex-splitter > div.resizable-panel__splitter-line (1px, #3e3e3e)
              reflex-element [PanelGroup]        initial 0 (closed), default 300px, snap 100 (EditorStore.ts:211-215)
          reflex-splitter
          reflex-element  [Showcase manager]     (right-hand, normally collapsed)
  div.editor__status-bar                                (2.2rem = 22px)
```

- `.editor__content-container`: `background: --color-bg-app`, `flex: 1 0 auto` (LS/style/components/_editor.scss:58-80).
- **Resizing handles** (LA/style/reset/_react-reflex.scss:65-144):
  - Vertical splitters are `width: 0.4rem; margin: 0 -0.2rem` (a 4px hit area that overlaps both neighbours) and transparent.
  - Horizontal splitters are `height: 0.4rem; margin: -0.2rem 0`.
  - On `:hover` or `.active`, the splitter turns `--color-border-strong #595959` with `transition: all 1s ease`.
  - The optional 1px `.resizable-panel__splitter-line` sits centred at 0.2rem and is hidden while the splitter is hovered.
  - The panel-group splitter uses line colour `--color-dark-grey-250 #3e3e3e`, or transparent when the panel is maximised (Editor.tsx:262-269).
  - Cursors are `col-resize` and `row-resize`. A collapsed panel gets `visibility: hidden`.

### 2.1 Activity bar (LS/src/components/editor/ActivityBar.tsx:164-595; LS/style/components/editor/_activity-bar.scss:19-182)

- `.activity-bar`: `width: 5rem (50px)`, `background: --color-bg-chrome #3e3e3e`, full height, flex column, no overflow.
- **Top menu cell** `__menu`: `height: 3.4rem`, `border-bottom: 0.1rem solid #353535`.
  - It holds `MenuIcon` (IoMenuOutline) at **2.3rem** in muted `#bbb`.
  - Clicking opens a dropdown to the right (anchored top-right, opening top-left, elevation 7) containing **About**,
    **See Showcases** (only if enabled), **Documentation** (disabled when no URL), any configured doc links,
    **Help...**, a divider, and **Back to workspace setup** (ActivityBar.tsx:211-260).
- **Items** `__items`: `flex: 1 1 auto; overflow-y: auto`.
- **Each item** `button.activity-bar__item`: 5rem × 5rem (50×50), flex-centred, `color: #bbb`, `position: relative`.
  The svg is **2rem (20px)**; some icons override that size.
  - Hover and `--active` change only the colour, to `#fafafa`. **There is no left bar, no background and no border for the active item** (scss :77-84).
  - An item is active when the sidebar is open and the item is the current activity (ActivityBar.tsx:527-531).
- Items, in order (ActivityBar.tsx:387-498). The tooltip is the `title`:

| # | Icon | Size | Tooltip | Shown when |
|---|---|---|---|---|
| 1 | `FileTrayIcon` (IoFileTrayFullOutline) | 2.3rem (`.activity-bar__explorer-icon`, scss :73-75) | "Explorer (Ctrl + Shift + X)" | always |
| 2 | `FlaskIcon` (IoFlaskSharp) | 2rem | "Test Runner" | not conflict-resolution, not lazy text |
| 3 | `CodeBranchIcon` (FaCodeBranch) plus a counter | 2rem | "Local Changes (Ctrl + Shift + G)" + " - N unpushed changes" | not conflict-resolution |
| 4 | `CloudDownloadIcon` (IoCloudDownloadOutline) plus a dot | 2rem | "Update Workspace (Ctrl + Shift + U)" + " - Update available[ with potential conflicts]" | not conflict-resolution |
| 5 | `GitPullRequestIcon` (GoGitPullRequest) plus a dot | 2.3rem (`__review-icon svg`) | "Review (Ctrl + Shift + M)" + " - N changes" | not conflict-resolution |
| 6 | `GitMergeIcon` (GoGitMerge), **rotated 180° with scaleX(-1)** | 2.4rem | "Conflict Resolution - N changes (M unresolved conflicts)" | **only** in conflict-resolution |
| 7 | `RepoIcon` (TbBook) | 2.3rem | "Project" | not conflict-resolution |
| 8 | `WrenchIcon` (FaWrench) | 2rem | "Workflow Manager" | not conflict-resolution |
| 9 | `DevIcon` (FaDev) plus beta badge | 2.4rem | "Dev Mode (Beta)" | |
| 10 | `RobotIcon` (FaRobot) plus beta badge | 2.4rem | "Register Service (Beta)" | |
| (11) | after a `menu__divider`: `WorkflowIcon` (FcWorkflow, a multi-colour icon) plus beta badge | 2.4rem | "End to End Workflows (Beta)" | non-production flag only |

- **Bottom items** sit outside `__items`, so they stay pinned to the bottom (ActivityBar.tsx:572-592):
  - `ReadMeIcon` (FaReadme), tooltip "Open Showcases".
  - The theme toggle: `SunIcon` (IoSunnyOutline) with tooltip "Switch to light theme" while in dark; `MoonIcon` (IoMoon) with "Switch to dark theme" while in light.
  - `CogIcon` (FaCog), tooltip "Settings". It opens a menu to the right of the bar, bottom edges aligned (anchor bottom-right, transform bottom-left), with a single item **"Show Developer Tool"** with a blank icon slot. The menu has `min-width: 20rem` and its svgs are 1rem.
- **Indicators.** All are absolutely positioned on `__item__icon-with-indicator` (scss :86-159):
  - **Local-change counter**: `min-width: 1.6rem; height: 1.6rem`, `background: #08629e`, `#f3f3f3` text, 0.9rem / 500,
    `padding: 0 0.5rem`, `border-radius: 1rem`, at `bottom: -0.5rem; right: -0.7rem`. Counts above 99 read "99+".
  - While change detection is still loading, the counter instead holds `EmptyClockIcon` (FaRegClock) at 1.2rem with padding 0 0.2rem.
  - **Conflict counter**: the same shape in `--color-conflict #ca663a` with `#fafafa` text, at `bottom: -0.4rem; right: -0.3rem`.
  - **Dots**: 1rem circles. The "update available" dot is accent `#08629e` (orange `#ca663a` if there are potential
    conflicts) at -0.4rem / -0.4rem. The "review has changes" dot is `#fbbc05` at -0.3rem / -0.3rem.
  - **Beta badge** (LL/style/application/_activity-bar.scss:19-38): a 1.6rem circle at `top: 0.6rem; right: 0.6rem`,
    `background: --color-category-experimental #7695ff`, `border: 0.1rem solid #353535`. It holds a `SparkleIcon` (custom
    SVG) at 1.2rem in `#ddd`, with tooltip "This is an experimental feature".
- On the setup page the bar contains only the menu, an empty spacer and the theme toggle (WorkspaceSetup.tsx:640-646).

### 2.2 Side bar container and header (LS/style/components/editor/_side-bar.scss:33-185; LA/src/layout/Panel.tsx:98-122)

- Every side-bar view is `div.panel.<view>` holding `div.panel__header.side-bar__header` and `div.panel__content.side-bar__content`.
- **`.panel__header` base** (LA/style/base/_panel.scss:40-57):
  - Flex, vertically centred, `space-between`, `padding-left: 1rem`.
  - `box-shadow: #0000004f 0 0.1rem 0.5rem 0`, `z-index: 1`, `cursor: default`.
  - Default height 2.8rem with background `#353535`.
- **`.side-bar__header`** overrides that to `height: 3.4rem; background: --color-bg-panel #2d2d2d; color: #bbb; z-index: 0`.
- **Title** `.panel__header__title__content.side-bar__header__title__content`: text from the code in UPPER CASE
  (**EXPLORER**, **LOCAL CHANGES**, **REVIEW**, **PROJECT**, …), `font-size: 1.2rem`, `font-weight: 500` (side-bar wins over
  the base `bold`), `color: #fafafa` from the base title-content rule, ellipsis.
- **Viewer-mode badge** (next to the title, `side-bar__header__title__viewer-mode-badge`): 2rem tall, `background: #fbbc05`,
  `color: --color-text-inverted #1e1e1e`, 1.2rem / 700, radius 0.3rem, `padding: 0 0.7rem`, `LockIcon` at 1rem with
  margin-right 0.3rem. The text is **"READ-ONLY"**, or the editor mode's label (LS/src/components/editor/side-bar/Explorer.tsx:1498-1509).
- **Header actions** `.panel__header__actions.side-bar__header__actions`: `padding-right: 0.5rem`.
  - Each `button.panel__header__action` is `width: 2.8rem`, full header height, transparent, `color: #bbb`.
  - Disabled svgs turn `#737373`. Icons inherit 1.4rem (14px) unless a rule overrides them.
- **Content** `.side-bar__content`: `background: #2d2d2d; color: #bbb; height: calc(100% - 3.4rem); overflow-y: hidden`.
- **Sub-panels inside a side bar** (`.side-bar__panel .panel__header`): `min-width: 9rem`, `background: #353535`,
  `color: #ddd`, `padding-left: 1rem`, height 2.8rem. The title content is bold `#fafafa` (e.g. "CHANGES").
  - Info icon: `InfoCircleIcon`, margin-left 0.5rem, in a tooltip wrapper.
  - **Count pill** `side-bar__panel__header__changes-count`: `background: #595959`, radius 0.8rem, `padding: 0.3rem 0.7rem`,
    height 1.6rem, 1rem / 500, `margin-right: 1rem`, text `#ddd` (scss :128-139).
  - Sub-panel content: `padding: 0.5rem 0; background: #2d2d2d`.
- **Side-bar list item** `.side-bar__panel__item` (a `<button>`): `height: 2.2rem; padding: 0 0.5rem 0 1rem`, flex
  space-between, full width, left-aligned. Hover `#7f7ab124`; `--selected` uses `#264f77` (scss :141-160).
- Inputs and textareas inside side bars are lifted to `#353535` with a matching border; focus border `#08629e` (scss :166-184).

---

## 3. Explorer

DOM and strings: LS/src/components/editor/side-bar/Explorer.tsx:1457-1613. Styles: LS/style/components/editor/side-bar/_explorer.scss
and LA/style/components/_tree-view.scss.

```
div.panel.explorer
  div.panel__header.side-bar__header  "EXPLORER" [READ-ONLY badge]
  div.panel__content.side-bar__content
    div.panel.explorer
      div.panel__header.explorer__header
        div.panel__header__title__label  "workspace" | "project"      (chip)
        div.panel__header__title__content  {workspaceId}              (bold #fafafa, ellipsis)
        div.panel__header__actions  [import][config][+ ▾][collapse][search]
      div.panel__content.explorer__content__container
        PanelLoadingIndicator
        ContextMenu.explorer__content  (flex column, padding 0.5rem 0, color #bbb)
          TreeView (main) · ProjectConfig "config" row · TreeView (system) · TreeView (dependencies) · TreeView (generation) · separator + file tree
          div.explorer__deselector   (flex: auto, min-height 5rem; clicking it deselects)
```

### 3.1 Explorer sub-header (scss :44-55; LA/style/base/_panel.scss:89-106)

- `.explorer__header`: `background: #353535; color: #ddd; padding-left: 1rem; min-width: 9rem`, height 2.8rem; the actions are pushed right.
- **Label chip** `.panel__header__title__label`: the text **"workspace"** (or **"project"** in viewer mode). `height: 1.8rem`,
  `line-height: 1.8rem`, radius 0.1rem, `padding: 0 0.5rem`, `color: #ddd`, `background: #595959`, **1.1rem**, margin-right 0.5rem.
- Next to it, the workspace id (or the project name in viewer mode), bold `#fafafa`, ellipsis (Explorer.tsx:1514-1527).
- **Actions** (Explorer.tsx:1394-1453), each 2.8rem wide, full height, `#bbb`, `#737373` when disabled:

| Icon | Tooltip | Notes |
|---|---|---|
| `FileImportIcon` (FaFileImport) | "Open Model Importer (F2)" | |
| `SettingsEthernetIcon` (MdSettingsEthernet), **1.6rem** | "Project Configuration Panel" | only when SDLC operations are supported |
| `PlusIcon` (FaPlus) | "New Element... (Ctrl + Shift + N)" | a dropdown, see §3.4 |
| `CompressIcon` (FaCompress) | "Collapse All" | |
| `SearchIcon` (FaSearch) | "Open Element... (Ctrl + P)" | opens the search modal in §9.7 |

### 3.2 Tree rows

- **Row** `div.tree-view__node__container.explorer__package-tree__node__container`:
  - `height: 2.2rem (22px)`, flex, vertically centred, `cursor: pointer` (`grab` when draggable), `padding-right: 0.5rem`.
  - **Indentation is `padding-left: level × 1rem`**, with root nodes at level 0 (Explorer.tsx:1086-1089; LA/src/tree/TreeView.tsx; the step is 1rem).
  - Hover: `--color-accent-subtle #7f7ab124`. Selected (and selected+hover): `--color-bg-selected #264f77` (scss :123-134).
  - The label text keeps `#bbb` when selected; only the background changes.
- **Icon block** `tree-view__node__icon.explorer__package-tree__node__icon`: `width: 4rem`, `height: 2.2rem`, flex-centred,
  `padding-right: 0.5rem`. It holds two 2rem cells:
  - **Expand cell** (`__icon__expand`, svg **1rem**): `ChevronDownIcon` (GoChevronDown) when open, `ChevronRightIcon`
    (GoChevronRight) when closed, empty for non-packages.
  - **Type cell** (`__icon__type`): a folder or element icon (§3.3).
- **Label** `button.tree-view__node__label`: `height: 2.2rem; line-height: 2.2rem`, ellipsis, inherits 1.4rem Roboto and `#bbb`.
  The tooltip is the full element path. Dependency roots show GAV coordinates.
- **Packages**: `FolderIcon` (FaFolder) when closed, `FolderOpenIcon` (FaFolderOpen) when open, in `#bbb`. Packages from
  special roots are tinted:
  - generated: `--color-generated #f05577`
  - system: `--color-system #6391d0`
  - dependency: `--color-dependency #a5ea81`
  - (Explorer.tsx:1034-1051; LAPP/style/_extensions.scss:28-30)
- **Project-configuration row** `explorer__floating-item` (Explorer.tsx:965-1001): `padding-left: 1rem`, an icon cell holding
  `SettingsEthernetIcon` at **1.6rem** in **`#fbbc05`** with `padding-left: 2rem` (scss :114-121), and the label **"config"**
  with tooltip "Project configuration". It can be selected like a tree row.
- File-generation trees are preceded by `explorer__content__separator`: 1px `#353535` with margin 0.5rem 0.

### 3.3 Element-type icons and colours (LS/src/components/ElementIconUtils.tsx:61-150; LA/src/icon/TypeIcon.tsx:40-204; colours LAPP/style/_extensions.scss:17-147)

Each icon is `div.icon.color--X`, flex-centred, Raleway 900 (LA/style/components/_icon.scss:19-27), at the inherited
1.4rem (14px) unless noted.

| Element | Glyph | TypeIcon.tsx | Colour token → hex |
|---|---|---|---|
| Class | letter **C** | :50-52 | `--color-class` → purple-100 **`#8a58ab`** |
| Enumeration | **E** | :62-64 | `--color-enumeration` → medium-green-100 **`#39cca2`** |
| Association | **A** | :54-56 | `--color-association` → light-grey-400 **`#bbb`** |
| Profile | **P** | :74-76 | `--color-profile` → lime-75 **`#aad468`** |
| Measure | **M** | :66-68 | `--color-measure` **`#39cca2`** |
| Unit | **u** | :70-72 | `--color-unit` **`#39cca2`** |
| Primitive | **p** | :40-42 | `--color-primitive` → light-blue-200 **`#477cc5`** |
| Enum value | **e** | :58-60 | `--color-enum-value` → green-100 **`#34a853`** |
| Function | `FunctionIcon` (TbMathFunction), svg **1.7rem** | :78-82 | `--color-function` → light-blue-20 **`#7fdbff`** |
| Mapping | `MapIcon` (FaMap) | :118-122 | `--color-mapping` → teal-50 **`#16b8bf`** |
| Runtime | `BusinessTimeIcon` (FaBusinessTime) | :146-150 | `--color-runtime` → red-180 **`#ea4646`** |
| Connection | `LinkIcon` (MdLink), svg **1.6rem** | :140-144 | `--color-connection` → yellow-100 **`#ffca34`** |
| Database (relational) | `DatabaseIcon` (FaDatabase) | :90-94 | `--color-database` → orange-100 **`#e97f49`** |
| Flat-data store | `LayerGroupIcon` (FaLayerGroup), svg **1.2rem** | :84-88 | `--color-flat-data` **`#e97f49`** |
| Service | `RobotIcon` (FaRobot) | :134-138 | `--color-service` → blue-40 **`#40a6ff`** |
| Data element | `TabulatedDataFileIcon` (BsFillFileEarmarkSpreadsheetFill) | :158-162 | `--color-data` → light-blue-50 **`#6391d0`** |
| File generation | `FileCodeIcon` (FaFileCode) | :128-132 | `--color-file-generation` → blue-50 **`#1c89d2`** |
| Generation spec | letter **G** | :124-126 | none (inherits `#bbb`) |
| Package (in the type menu) | `PackageIcon` (FiPackage) | :44-48 | `color--package` has no rule, so it inherits |
| Snowflake app / M2M UDF | `Snowflake_BrandIcon` (TbBrandSnowflake) | :164-174 | `--color-snowflake-app` **`#40a6ff`** |
| Data product (beta) | `AccessPointIcon` (LuRadioTower) | :176-180 | `#40a6ff` |
| Compute (beta) | `CpuIcon` (TbCpu) | :200-204 | `--color-compute` **`#40a6ff`** |
| Ingest / Availability | `DatabaseImportIcon` (TbDatabaseImport) | :188-192 | `--color-data` **`#6391d0`** |
| MemSQL function | `SinglestoreIcon` (SiSinglestore) | :194-198 | `--color-mem-sql-function` **`#820ddf`** |
| Local connection (temporary) | `LinkIcon` (MdLink), no colour | ElementIconUtils.tsx:102-103 | inherits |
| Any other FunctionActivator | `LaunchIcon` (MdRocketLaunch) | ElementIconUtils.tsx:142-144 | inherits |
| Unknown | `QuestionSquareIcon` (BsQuestionSquare), 1.5rem, `#737373` | :182-186; _icon.scss:29-32 | |
| **Data space** (extension) | `SquareIcon` (FaSquare) in `.icon--data-space` | legend-extension-dsl-data-space/src/components/shared/DSL_DataSpace_Icon.tsx:19-23 | `--color-data-space` → blue-50 **`#1c89d2`** |
| Diagram (extension) | `ShapesIcon` (FaShapes) | legend-extension-dsl-diagram-studio/…Plugin.tsx:133-137 | `--color-diagram` → magenta-100 **`#bb619b`** |
| Text (extension) | `FileIcon` (FaFile) | legend-extension-dsl-text/…Plugin.tsx:109-112 | `--color-text-element` **`#1c89d2`** |
| Persistence / context | `MeteorIcon` (FaMeteor) / `PuzzlePieceIcon` (FaPuzzlePiece) | legend-extension-dsl-persistence/…Plugin.tsx:114-124 | `#f90` / `#2cca72` |
| Service store | `SwaggerIcon` (SiSwagger) | legend-extension-store-service-store/…Plugin.tsx:170-179 | inline `--color-light-grey-50` **`#f3f3f3`** |

Other relevant colours from the same file (_extensions.scss): `--color-schema #11846d`, `--color-table #477cc5`,
`--color-relational #002f4f`.

### 3.4 Context menu and the "+ New element" menu

- **Menu look** (LA/style/base/_menu.scss:19-71; LA/style/reset/muiOverrides.scss:17-25):
  - It sits in an MUI Popover/Menu: paper `background: #252525`, `border-radius: 0`, list padding 0, elevation 7 (an MUI drop shadow).
  - `.menu`: `background: #252525; border: 0.1rem solid #2d2d2d; padding: 0.5rem 0`.
  - `.menu__item` (a `<button>`): flex, vertically centred, full width, `height: 2.8rem; padding: 0 1rem; color: #bbb`, svg margin-right 0.5rem.
    Hover (when enabled) `#264f77`. Disabled `#737373` with cursor not-allowed.
  - `.menu__item__icon` is a 2rem fixed cell; `.menu__item__label` has margin-left 1rem.
  - `.menu__divider`: 0.1rem tall, `#353535`, radius 0.1rem, margin 0.5rem.
- **Element context menu** (Explorer.tsx:864-961). Items depend on the element type:
  - Class: "Query...", "Generate Sample Data...", then a divider.
  - Service: "Query...".
  - Relational connection: "Execute SQL...", "Build Database...".
  - Database: "Query...", "Build Models".
  - Data-cube-capable elements: "Data Cube (BETA)...".
  - Then the common items: "Rename", "Remove", a divider, "View in Project", "Copy Path", "Copy Link", and "Copy SDLC Project Link" for dependency elements.
- **Package context menu, or the + dropdown** (Explorer.tsx:820-858 and 1113-1157). The menu lists **groups of element types**:
  - Each group is `div.editor-group__view-mode__option__group.--native`: flex row, `background` and `border` `--color-status-info #007acc`.
  - Group name cell `__option__group__name`: **vertical text** (`writing-mode: vertical-lr; transform: rotate(180deg)`),
    1.1rem, `width: 2.2rem`, `padding: 0.5rem 0`, `#007acc` background, `#f3f3f3` text. The names are **Model**, **Store**,
    **Query**, **External Format**, **Generation**, **Other** (LS/src/stores/editor/utils/ModelClassifierUtils.ts:86-93).
  - Options column: `background: #2d2d2d`. Each option is a `.menu__item` with the type icon from §3.3 and a title-cased label
    (Class, Association, Enumeration, Profile, Function, Measure, Data, Relational Database, Flat-Data Store, Connection, Runtime, Mapping, Service, …).
    Styles: LS/style/components/editor/_editor-group.scss:176-236.
  - On the root package only **Package** is offered (Explorer.tsx:1121-1129).
  - For a package node, "Rename" and "Remove" follow, each with a blank icon slot.
- The `menu__trigger--on-menu-open` class has **no CSS**, so a right-clicked row is not highlighted unless it is already selected.

### 3.5 Empty, loading and failure states

- **Loading**: a PanelLoadingIndicator bar (§9.6), plus progress text in `explorer__content__progress-msg`
  (`margin: 1rem; line-height: 2rem; #737373; Roboto Mono 1.2rem; no selection`, scss :78-85). The text is the current
  init or build message (Explorer.tsx:1573-1587).
- **Empty workspace** (`explorer__content--empty`, padding 2rem; scss :176-194):
  - Text (line-height 1.8rem, margin-bottom 1rem): **"Your workspace is empty, you can add elements or load existing model/entites for quick adding"** (sic, "entites").
  - Then a full-width **"Open Model Importer"** button: `height: 3.4rem`, `#08629e` background, `#f3f3f3` text (Explorer.tsx:1351-1362).
- **Graph build failed**: inside a BlankPanelContent, a watermark `ExclamationTriangleIcon` at **11rem** in `--color-border-subtle #2d2d2d`
  (nearly invisible by design), then **"Failed to build graph"** (500, margin-top 1rem). Explorer.tsx:1588-1600; scss :87-104.
- **Conflict-resolution mode**: the same empty-box layout. One state shows "All conflicts have been resolved, you can build the graph now to start testing your changes" with button **"Build Graph"**. The other shows "Can't build graph as workspace contains merge conflicts, please resolve them before trying to build the graph again" with button **"Resolve Merge Conflicts"**. Both buttons are `btn--dark btn--conflict btn--important`: orange `#ca663a`, uppercase, 1.2rem / 500 (Explorer.tsx:1536-1567; LA/style/base/_button.scss:110-114, 184-199).

---

## 4. Editor tabs and the empty splash

### 4.1 Tab bar (LS/src/components/editor/editor-group/EditorGroup.tsx:505-674; LS/style/components/editor/_editor-group.scss:19-74; LL/src/application/TabManager.tsx; LL/style/application/_tab-manager.scss)

- `div.panel.editor-group > div.panel__header.editor-group__header`: `height: 3.4rem`, `background: --color-bg-panel #2d2d2d`,
  `padding: 0`, `z-index: 1`, plus the base header shadow.
- `div.editor-group__header__tabs` (flex 1, overflow hidden) holds `div.tab-manager`. That splits into
  `div.tab-manager__content`, which takes `width: calc(100% - 2.8rem)` and is `overflow-x: auto`. **The mouse wheel scrolls the strip horizontally** (`horizontalToVerticalScroll`).
  The 2.8rem on the right is a `ChevronDownIcon` toggler.
- **Tab** `div.tab-manager__tab` (_tab-manager.scss:31-118):
  - Flex, vertically centred, `cursor: pointer`.
  - Inactive: `background: --color-bg-panel-header #353535`, `color: #bbb`, `border-right: 0.1rem solid #2d2d2d`.
  - **Active**: `background: --color-bg-app #1e1e1e`, `color: #fafafa`. The active tab merges into the editor surface,
    as in VS Code. **There is no underline on editor tabs.**
  - While dragged: `filter: opacity(0.7)`.
  - Label `button.tab-manager__tab__label`: full height, `padding: 0 0.5rem 0 1rem`, nowrap, ellipsis. Its tooltip is the tab description.
  - Inside the label (EditorGroup.tsx:470-495), `div.editor-group__header__tab` contains:
    - The element icon (§3.3), or `GenericTextFileIcon` (BsTextLeft) for non-element tabs.
    - The label (`margin-left: 0.5rem`).
    - When two open tabs share a name, a **path chip** with the full path: Roboto Mono 1rem / 500, `#737373`, height 1.6rem, `padding: 0 0.5rem`, radius 0.2rem, margin-left 0.5rem.
  - **Close button** `tab-manager__tab__close-btn`: 2rem × 2rem, radius 0.2rem, `margin-right: 0.4rem`. It holds `TimesIcon`
    (FaTimes) at 1.2rem, `#ddd` (primary when visible). **It is `visibility: hidden` except on the active tab or on hover.**
    Hover background `#3e3e3e`. Tooltip "Close".
  - Pinned tabs replace the X with `PushPinIcon` (MdOutlinePushPin) at 1.4rem, `#737373`, tooltip "Unpin".
  - Middle-click closes a tab (TabManager.tsx:141).
  - Right-click menu: "Close", "Close Others" (disabled when fewer than 2 tabs), "Close All", a divider, then "Pin" or "Unpin" (TabManager.tsx:78-93).
  - **Dirty marker: none.** Tabs never show unsaved state. Unpushed changes show only as the `*` after the workspace name in the
    status bar (§7) and the counter on the activity bar (§2.1).
- **Overflow menu** `tab-manager__menu__toggler`: 2.8rem wide, `#bbb`, `border-left: 0.1rem solid #2d2d2d`, tooltip **"Show All Tabs"**.
  - It opens a `.tab-manager__menu` (min 15rem, max 30rem) listing every tab: label 1.3rem with ellipsis, an X close at 1.2rem, padding 0.4rem.
  - Active and hovered entries use `#264f77`.
- **View-mode switcher** (right of the tabs, element tabs only; EditorGroup.tsx:520-618; scss :143-165):
  - `editor-group__view-mode__type`: `width: 15rem`, `background: #2d2d2d`, `border-left/right: 0.1rem solid #1e1e1e`, `color: #ddd`, hover `#3e3e3e`.
  - Its label is 3.4rem tall with `padding: 0 0.5rem` and a **0.3rem yellow top border** (`#fbbc05`). The text is the current
    mode: **Form**, **JSON** or **Grammar** (LS/src/stores/editor/EditorConfig.ts:55-59). Tooltip "View as...".
  - The menu groups modes as **native** (blue `#007acc` group), **external format** and **file** (pink
    `--color-category-generation #d14664` groups). Groups are separated by 0.2rem.
- `.editor-group__content`: `background: #1e1e1e`, `height: calc(100% - 3.4rem)`, overflow hidden.

### 4.2 Empty editor splash (EditorGroup.tsx:146-230; scss :239-435)

- `div.editor-group__splash-screen`: fills the area, flex-centred column, `background: #1e1e1e`, no text selection.
  **The content is hidden entirely below 300 × 180 px** (EditorGroup.tsx:147-155).
- `__content`: `width: clamp(70%, 100rem, 95%)`, `height: 60%`, `max-width: 130rem`, `max-height: 84.4rem`, column `space-between`.
- **Upper half: cards** (`__content__cards`, 50%):
  - Three cards, **Rule engagement** (only if configured), **"Showcase Projects"** and **"Documentation"**, styled as on the setup page.
  - Each is `flex: 0 0 min(42rem, calc((100% - 6rem)/3))` with `aspect-ratio: 420/422`, `#2d2d2d` background and radius 1rem.
  - The card header is `clamp(1.4rem, 7.4cqw, 2.8rem)` on one line with ellipsis.
  - **Hidden at 800px wide or less.**
- **Divider**: 0.1rem, `#353535`.
- **Lower block: shortcuts** (`__content__actions`, 30%):
  - Header **"Essential Keyboard Shortcuts"**: bold, `#fafafa`, `font-size: min(2vw, 2vh)`, `margin-bottom: 3vh`.
  - Items in a **3-column grid** with `column-gap: 8rem; row-gap: 2rem`. Each item is a 2-column grid (label, then keys), `min-height: 3.4rem`.
  - Labels: 500, `font-size: min(1.5vh, 1.5vw)`, **`--color-text-link #007acc`**.
  - **Key caps** `.hotkey__key`: Roboto Mono 500, `padding: 0 0.7rem`, radius 0.3rem, margin `0 0.2rem`,
    `background: #595959`, `color: #bbb`, `font-size: min(1.5vh,1.5vw)`, `height: min(2vh,2vw)`.
    Between keys sits a `PlusIcon` (FaPlus) at `min(1vh,1vw)` in `#bbb` (LA/style/base/_common.scss:49-76; editor-group scss :422-434).
  - The items, in order:

    | Label | Keys |
    |---|---|
    | Open or Search for an Element | Ctrl + P |
    | Push Local Changes | Ctrl + S |
    | Open Showcases | F7 |
    | Go To Text Mode | F8 |
    | Compile | F9 |

- The viewer-mode splash shows only **"Open or Search for an Element"** with Ctrl + P (EditorGroup.tsx:110-144).

---

## 5. Grammar text editor ("Text Mode", F8)

### 5.1 Chrome (LS/src/components/editor/editor-group/GrammarTextEditor.tsx:1303-1386)

- The same `panel editor-group` frame.
- The tab strip holds one static tab, `.editor-group__text-mode__tab--active`, labelled **"Text Mode"**
  (`GraphEditGrammarModeState.headerLabel`, LS/src/stores/editor/GraphEditGrammarModeState.ts:93-95). It uses `background: #1e1e1e; color: #fafafa`,
  label `padding: 0 1rem`, `border-right: 0.1rem solid #1e1e1e`. The inactive variant would be `#252525` with `#bbb` (scss :79-103).
- Right-hand actions, each in `editor-group__text-mode__action` (`padding: 0.5rem 0.5rem 0.5rem 0`), so each button is about 2.4rem tall:
  - **"Compile"**: `btn--dark`, `width: 12rem`, accent `#08629e`, hover `#007acc`, tooltip "Compile (F9)".
  - **"Exit Text Mode"**: the same style, tooltip "Click to exit text mode and go back to form mode (F8)".
  - **"Advanced ▾"**: a pill borrowed from the query builder (LQB/style/_query-builder.scss:121-146). `height: 2.8rem; padding: 0 1rem`,
    radius 0.2rem, 500, `background: #08629e`. Label 1.2rem in `--color-light-grey-180 #dcdcdc`, `CaretDownIcon` with
    margin-left 1rem. Tooltip "Show Advanced Menu...".
  - The Advanced menu has **"Wrap Overflowing Words"** and **"Auto Fold Elements"**, each with a `CheckIcon` (FaCheck) slot that is filled when the option is on.
- Body: `PanelContent.editor-group__content` → PanelLoadingIndicator (shown while compiling) → `div.code-editor__container > div.code-editor__body`.
  The body is absolutely positioned and fills the area (LL/style/_code-editor.scss:61-68).

### 5.2 Monaco options (LCE/src/CodeEditorUtils.ts:51-78; GrammarTextEditor.tsx:885-898)

| Option | Value |
|---|---|
| `language` | `pure` |
| `theme` | **`default-dark`** (light theme: `github-light`) |
| `fontSize` | **14** (and CSS `.monaco-editor * { font-size: 1.4rem }`, LL/style/reset/_monaco-editor.scss:17-19) |
| `fontFamily` | **`'Roboto Mono'`** (loaded at weights 400 and 700 before any editor is created; CodeEditorUtils.ts:215-228) |
| `fontLigatures` | `true` |
| `tabSize` | 2, `detectIndentation: false` |
| `contextmenu` | `false` (Monaco's own right-click menu is off) |
| `copyWithSyntaxHighlighting` | `false` |
| `bracketPairColorization.enabled` | `false` |
| `fixedOverflowWidgets` | `true` |
| `automaticLayout` | `true` |
| `renderValidationDecorations` | `'on'` |
| `wordWrap` | from the setting (Advanced → Wrap) |
| `readOnly` | in viewer mode |
| `lineHeight`, `minimap`, `lineNumbers` | **not set**, so Monaco 0.52.2 defaults apply: minimap on (right side), line numbers on, line height derived from font size |

The gutter is widened as the document grows (`lineNumbersMinChars = max(floor(log10(lines)) + 3, 5)`, CodeEditorUtils.ts:160-170).

### 5.3 Theme `default-dark` (LCE/src/CodeEditorTheme.ts:29-45, 83-95)

The theme is `base: 'vs-dark', inherit: true, colors: {}`. It changes **no editor colours** (background, gutter, selection and
cursor all come from Monaco's built-in `vs-dark`) and adds token rules. The Pure tokenizer adds the postfix `.pure` to every
token (LCE/src/PureLanguageService.ts:86-87).

| Pure token | Matches (PureLanguageService.ts) | Foreground |
|---|---|---|
| `identifier` | default identifiers (:220-232) | **`#dcdcaa`** |
| `number`, `date`, `color` | numbers; `%2020-01-01`, `%latest`, `%12:00` (:335-355); `#a1b2c3` (:340) | **`#b5cea8`** |
| `package` | `a::b::` path prefixes (:266-282) | **`#808080`** |
| `parser` | `^###Name` section headers (:214-218) | **`#c586c0`** |
| `language-struct` | `import`, `native` (:172) | **`#c586c0`** |
| `multiplicity` | `[1]`, `[0..1]`, `[*]` after a type (:267-275) | **`#2d796b`** |
| `generics` | `<T>` after a type | **`#2d796b`** |
| `property` | `.name`, `name =`, `X.name` (:300-314) | **`#9cdcfe`** |
| `parameter` | `name:` (:316-319) | **`#9cdcfe`** |
| `variable` | `let x =` and `$x` references (:321-331) | **`#4fc1ff`** |
| `type` | the identifier in `pkg::Type`, `Type[1]`, `@Type`, `^Type` | **`#3dc9b0`** |
| `string.escape` | `\n` etc. inside `'…'` | **`#d7ba7d`** |
| `string.sql`, `white/identifier/operator.sql` | embedded SQL | `#ce9178`, `#d4d4d4` |

**Inherited from Monaco's built-in `vs-dark`.** These values are not in the repo; they are the monaco-editor 0.52.2
defaults, which the rebuild must copy:

- Token colours:

  | Token | Colour |
  |---|---|
  | `keyword` (Class, Enum, Mapping, let, true/false, extends, …; list at :89-139) | `#569cd6` |
  | `comment` (`//`, `/* */`, doc comments) | `#6a9955` |
  | `string` | `#ce9178` |
  | `delimiter` (`; , .`) | `#dcdcdc` |
  | `operator` (no vs-dark rule) | default `#d4d4d4` |
  | `invalid` | `#f44747` |

  **The Monarch `defaultToken` is `'invalid'`** (:85), so any character no rule matches renders red `#f44747`.
- Editor colours:

  | Element | Colour |
  |---|---|
  | Background | `#1e1e1e` (matches `--color-bg-app`) |
  | Default foreground | `#d4d4d4` |
  | Line numbers | `#858585` (active `#c6c6c6`) |
  | Selection | `#264f78` |
  | Inactive selection | `#3a3d41` |
  | Current-line border | `#282828` |
  | Cursor | `#aeafad` |
  | Indent guides | `#404040` (active `#707070`) |
  | Widgets | `#252526` |

- **Error markers**: `setErrorMarkers` sets severity Error. Monaco draws a red squiggle underline (`editorError.foreground #f14c4c`)
  and shows a red overview-ruler mark. An empty message is replaced with "(no error message)"; `endColumn + 1` because Monaco
  ranges are exclusive (CodeEditorUtils.ts:93-117).
- **Warnings** use a yellow squiggle (`#cca700`), with "(no warning message)" for an empty message (CodeEditorUtils.ts:119-143).
- Language configuration (PureLanguageService.ts:30-64):
  - Comments `//` and `/* */`; brackets `{} [] ()`.
  - Auto-closing pairs `{} [] () "" ''`.
  - Fold markers `//region` and `//endregion`.
  - Word separators ``` `~!@#%^&*()-=+[{]}\|;:'",.<>/? ``` (no `$`).

---

## 6. Panel group: Problems, Console and others (LS/src/components/editor/panel-group/PanelGroup.tsx:96-190; LS/style/components/editor/_panel-group.scss:22-182)

- `div.panel.panel-group`: absolutely positioned at 0/0, full width, `z-index: 1`, **`border-top: 0.1rem solid #353535`**.
- Header: `padding: 0 0 0 1rem; height: 3.4rem; background: --color-bg-app #1e1e1e` (overriding the default header background; the shadow stays).
  Content: `height: calc(100% - 3.4rem); background: #1e1e1e`.
- **Tabs** (`panel-group__header__tabs`, flex, 3.4rem):
  - Each tab is a `<button>`, `height: 3.4rem`, `margin: 0 1rem`, `padding: 0 0.5rem`, 1.2rem / 500.
  - Inactive: `color: #737373` with `border-bottom: 0.2rem solid #1e1e1e`, which is invisible against the background.
  - **Active**: `color: #ddd` with `border-bottom: 0.2rem solid #fbbc05`.
  - Tabs, in order (PanelGroup.tsx:69-95; ids in LS/src/stores/editor/EditorConfig.ts:48-53):

    | Tab | Extra |
    |---|---|
    | **CONSOLE** | beta badge |
    | **DEVELOPER TOOLS** | |
    | **PROBLEMS** | count badge |
    | **SQL PLAYGROUND** | beta badge, form mode only |

  - **Count badge**: `div.badge.panel-group__header__tab__title__problem__count` with the count lower-cased, height and
    line-height 1.6rem, 1rem / 500, `padding: 0 0.5rem`, **radius 0.8rem**, `background: #595959`, **`color: #ddd`**, margin-left 0.5rem
    (LA/src/badge/Badge.tsx:19-30; LA/style/components/_badge.scss:19-29; panel-group scss :81-87).
  - **Beta badge**: a 1.2rem circle in `#7695ff`, margin-left 0.5rem, holding a 1rem `SparkleIcon` in `#1e1e1e`. Tooltip "This is an experimental feature".
- **Header actions**: 3.6rem squares, svg **1.8rem** `#bbb`.
  - `ChevronUpIcon` (GoChevronUp), or `ChevronDownIcon` when maximised, with tooltip **"Toggle expand/collapse"**.
  - `XIcon` (GoX) with tooltip **"Close"**.
- **Problems content** (LS/src/components/editor/panel-group/ProblemsPanel.tsx:68-95):
  - **Stale banner**: `height: 3.8rem; padding: 1rem; background: #2d2d2d; border-left: 0.5rem solid #fbbc05`. Text:
    **"The following result might be stale - please run compilation (F9) to check for the latest problems"**.
  - **Empty**: **"No problems have been detected in the workspace."**, 2.8rem tall, `padding: 0 1rem`, `margin: 1rem 0`.
  - The list (PanelList, margin `1rem 0`) holds a `button.panel-group__problem` per problem (:39-66):
    - Full width, `height: 2.8rem; padding: 0 1rem`; hover `#3e3e3e`; tooltip is the full message.
    - Icon: `ErrorIcon` (VscError) in **`#f5584b`**, or `WarningIcon` (VscWarning) in **`#fbbc05`**, with `margin: 0 1rem 0 0.5rem`.
    - Message: `#bbb`, ellipsis.
    - Source, **text mode only**: `[Ln {line}, Col {col}]` in `#737373`, margin-left 0.5rem.
    - Stale rows: the message turns `#737373` and the cursor is default.
- The panel starts closed (size 0). Toggling it opens to 300px, it snaps at 100px, and maximise fills the editor height (EditorStore.ts:211-215; Editor.tsx:145-149).

---

## 7. Status bar (LS/src/components/editor/StatusBar.tsx:163-431; LS/style/components/editor/_status-bar.scss:19-231)

- `div.editor__status-bar`: **`height: 2.2rem (22px)`**, flex `space-between`, vertically centred, `padding: 0 0.5rem 0 1rem`,
  text `#f3f3f3` at the inherited 1.4rem.
- **Background depends on state:**

  | State | Background |
  |---|---|
  | Normal | **`--color-status-info #007acc`** (VS Code blue) |
  | **Conflict-resolution mode** | `.editor__status-bar--conflict-resolution` → **`--color-conflict #ca663a`** (orange) |
  | Lazy text editor route (`/text/...`) | `.lazy-text-editor__status-bar` → `--color-status-success-bg #014321` (dark green) |
  | Text mode (F8) | **no change** in the normal editor; only the toggle icon lights up |

- **Left side** (`__left > __workspace`, default cursor):
  1. `CodeBranchIcon` (FaCodeBranch).
  2. `button.__workspace__project`: the project name (or "unknown"), padding 0 0.5rem. Tooltip "Go back to workspace setup using the specified project".
  3. A literal **`/`**.
  4. `button.__workspace__workspace`: `[patch / {id} / ]{workspaceId}`, plus a **`*` when there are unpushed changes**.
     Tooltip "Go back to workspace setup using the specified workspace".
  5. Optional status chips, `__workspace__status__btn`: 1.6rem tall, 1rem / 500, `background: --color-state-disabled-on-accent #00000061`
     (a dark translucent pill), radius 0.3rem, `padding: 0 0.5rem`, `margin: 0 0.5rem`:
     - **"OUT-OF-SYNC"**, tooltip "Local workspace is out-of-sync. Click to see incoming changes to your workspace."
     - **"OUTDATED"**, tooltip "Workspace is outdated. Click to see latest changes of the project".
  6. **Problems button** `__problems`: `padding: 0 0.5rem`, hover `--color-state-hover-on-accent #ffffff12`.
     It shows `ErrorIcon` (VscError), the error count (0 or 1), `WarningIcon` (VscWarning) and the warning count.
     The counters are 1.2rem with padding 0 0.5rem. Tooltip "Error: 1, Warnings: N" (or "Warnings: N"). Clicking opens Problems.
- **Right side**:
  1. Sync status text, default cursor. Depending on state:

     | State | Text |
     |---|---|
     | Graph or change detection failed | "change detection halted" |
     | Indexing | "building indexes..." |
     | Starting | "starting change detection..." |
     | Pushing | "pushing local changes..." |
     | Updating configuration | "updating configuration..." |
     | Unpushed changes | "**N unpushed changes**" |
     | Clean | "no changes detected" |

     In conflict mode the texts are: "submitting conflict resolution...", "has unresolved merge conflicts", "conflict resolution not accepted" and "all conflicts resolved".
  2. Push button `__push-changes__btn`: `CloudUploadIcon` (IoCloudUploadOutline) at **1.6rem**, `padding: 0 0.5rem; margin-left: 0.5rem`.
     Disabled colour `#00000061`. While pushing it **jiggles** (`translateY ±0.1rem`, 0.3s). Tooltip "Push local changes (Ctrl + S)".
     In conflict mode the button is `SyncIcon` (GoSync) with tooltip "Accept conflict resolution".
  3. Then a row of `button.__action` buttons, each `width: 3rem`, full height, hover `#ffffff12`:

     | Icon | Tooltip | Notes |
     |---|---|---|
     | `FireIcon` (FaFire) | "Generate (F10)" | bobs while running (`flame-rise`, 0.5s) |
     | `TrashIcon` (FaTrash) | "Clear generation entities" | |
     | `HammerIcon` (FaHammer) | "Compile (F9)" | while compiling it **wiggles** (rotate −7° to 10°, 0.5s, origin bottom-left) |
     | `TerminalIcon` (FaTerminal) | "Toggle panel (Ctrl + `)" | toggler (see below) |
     | `HackerIcon` (FaUserSecret) | "Toggle text mode (F8)" | toggler; lit in text mode |
     | `ShieldIcon` (FaShieldAlt) | "Re-authenticate with SDLC (preserves local state)" | only when popup re-auth is enabled |
     | `AssistantIcon` (MdAssistant) | "Toggle assistant" | toggler |

     **Togglers** are `#00000061` (dimmed) when off and `#f3f3f3` when on.
     Disabled buttons use `#00000061`.
- On the setup page the bar has the same blue background, an empty left side, and only the assistant toggle on the right (WorkspaceSetup.tsx:884-905).

---

## 8. Side-bar panels

### 8.1 Local Changes (LS/src/components/editor/side-bar/LocalChanges.tsx:220-460; LS/style/components/editor/side-bar/_local-changes.scss)

- Header **"LOCAL CHANGES"**. Actions:

  | Icon | Tooltip | Notes |
  |---|---|---|
  | `DownloadIcon` (FaDownload) | "Download local entity changes" | |
  | `UploadIcon` (FaUpload) | "Upload local entity changes" | |
  | `RefreshIcon` (MdRefresh) | "Refresh" | 1.7rem; spins while refreshing |
  | `CloudUploadIcon` | "Push local changes (Ctrl + S)" | 1.6rem; jiggles while pushing |
  | `CloudDownloadIcon` | "Pull remote workspace changes" | |

- Body: a PanelLoadingIndicator, then **three vertically stacked, resizable sub-panels** (`reflex horizontal`, each min 28px;
  the first starts at 600px; splitter lines `#2d2d2d`):
  1. **"CHANGES"**: info "All local changes that have not been yet pushed with the server", count pill. Its rows are diff items (below).
  2. **"INCOMING REMOTE REVISIONS"**: info "All incoming remote revisions since last syncing of workspace". Rows read `{message}` in `#bbb`
     followed by the committer name (1.2rem `#737373`, margin-left 1rem), with ellipsis.
  3. **"INCOMING REMOTE CHANGES"**: conflicts first, then a 1px `#353535` separator (`diff-panel__item-section-separator`), then diffs.
- **Diff row** `EntityDiffSideBarItem` (LS/src/components/editor/editor-group/diff-editor/EntityDiffView.tsx:48-82; side-bar/_diff-panel.scss):
  - A `side-bar__panel__item` (22px).
  - Left side: the **entity name** (500, `#bbb`, padding-right 0.5rem; deletes get a 0.1rem line-through), then the **path**
    (1.2rem `#737373`), all with ellipsis.
  - Right side: a 2.2rem cell with the change letter at 1.2rem / 500. The letters and colours are:

    | Change | Letter | Colour |
    |---|---|---|
    | Create | **N** | `#34a853` |
    | Modify | **M** | `#fbbc05` |
    | Rename | **R** | `#fbbc05` |
    | Delete | **D** | `#f5584b` |
    | Conflict | | `#ca663a` |

    Letters are from legend-server-sdlc/src/models/comparison/EntityDiff.ts:27-40.
  - When the row is selected (background `#264f77`), all of its text turns `#f3f3f3`.

### 8.2 Workspace Review (LS/src/components/editor/side-bar/WorkspaceReview.tsx:198-330; side-bar/_workspace-review.scss)

- Header **"REVIEW"**. Actions: `RefreshIcon` "Refresh" (1.7rem, spins while refreshing) and `TimesIcon` "Close review" (1.6rem; disabled when there is no review).
- **No review yet**: a form row `.workspace-review__title` (padding 0.5rem, flex):
  - Input `input--dark`, placeholder **"Title"**, `height: 2.8rem`, `padding: 0.5rem`, `#2d2d2d` background with matching border
    (the side-bar override makes it `#353535`). It takes the full width minus 3.3rem.
  - Then a **2.8rem square accent button** `btn--dark btn--sm` with `PlusIcon`. Its tooltip is "Create review", or a reason
    when it is disabled. It turns red (`btn--error`, `#f5584b`) if the workspace has snapshot dependencies.
- **Review exists**:
  - The title row shows the review title as a link-styled button (`#ddd`, primary on hover) with an `ExternalLinkSquareIcon` cell at
    the right. Tooltip "See review detail".
  - Then a square accent button holding `TruncatedGitMergeIcon` (FiGitMerge) at 1.7rem, tooltip "Commit review" (or the reason it is disabled).
  - Under it, the review status **"created {N minutes} ago"**: 1.1rem, `#737373`, `margin: 0 0.5rem 0.6rem`.
- Below that comes the **"CHANGES"** sub-panel (info "All changes made in the workspace since the revision the workspace is
  created", count pill, diff rows as in §8.1), with `height: calc(100% - 3.4rem)`.

### 8.3 Project Overview (LS/src/components/editor/side-bar/ProjectOverview.tsx:1170-1289; side-bar/_project-overview.scss)

- Header **"PROJECT"**. Actions: `ShareIcon` (FaShare) "Share...", and `ExternalLinkIcon` (FaExternalLinkAlt) "Go to project in underlying VCS system".
- The content is flex row: a **vertical tab rail** followed by the active sub-panel (`width: calc(100% - 3rem)`).
- **Tab rail** `project-overview__activity-bar` (scss :53-96):
  - `width: 3rem`, `padding-top: 2.8rem`, `background: #353535`, left and right borders 0.1rem `#2d2d2d`.
  - Items are rotated text (`writing-mode: vertical-lr; transform: rotate(180deg)`, 1.2rem) with `padding: 1rem 0`,
    `border-left: 0.2rem solid #353535` and colour `#737373`.
  - **Active**: `#ddd` text with a **0.2rem yellow left border** (`#fbbc05`).
  - Labels are the raw enum values (ProjectOverviewState.ts:51-57): **OVERVIEW**, **RELEASE** (hidden in embedded mode),
    **VERSIONS**, **WORKSPACES**, **PATCH**. Their tooltips are Overview, Release, Versions, Workspaces and Patch.
- **OVERVIEW**: a sub-panel header with an "Update" pill on the right (`project-overview__update-btn`, 8rem slot).
  - The pill is 2.2rem tall, `#08629e`, radius 0.2rem, with 1.2rem / 500 `#f3f3f3` text **"Update"** and tooltip "Update Project".
  - The form has **Project Name** (an input), **Description** (a textarea with a prompt) and **Tags** (the list editor from §9.4 with
    PencilIcon edit and TimesIcon remove; "Add Value").
- **RELEASE** (:370-526):
  - A row with a 9.4rem-tall textarea, placeholder **"Release notes"** (`#2d2d2d` background, padding 0.5rem, line-height 2rem).
  - To its right, a column of three 5rem × 2.8rem buttons, 1.1rem / 500, 0.5rem apart:
    - **MAJOR**: `btn--caution`, pink `#b33659`. Tooltip "Create a major release which comes with backward-incompatible features".
    - **MINOR**: accent. Tooltip "…backward-compatible features".
    - **PATCH**: accent. Tooltip "…backward-compatible bug fixes".
  - Below: a **"LATEST RELEASE"** sub-panel (6rem tall) showing `{version}` in `#bbb` and `{notes}` in 1.2rem `#737373`.
    Tooltip "See version". If there is none, the panel reads "This project has no release".
  - Then **"COMMITTED REVIEWS"**: info "All committed reviews in the project since the latest release", count pill,
    rows `{title}` plus the author. Tooltip "See review".
- **VERSIONS**: a header with a count pill; rows are `side-bar__panel__item` showing `{version id}` (`#bbb`) followed by notes
  (1.2rem `#737373`, margin-left 1rem). Tooltip "See version".
- **WORKSPACES**: a header with a count pill; rows show a user or users icon, the workspace id and an optional patch chip
  (7rem × 1.8rem, `#595959` background, `#1e1e1e` text, 1rem / 500). Right-click gives **"Delete"**.

### 8.4 Project configuration editor: dependency list (LS/src/components/editor/editor-group/project-configuration-editor/ProjectConfigurationEditor.tsx:852-930; ProjectDependencyEditor.tsx:1444-1600; LS/style/components/editor/_project-configuration-editor.scss)

- This opens as an editor **tab**. The background is `#1e1e1e`.
- **First header**: label chip **"project configuration"**, then the project name in bold. On the right is an **"Update"** button,
  `10rem × 2rem`, `#08629e` (hover `#007acc`, disabled `#595959`), radius 0.2rem, 1.2rem. It is disabled when nothing has changed.
- **Second header**: a tab strip (2.8rem). Tabs: **"Project Structure"**, **"Project Dependencies"**, **"Platform Configurations"**,
  **"Advanced"** (`prettyCONSTName` of the enum values, LS/src/stores/editor/editor-state/project-configuration-editor-state/ProjectConfigurationEditorState.ts:53-58,
  LSH/src/format/FormatterUtils.ts:77-84).
  - Tabs: `padding: 0 1rem`, `#bbb`, `border-right: 0.1rem solid #2d2d2d`. The active tab gets a 0.2rem yellow underline (scss :81-117).
  - On the right is a `PlusIcon` action with tooltip "Add project dependencies".
- **Each dependency row** `div.project-dependency-editor` (flex, `margin-top: 0.5rem`):
  - A project react-select (`flex: 1 0 auto`) and, 0.5rem to its right, a version react-select. The version placeholder is
    "Choose project", "Select version", "Fetching project versions" or "No project version found. Please create a new one."
  - An inline exclusions selector, placeholder "Add exclusion...".
  - A **"Go to... ▾"** button (`btn--medium`: `#353535` background, `#3e3e3e` on hover, 2.8rem tall, padding 0 0.5rem, caret 1.2rem).
    Its menu has "Project" and "SDLC project".
  - A **2.8rem square pink X** (`btn--dark btn--caution`, `#b33659`) with tooltip "Close". When disabled it turns `#737373`.
  - Below each row, an indented exclusions list. Each entry has a pink X with tooltip "Remove exclusion".
- Progress text (Roboto Mono 1.2rem `#737373`, margin 1rem, line-height 2rem): "Updating configuration...", "Fetching dependency versions",
  "Validating dependencies and compiling..." or "Updating project dependency tree and potential conflicts".

---

## 9. Dialogs, buttons, inputs and selects

### 9.1 MUI Dialog shell (LA/src/dialog/Dialog.ts:17 re-exports MUI; overrides in LA/style/reset/muiOverrides.scss:196-207)

- **Dialogs open at the top, not centred.** `.MuiDialog-root { margin-top: 3.4rem }` and `.MuiDialog-scrollPaper { align-items: flex-start }`.
  The paper has `margin: 0; max-width: initial`.
- Everything else is the MUI 7.3.4 default: a backdrop of `rgba(0,0,0,0.5)`, a paper with 4px radius and an elevation-24 shadow,
  and `max-height: calc(100% - 64px)`.
- Blocking and action alerts **are** vertically centred (`blocking-alert__container { align-items: center }`, LAPP/style/components/_blocking-alert.scss:22-28).
- The modal box inside the paper is `div.modal` (LA/src/dialog/Modal.tsx:21-32). With `darkMode` it gets **`.modal--dark`**:
  `background: #1e1e1e; color: #ddd; border: 0.1rem solid #08629e` (LA/style/base/_modal.scss:132-136).
- **`.search-modal`**, the most common size: `width: 60rem; padding: 1rem`, with the title's `margin-bottom: 1rem`.
  `.search-modal__actions` is a right-aligned flex row (_modal.scss:142-167).
- Other sizes: `.editor-modal` is `80vw × 80vh`, padding 0, with body `calc(100% - 8.6rem)` (:169-198). The setup dialogs are 75rem (§1.6, §1.7).

### 9.2 Generic modal header, body and footer (LA/src/dialog/Modal.tsx:34-139; _modal.scss:19-128)

- `ModalHeader` → `div.modal__header`:
  - `height: 3.6rem`, **`background: --color-accent #08629e`**, `padding-left: 1rem`, flex space-between.
  - Title `div.modal__title > div.modal__title__label`, **toTitleCase**d, 1.8rem / 700.
  - Optional title icon, coloured `--color-state-disabled-on-accent`, margin-right 0.7rem.
  - Header actions are 2.4rem squares (margin-right 0.5rem) with 1.6rem svgs in `#f3f3f3`.
- Many Studio dialogs skip the header and use a bare `div.modal__title` (1.8rem / 700) inside a `.search-modal`.
- `ModalBody` → `div.modal__body`: `position: relative; padding: 2rem`.
- `ModalFooter` → `div.modal__footer`: `border-top: 0.1rem solid #2d2d2d`, `height: 5rem`, `padding-right: 1rem`, flex, right-aligned.
  - Footer `.btn`: `height: 3.6rem`, radius 0.2rem, `#f3f3f3` text.
  - Optional italic `__footer__status` text to the left of the buttons.
- `ModalFooterButton` (Modal.tsx:82-139) renders `button.btn.modal__footer__btn.btn--dark`. Its text goes through **prettyCONSTName**
  unless `formatText=false`, so "CREATE_PROJECT" becomes "Create Project".
  - **Primary**: `#08629e`, hover `#007acc`.
  - **Secondary** (`--secondary`): `#353535`, hover `#3e3e3e`. A footer button's svg gets margin-right 0.5rem and 1.7rem.

### 9.3 Buttons (LA/style/base/_button.scss)

| Class | Look | Cite |
|---|---|---|
| `.btn` | flex-centred, `padding: 1rem`, `#252525` background, `#fafafa` text; `.btn + .btn` gets margin-left 0.5rem; disabled is `#353535` with `#737373` text | :30-54 |
| `.btn--dark` (**the primary button**) | radius 0.1rem, `#08629e` background, `#f3f3f3` text; hover `#007acc`; disabled `#595959` with `#737373` text; focus outline 1px `#08629e` with 1px offset | :117-137 |
| `.btn--medium` (secondary chip) | radius 0.1rem, `#353535` background, `#fafafa` text; hover `#3e3e3e`; disabled `#595959` | :140-162 |
| `.btn--dark.btn--caution` | `#b33659`, hover `#c5375f`, disabled `#595959` | :166-181 |
| `.btn--dark.btn--conflict` | `#ca663a`, hover `#c95f03` | :184-199 |
| `.btn--error` | `#f5584b` background, `#f3f3f3` text | :208-212 |
| `.btn--important` | uppercase, 1.2rem / 500 | :110-114 |
| `.btn--wide` | `height: 2.8rem; padding: 0 1rem` | :56-61 |
| `.btn--sm` | `2.8rem × 2.8rem` (icon square) | :63-67 |
| `.btn--icon--small` | `padding: 0.5rem`, radius 0.1rem, `#595959` background (hover `#3e3e3e`) | :72-83 |

### 9.4 Inputs and form sections (LA/style/base/_input.scss; LA/style/base/_panel.scss:298-718; LA/src/layout/Panel.tsx:334-470)

- **`.input`**: `height: 2.8rem; width: 100%; padding: 0 0.5rem`.
- **`.input--dark`** (used almost everywhere in dark mode):
  - `background: #2d2d2d; color: #fafafa; border: 0.1rem solid #353535`, placeholder `#737373`.
  - Hover border `#595959`, focus border `#08629e`, disabled text `#737373`.
- **`.input--caution`**: border `#fbbc05`, background `#342a18`.
- **Form section** `.panel__content__form` (padding 2rem, max-width 80rem); `__section` (padding 0 1rem; consecutive sections 2rem apart):
  - **Label** `__section__header__label`: 500, `#fafafa`, line-height 2rem, margin-bottom 0.5rem. It is `capitalize(name)`.
  - **Prompt** `__section__header__prompt`: 1.4rem, `#bbb`, line-height 2rem, margin-bottom 0.8rem.
  - **Text input** `__section__input`: `height: 2.8rem; padding: 1rem`, radius 0.1rem, border 1px `#353535` (focus `#08629e`),
    `#2d2d2d` background, `#ddd` text. It is `max-width: 45rem` unless it is full width (`.panel .input--small`).
  - **Textarea** `__section__textarea`: `height: 8rem`, max-width 45rem, padding 1rem, line-height 2rem, no resize, same colours.
  - **Validation error** `input-group__error-message`: an absolutely positioned box just under the input (`top: calc(100% - 0.2rem)`),
    line-height 2.2rem, 1.2rem, `padding: 0 1rem`, `background: #ff00001a`, `border: 0.1rem solid #f5584b`,
    radius `0 0 0.1rem 0.1rem`, `#fafafa` text.
  - **Boolean toggle** `PanelFormBooleanField`: a 2rem `CheckSquareIcon` (FaCheckSquare) when on, in accent `#08629e` (hover `#007acc`).
    When off it is `SquareIcon` (**FaSquare**, solid) in `#737373` (hover `#bbb`). The prompt sits to the right (margin-left 0.8rem,
    `#bbb`, line-height 2rem); the whole row is clickable (Panel.tsx:440-468; _panel.scss:480-539).
  - **List editor** (tags):
    - Rows are 1.3rem `#bbb` with padding-left 0.3rem and gap 1rem. Hover `#3e3e3e` reveals the actions: 2.2rem `PencilIcon` (MdModeEdit)
      and `TimesIcon`, `#bbb`, white on hover.
    - Add and edit rows hold a 2.2rem input plus **"Save"** (accent) and **"Cancel"** (`#353535`) buttons at 1.2rem with padding 0 1rem.
    - An **"Add Value"** button sits under the list (margin-top 0.8rem).
    - An empty list shows a 4rem dashed-bordered (0.2rem `#353535`) box with 500 `#737373` text.
    - _panel.scss:545-696.

### 9.5 Selects (react-select 5.10.1 through LA/src/autocomplete/CustomSelectorInput.tsx; styles LA/style/components/_selector-input.scss)

- In dark mode the class prefix is **`selector-input--dark`** (CustomSelectorInput.tsx:165-166, 318-323).
- **Control**: `height: 2.8rem`, `border-radius: 0`, `background: #2d2d2d`, `border: 0.1rem solid --color-border-strong #595959`, text `#ddd`.
  - Hover and focus border **`#007acc`**, with no box-shadow.
  - Disabled: the border matches the background, the dropdown indicator is hidden, placeholder `#737373` (scss :195-231).
- **Value container**: `height: 2.6rem; padding: 0 0.5rem`, text `#ddd`. Placeholder `#737373`, nowrap.
- **Indicators**: 1.6rem wide, `#ddd`.
  - The separator is coloured like the background (invisible).
  - **Dropdown**: `CaretDownIcon` (FaCaretDown) at 1.3rem in a 1.5rem cell.
  - **Clear**: `TimesIcon` in a 2.6rem cell.
  - **Loading**: `CircleNotchIcon` (FaCircleNotch) spinning (1s), in accent `#08629e` (CustomSelectorInput.tsx:167-196).
- **Menu**: portalled to `<body>` at `z-index: 9999`. Background and border `#2d2d2d`, text `#ddd`, `margin: 0`.
  - It is virtualised by react-window with **row height 3.5rem (35px)** and **at most 6 rows visible** (:74-117).
  - **Options**: `padding: 0.8rem 1.2rem` (base rule). Focused `--color-accent-subtle #7f7ab124`; selected and active `#264f77` with `#fafafa` text;
    disabled `#737373`.
  - **No match**: "No match found", 2.8rem tall, 1.2rem / 500, `#737373`, padding 0.5rem (:120-130; scss :164-176).
  - Multi-value chips: `#595959` background with `#1e1e1e` text; the remove button hovers `#3e3e3e`.
- **Error state** (`.selector-input--has-error`): red border `#f5584b`; the value container and indicators get a `#ff00001a` tint with red icons.

### 9.6 Loading bar, blank-panel and drop-zone placeholders

- **PanelLoadingIndicator** (LA/style/components/_panel-loading-indicator.scss:17-79): a 0.2rem (2px) tall strip across the top of the
  panel. A short `#08629e` segment (20–50px wide) slides left to right over **2.5s linear infinite**. It is `display: none` when idle.
  Inside modals it is offset by −2rem/−2rem so it sits on the border.
- **BlankPanelContent** (LA/src/layout/BlankPanelContent.tsx:27-72; _panel.scss:191-211): centred text, 700, `#737373`, `padding: 0 5rem`,
  line-height 1.8rem. The text auto-hides (visibility) if it does not fit with 20px padding.
- **BlankPanelPlaceholder** (LA/src/layout/BlankPanelPlaceholder.tsx; LA/style/components/_blank-panel-placeholder.scss), shown for empty lists:
  - Bold text in `#737373`, then a **10rem × 10rem dashed box** (0.3rem dashed `#2d2d2d`, radius 0.3rem) holding a 4rem icon:
    `AddIcon` (MdAdd), `EditIcon` (MdEdit) or `VerticalAlignBottomIcon` (for drag-and-drop).
  - On hover the dash and icon turn `#353535`.
  - It fades in over 0.1s, and parts hide below 50px.

### 9.7 Other dialogs in the editor

- **New Element** (LS/src/components/editor/side-bar/CreateNewElementModal.tsx:775-829):
  - Box: `form.modal.modal--dark.search-modal` (60rem wide, `#1e1e1e`, accent border, padding 1rem).
  - Title **"Create a new {Type}"**, title-cased, falling back to "element". Example: "Create a new Class".
  - An optional type select (dark react-select), disabled when only one type applies.
  - A name input (`input--dark explorer__new-element-modal__name-input`, `height: 2.8rem; width: 100%; margin-bottom: 1rem`).
    Placeholder **"Enter a name, use :: to create new package(s) for the {type}"**.
  - Per-type driver selects. Examples: the runtime placeholder is "Choose a compatible runtime..."; the data product has a
    "Title" input with placeholder "Choose a title for this Data Product to display in Marketplace".
  - Actions, right-aligned: **"Cancel"** and **"Create"**, both `btn btn--dark` (padding 1rem, accent). Create is disabled when the name
    is empty, the element already exists, or the driver state is invalid.
- **Rename Element** (Explorer.tsx:266-292): `form.modal.modal--dark.search-modal`, title **"Rename Element"**, an input with
  placeholder **"Enter element path"** plus the inline error box, and actions **"Cancel"** and **"Rename"**.
- **Open Element (Ctrl+P)** (LS/src/components/editor/command-center/ProjectSearchCommand.tsx:107-170; LS/style/components/editor/command/project-search.scss):
  - A non-blocking dialog with `Modal.search-modal`.
  - The row starts with a type-filter button: a 3.5rem cell with `MoreHorizontalIcon` (MdMoreHoriz) or the chosen type's icon,
    plus a 1.5rem caret cell. Both have `#2d2d2d` background and `#353535` borders, 2.8rem tall. Tooltip "Choose Element Type...".
  - Then a dark react-select with placeholder **"Search elements by path"** (or "Search {type} by path").
- **Blocking alert** (LAPP/src/components/BlockingAlert.tsx:32-64; LAPP/style/components/_blocking-alert.scss):
  - A centred `modal--dark blocking-alert` with padding 0 and a loading bar.
  - Body padding 2rem, line-height 2.2rem, justified. The message is centred.
  - An optional prompt line: 1.2rem / 500, `--color-text-link #007acc`, centred.
  - It cannot be dismissed.
- **Action alert / confirm** (LAPP/src/components/ActionAlert.tsx:39-130):
  - `form.modal.search-modal.modal--dark.blocking-alert.blocking-alert--{standard|caution|error}`.
  - It has an accent header only when there is a title.
  - Body: summary text in 500 `#bbb`, then a prompt line (1.3rem / 500, `#007acc`, margin-top 1rem; **pink `#d14664` for caution**).
  - The border is accent for standard and **pink `#b33659` for caution**. The header stays blue in caution because the CSS targets the
    misspelled `.mode__header` (_blocking-alert.scss:74-84).
  - Footer buttons are `btn btn--dark` at 2.8rem. `PROCEED_WITH_CAUTION` actions use `btn--caution` (pink). The default action is the
    submit and is auto-focused. With no actions, a single "Cancel" appears.
  - Canonical example (Editor.tsx:180-199): "You have unpushed changes. Leave anyway?" with buttons **"Leave this page"** (pink) and
    **"Stay on this page"** (blue, default).

---

## 10. Notifications and toasts (LAPP/src/components/NotificationManager.tsx:40-191; LAPP/style/components/_notification.scss; LAPP/src/stores/NotificationService.ts)

- **One toast at a time.** It is an MUI Snackbar anchored **bottom-right**, at **`bottom: 3rem; right: 1rem`**, which keeps it clear of the 2.2rem status bar.
- **Content** `.notification__content`: `background: --color-bg-elevated #252525`, `color: #fafafa`, `border-radius: 0.3rem`, items aligned to the top.
  The font is overridden to 1.2rem (muiOverrides.scss:187-189). MUI's default padding (6px 16px) and elevation-6 shadow remain.
- Layout: `[icon] [message text] [copy?] | [expand ▴/▾] [✕]`.
  - **Icon** (1.6rem, padding-top 0.2rem, padding-right 1rem), by severity:

    | Severity | Icon | Colour |
    |---|---|---|
    | info | `InfoCircleIcon` (FaInfoCircle) | `--color-status-info #007acc` |
    | success | `CheckCircleIcon` (FaCheckCircle) | `#34a853` |
    | warning | `ExclamationTriangleIcon` (FaExclamationTriangle) | `#fbbc05` |
    | error | `TimesCircleIcon` (FaTimesCircle) | `#f5584b` |
    | illegal state | `BugIcon` (FaBug) | `#f5584b` |

  - **Text**: a single line with ellipsis, `max-width: 60rem; max-height: 20rem`. **Clicking it copies the message**
    (tooltip "Click to Copy"; the text press state is `#2d2d2d`).
    When expanded it becomes `white-space: pre-line; width: 60rem; overflow: auto` and appends the details.
  - **Copy button** (only when there are details): `CopyIcon` (FaRegCopy), margin-left 8px, tooltip "Copy message and trace".
    Its hover colour references an undefined `--primary-color` token, so it has no visible hover.
  - **Actions** (`padding: 0.8rem 0 0.8rem 1rem`): 2rem-wide icon buttons in `#bbb` (white on hover):
    `ChevronUpIcon` "Expand" or `ChevronDownIcon` "Collapse", then `TimesIcon` "Dismiss".
- **Auto-hide** (NotificationService.ts:26-171):
  - Info, success, warning and illegal-state notifications hide after **6000 ms**.
  - **Errors never auto-hide** because `notifyError` passes no duration, which becomes `null`. `DEFAULT_ERROR_NOTIFICATION_HIDE_TIME = 10000`
    is defined but unused.
  - Clicking elsewhere does **not** dismiss a toast; only the timeout or ✕ does (NotificationManager.tsx:98-108).
  - A new message replaces the current one (keyed by message).

---

## 11. Global summary

- Base font is Roboto at 1.4rem = 14px with line-height 1; mono is Roboto Mono; letter icons are Raleway 900. 1rem = 10px.
- The app has three tiers of greys: `#1e1e1e` app/editor, `#2d2d2d` panels, `#353535` headers and inactive tabs, `#3e3e3e` chrome
  (activity bar) and hover, `#252525` menus and toasts, `#595959` tags and pills.
- **Two accents:**
  - **Interactive blue** `#08629e`, hovering to `#007acc`. It is used for buttons, modal headers and focus borders.
  - **Status-bar blue** `#007acc`.
- **One active indicator: yellow `#fbbc05`**, used for the panel-group tab underline, the project-config and create-project tab
  underlines, the view-mode top border and the project-overview rail. **Editor tabs and the activity bar do not use it.**
- Scrollbars are 8px with a translucent white thumb. There are no focus outlines except on `.btn--dark`. There are no transitions.
- Dark is the default; there is a light toggle that uses `default-light`; Monaco switches to `github-light` with it.

---

## Build list

### Per region: what the rebuild must reproduce

1. **Workspace setup**
   - A 50px chrome-coloured left strip holding a menu and the sun/moon toggle, and a 22px blue status bar with only the assistant toggle.
   - A centred rounded card (`#2d2d2d`, radius 10px, soft shadow) sized in vh/vw, with:
     - the favicon logo and "Welcome to Legend Studio" in Roboto Condensed 700;
     - recent-workspace tiles (18rem columns, horizontal scroll, hover-revealed ✕, relative time);
     - two selector rows, each with a 4vh icon cell (search or branch) plus a dark select whose caret is in an accent-blue square;
     - the "Need to create a new workspace?" link;
     - the accent "Go →" button (255:61 aspect);
     - the "OR" divider (3px lines);
     - the "Create New Project" button.
   - Three doc cards below, hidden at 800px or less.
   - Create Workspace dialog: 75rem, accent border, name, source select defaulting to HEAD, a Group toggle that is on by default, and "Create".
   - Create Project dialog: 75rem, Create/Import tabs with a yellow underline, the form fields and tags list, and a 36px "Create".
2. **Editor shell**
   - Flex row: 50px activity bar, then a sidebar (300px initial, resizable, snap 150), then the editor.
   - The editor is a vertical split: tabs/editor above, panel group below (closed initially, 300px default).
   - Resize handles are 4px transparent and turn `#595959` on hover; the panel-group splitter line is 1px `#3e3e3e`.
   - The status bar is 22px.
   - Activity bar: 50×50 buttons, 20px muted icons (23–24px for the noted ones), white when hovered or active, **no active bar**,
     numeric counter and coloured-dot indicators, and the beta sparkle badge.
   - Pinned bottom buttons: showcases, theme, settings.
   - Sidebar header: 34px, `#2d2d2d`, UPPERCASE 12px / 500 title, 28px-wide icon actions.
3. **Explorer**
   - A 28px sub-header: the "workspace" chip, the workspace id in bold, and 5 actions (import, config, +, collapse, search).
   - 22px rows, 1rem per level indent, a 40px icon block (10px chevrons plus folder or type icon), hover `#7f7ab124`, selected `#264f77`.
   - Coloured letter and svg type icons as in §3.3; tinted folders for generated, system and dependency roots.
   - The yellow "config" row.
   - The context menu (`#252525`, 28px items, hover `#264f77`) and the "+" menu with blue vertical category labels.
   - Empty, loading (mono progress text plus a 2px bar) and failure states.
4. **Tabs and splash**
   - A 34px `#2d2d2d` strip. Tabs are `#353535` with muted text; the active tab is `#1e1e1e` with white text and no underline.
   - The ✕ is hidden until hover or active; middle-click closes.
   - A path chip appears on duplicate names; no dirty marker.
   - The wheel scrolls tabs horizontally; a "Show All Tabs" chevron menu; the view-mode box (15rem, yellow top border).
   - Splash: 3 cards, a divider, "Essential Keyboard Shortcuts" with link-blue labels, and grey mono key caps with + glyphs, in a 3-column grid.
     It is hidden when smaller than 300×180.
5. **Grammar editor**
   - A "Text Mode" tab, two 12rem accent buttons ("Compile", "Exit Text Mode") and an "Advanced ▾" pill.
   - Monaco on `vs-dark` with Roboto Mono 14px, tab size 2, ligatures, no context menu, minimap and line numbers at their defaults.
   - The Pure token colours in §5.3, including **default token = invalid (red)**; red and yellow squiggles for markers.
6. **Panel group**
   - A 34px `#1e1e1e` header with a 1px top border.
   - Uppercase 12px / 500 tabs, grey, turning `#ddd` with a 2px yellow underline when active; a pill count badge on PROBLEMS; beta dots.
   - Chevron and X actions at 18px.
   - Problem rows 28px with a VscError or VscWarning icon, ellipsis message and `[Ln, Col]` in text mode; the stale banner with a yellow left bar;
     the empty-state text.
7. **Status bar**
   - 22px, `#007acc`, or orange `#ca663a` in conflict mode.
   - Left: branch icon, project / workspace with `*` for unpushed changes, OUT-OF-SYNC and OUTDATED pills, the error/warning counts.
   - Right: sync text and the push cloud (jiggles), then fire, trash, hammer (wiggles), terminal toggle, text-mode toggle, (shield), assistant toggle.
   - Hover is `#ffffff12`; inactive togglers and disabled buttons are `#00000061`.
8. **Side-bar panels**
   - Local Changes: 5 actions and three resizable sub-panels with count pills; diff rows with N/M/R/D letters in green, yellow, yellow and red.
   - Review: a title input plus a + square, or a review link plus a merge square and "created … ago"; a CHANGES list.
   - Project: the 30px vertical tab rail with rotated labels and a yellow left border when active.
     OVERVIEW has a form and an "Update" pill; RELEASE has release notes plus MAJOR (pink), MINOR and PATCH; VERSIONS and WORKSPACES lists.
   - Project configuration: a tab strip with a yellow underline; dependency rows of two selects, "Go to... ▾" and a pink ✕.
9. **Dialogs**
   - Top-aligned 34px from the top over a 50% black backdrop.
   - `modal--dark` boxes: `#1e1e1e` with a 1px `#08629e` border, 60rem search-modal size, 18px / 700 titles.
   - Accent 36px headers when titled; footers 50px tall with a top border.
   - Accent primary and `#353535` secondary buttons; pink caution; orange conflict.
   - 28px dark inputs and selects (border `#595959`, focusing to `#007acc`; menu `#2d2d2d`; 35px virtual rows; max 6 visible).
   - Form labels, prompts and the boolean square; the inline red error box.
   - Blocking alert (centred, not dismissible) and action alert (pink variant).
10. **Toasts**
    - One at a time, bottom-right at 30px / 10px, `#252525`, radius 3px, 12px text.
    - A severity icon (info blue, success green, warning yellow, error and bug red) and a single-line message that copies on click.
    - Expand and dismiss buttons.
    - Auto-hide after 6s **except errors**, which stay until dismissed.
11. **Global**
    - 1rem = 10px; Roboto 14px with line-height 1; Roboto Mono for code.
    - 8px translucent scrollbars; outlines off; no transitions; the native browser context menu disabled.
    - Dark default plus a `default-light` toggle (different from Query's `legacy-light`).
    - Add these tokens to our CSS:
      - `--color-text-inverted #1e1e1e`
      - `--color-status-info #007acc`
      - `--color-state-hover-on-accent #ffffff12`
      - `--color-state-disabled-on-accent #00000061`
      - `--color-conflict #ca663a`
      - `--color-category-experimental #7695ff`
      - `--color-category-generation #d14664`
      - the element colours in §3.3

### Icons needed: Icon.ts name → react-icons component

`LA/src/icon/Icon.ts:<line>`; react-icons 5.5.0. Rows marked **(have)** are already in `query/src/ui/icons.ts`.

| Icon.ts export | react-icons | Icon.ts line | Used in |
|---|---|---|---|
| MenuIcon | IoMenuOutline (io5) | 329 | activity bar menu **(have)** |
| FileTrayIcon | IoFileTrayFullOutline (io5) | 328 | activity: Explorer |
| FlaskIcon | IoFlaskSharp (io5) | 331 | activity: Test Runner |
| CodeBranchIcon | FaCodeBranch | 534 | activity: Local Changes; status bar |
| CloudDownloadIcon | IoCloudDownloadOutline (io5) | 335 | activity: Update; Local Changes pull |
| CloudUploadIcon | IoCloudUploadOutline (io5) | 336 | status bar push; Local Changes push |
| GitPullRequestIcon | GoGitPullRequest | 689 | activity: Review |
| GitMergeIcon | GoGitMerge | 690 | activity: Conflict Resolution |
| RepoIcon | TbBook | 67 | activity: Project |
| WrenchIcon | FaWrench | 642 | activity: Workflow Manager |
| DevIcon | FaDev | 598 | activity: Dev Mode |
| RobotIcon | FaRobot | 605 | activity: Register Service; Service element icon |
| WorkflowIcon | FcWorkflow (multi-colour) | 771 | activity: E2E workflows |
| ReadMeIcon | FaReadme | 603 | activity: Showcases |
| SunIcon | IoSunnyOutline (io5) | 338 | theme toggle **(have)** |
| MoonIcon | IoMoon (io5) | 340 | theme toggle **(have)** |
| CogIcon | FaCog | 535 | activity: Settings **(have)** |
| EmptyClockIcon | FaRegClock | 546 | activity counter "waiting" |
| SparkleIcon | *custom SVG* (Primer sparkle-fill-16), `LA/src/icon/SVGIcon.tsx:21-31` | n/a | beta badges |
| FileImportIcon | FaFileImport | 565 | explorer: Model Importer |
| SettingsEthernetIcon | MdSettingsEthernet | 164 | explorer: config action and row |
| PlusIcon | FaPlus | 599 | explorer +, review +, hotkey plus, project config add **(have)** |
| CompressIcon | FaCompress | 536 | explorer Collapse All **(have)** |
| SearchIcon | FaSearch | 608 | explorer Open Element; setup project cell **(have)** |
| LockIcon | FaLock | 583 | READ-ONLY badge |
| ChevronRightIcon | GoChevronRight | 686 | tree caret **(have)** |
| ChevronDownIcon | GoChevronDown | 683 | tree caret; tab menu; panel maximise; toast collapse **(have)** |
| ChevronUpIcon | GoChevronUp | 684 | panel maximise; toast expand |
| FolderIcon | FaFolder | 569 | package closed; recent tile |
| FolderOpenIcon | FaFolderOpen | 570 | package open |
| ExclamationTriangleIcon | FaExclamationTriangle | 554 | explorer failure; warning toast **(have)** |
| PackageIcon | FiPackage | 739 | PURE_PackageIcon (type menu) |
| FunctionIcon | TbMathFunction | 66 | PURE_FunctionIcon |
| MapIcon | FaMap | 587 | PURE_MappingIcon |
| BusinessTimeIcon | FaBusinessTime | 517 | PURE_RuntimeIcon |
| LinkIcon | MdLink | 165 | PURE_ConnectionIcon |
| DatabaseIcon | FaDatabase | 541 | PURE_DatabaseIcon |
| LayerGroupIcon | FaLayerGroup | 579 | PURE_FlatDataStoreIcon |
| FileCodeIcon | FaFileCode | 563 | PURE_FileGenerationIcon |
| TabulatedDataFileIcon | BsFillFileEarmarkSpreadsheetFill | 294 | PURE_DataIcon |
| Snowflake_BrandIcon | TbBrandSnowflake | 72 | Snowflake app / UDF |
| AccessPointIcon | LuRadioTower | 784 | Data product |
| CpuIcon | TbCpu | 76 | Compute |
| DatabaseImportIcon | TbDatabaseImport | 74 | Ingest / Availability |
| SinglestoreIcon | SiSinglestore | 701 | MemSQL function |
| LaunchIcon | MdRocketLaunch | 169 | other function activators |
| QuestionSquareIcon | BsQuestionSquare | 293 | unknown element |
| ServerIcon | FaServer | 609 | schema (PURE_DatabaseSchemaIcon) |
| TableIcon | VscTable | 244 | table (PURE_DatabaseTableIcon) |
| ShapeLineIcon | RiShapeLine | 762 | model store (PURE_ModelStoreIcon) |
| SquareIcon | FaSquare | 619 | **Data space** icon; boolean "off" square **(have, but our icons.ts labels it `EmptySquareIcon`; upstream EmptySquareIcon is FaRegSquare, line 548)** |
| ShapesIcon | FaShapes | 610 | Diagram |
| FileIcon | FaFile | 564 | Text element |
| MeteorIcon | FaMeteor | 590 | Persistence |
| PuzzlePieceIcon | FaPuzzlePiece | 600 | Persistence context |
| SwaggerIcon | SiSwagger | 699 | Service store |
| TimesIcon | FaTimes | 629 | tab close; select clear; dismiss; review close; list remove **(have)** |
| PushPinIcon | MdOutlinePushPin | 180 | pinned tab |
| GenericTextFileIcon | BsTextLeft | 290 | non-element tab icon |
| ArrowsAltHIcon | FaArrowsAltH | 507 | diff tab |
| CheckIcon | FaCheck | 528 | Advanced menu checks |
| CaretDownIcon | FaCaretDown | 521 | select indicator; Advanced; Go to... **(have)** |
| CircleNotchIcon | FaCircleNotch | 531 | select loading spinner |
| XIcon | GoX | 693 | panel group close |
| ErrorIcon | VscError | 226 | problems row; status bar |
| WarningIcon | VscWarning | 227 | problems row; status bar |
| HammerIcon | FaHammer | 573 | status: Compile **(have)** |
| FireIcon | FaFire | 568 | status: Generate |
| TrashIcon | FaTrash | 632 | status: Clear generation **(have)** |
| TerminalIcon | FaTerminal | 625 | status: Toggle panel |
| HackerIcon | FaUserSecret | 572 | status: Toggle text mode |
| ShieldIcon | FaShieldAlt | 612 | status: Re-auth |
| AssistantIcon | MdAssistant | 163 | status: assistant |
| SyncIcon | GoSync | 691 | status: accept conflict resolution |
| DownloadIcon | FaDownload | 544 | Local Changes download |
| UploadIcon | FaUpload | 635 | Local Changes upload |
| RefreshIcon | MdRefresh | 155 | Local Changes / Review refresh |
| InfoCircleIcon | FaInfoCircle | 576 | sub-panel info; info toast **(have)** |
| TruncatedGitMergeIcon | FiGitMerge | 748 | Review commit |
| ExternalLinkSquareIcon | FaExternalLinkSquareAlt | 558 | Review link |
| ShareIcon | FaShare | 611 | Project: Share |
| ExternalLinkIcon | FaExternalLinkAlt | 557 | Project: open VCS |
| UserIcon | FaUser | 637 | user workspace |
| UsersIcon | FaUserFriends | 638 | group workspace |
| PencilIcon | MdModeEdit | 146 | list-item edit |
| GitBranchIcon | GoGitBranch | 692 | setup workspace cell |
| LongArrowRightIcon | FaLongArrowAltRight | 586 | setup Go button |
| OpenIcon | IoOpenOutline (io5) | 326 | doc-card actions |
| HistoryIcon | FaHistory | 575 | Recent workspaces header |
| ArrowCircleRightIcon | FaArrowAltCircleRight | 502 | project option "view" / "configure" |
| ExclamationCircleIcon | FaExclamationCircle | 553 | project option "configure" |
| QuestionCircleIcon | FaQuestionCircle | 601 | DocumentationLink "?" |
| MoreHorizontalIcon | MdMoreHoriz | 149 | Ctrl+P type filter |
| CheckSquareIcon | FaCheckSquare | 529 | boolean on **(have)** |
| VerticalAlignBottomIcon | MdVerticalAlignBottom | 154 | drop-zone placeholder **(have)** |
| AddIcon | MdAdd | 159 | blank placeholder "add" |
| EditIcon | MdEdit | 160 | blank placeholder "modify" |
| CheckCircleIcon | FaCheckCircle | 527 | success toast |
| TimesCircleIcon | FaTimesCircle | 628 | error toast |
| BugIcon | FaBug | 515 | illegal-state toast |
| CopyIcon | FaRegCopy | 537 | toast copy |
| VersionsIcon / ExpandAllIcon | VscVersions / FaExpandArrowsAlt | 236 / 555 | dependency explorer (optional) |

The letter type icons (C, E, A, P, M, p, u, e, G) are not react-icons. They are text in Raleway 900, coloured by `.color--*` (§3.3).
