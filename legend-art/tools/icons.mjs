// Writes legend-art/src/icons.ts: the icons upstream Legend shows (legend-art's Icon.ts, by react-icons name), as
// SVG strings -- the same paths, no React, no runtime dependency -- for every app here (Query, Studio).
//
//   curl -sL https://registry.npmjs.org/react-icons/-/react-icons-5.5.0.tgz | tar -xz -C <dir>
//   node legend-art/tools/icons.mjs <dir>/package > legend-art/src/icons.ts
//
// 5.5.0 is the version legend-studio pins (packages/legend-art/package.json). The names upstream gives each icon,
// and where each is used: studio/docs/UPSTREAM_STUDIO_LOOK.md ("Icons needed"), query/docs/UPSTREAM_QUERY_CENSUS.md.

import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const [,, root] = process.argv;
if (!root) throw new Error('usage: node legend-art/tools/icons.mjs <react-icons package dir>');

/** Our name -> [react-icons set, react-icons name, upstream's name in legend-art/src/icon/Icon.ts]. */
const ICONS = {
  // ---- Query's (their names unchanged) ----
  play: ['fa', 'FaPlay', 'PlayIcon'],
  plus: ['fa', 'FaPlus', 'PlusIcon'],
  plusCircle: ['fi', 'FiPlusCircle', 'PlusCircleIcon'],
  times: ['fa', 'FaTimes', 'TimesIcon'],
  trash: ['fa', 'FaTrash', 'TrashIcon'],
  caretDown: ['fa', 'FaCaretDown', 'CaretDownIcon'],
  chevronRight: ['go', 'GoChevronRight', 'ChevronRightIcon'],
  chevronDown: ['go', 'GoChevronDown', 'ChevronDownIcon'],
  compress: ['fa', 'FaCompress', 'CompressIcon'],
  expand: ['fa', 'FaExpand', 'ExpandIcon'],
  info: ['fa', 'FaInfoCircle', 'InfoCircleIcon'],
  eye: ['fa', 'FaEye', 'EyeIcon'],
  search: ['fa', 'FaSearch', 'SearchIcon'],
  cog: ['fa', 'FaCog', 'CogIcon'],
  calculator: ['fa', 'FaCalculator', 'CalculatorIcon'],
  undo: ['fa', 'FaUndo', 'UndoIcon'],
  redo: ['fa', 'FaRedo', 'RedoIcon'],
  hammer: ['fa', 'FaHammer', 'HammerIcon'],
  warning: ['fa', 'FaExclamationTriangle', 'ExclamationTriangleIcon'],
  checked: ['fa', 'FaCheckSquare', 'CheckSquareIcon'],
  // upstream's SquareIcon (Icon.ts:619); its EmptySquareIcon is FaRegSquare (`emptySquare` below)
  unchecked: ['fa', 'FaSquare', 'SquareIcon'],
  typeBoolean: ['fa', 'FaToggleOn', 'ToggleIcon'],
  typeNumber: ['fa', 'FaHashtag', 'HashtagIcon'],
  typeDate: ['fa', 'FaClock', 'ClockIcon'],
  typeString: ['md', 'MdTextFields', 'StringTypeIcon'],
  save: ['md', 'MdSave', 'SaveCurrIcon'],
  saveAs: ['md', 'MdSaveAs', 'SaveAsIcon'],
  load: ['md', 'MdManageSearch', 'ManageSearchIcon'],
  more: ['md', 'MdMoreVert', 'MoreVerticalIcon'],
  sigma: ['md', 'MdFunctions', 'SigmaIcon'],
  dropHere: ['md', 'MdVerticalAlignBottom', 'VerticalAlignBottomIcon'],
  link: ['fi', 'FiLink', 'AnchorLinkIcon'],
  menu: ['io5', 'IoMenuOutline', 'MenuIcon'],
  moon: ['io5', 'IoMoon', 'MoonIcon'],
  sun: ['io5', 'IoSunnyOutline', 'SunIcon'],
  // ---- Studio's: the activity bar and status bar ----
  fileTray: ['io5', 'IoFileTrayFullOutline', 'FileTrayIcon'],
  flask: ['io5', 'IoFlaskSharp', 'FlaskIcon'],
  codeBranch: ['fa', 'FaCodeBranch', 'CodeBranchIcon'],
  cloudDownload: ['io5', 'IoCloudDownloadOutline', 'CloudDownloadIcon'],
  cloudUpload: ['io5', 'IoCloudUploadOutline', 'CloudUploadIcon'],
  gitPullRequest: ['go', 'GoGitPullRequest', 'GitPullRequestIcon'],
  gitMerge: ['go', 'GoGitMerge', 'GitMergeIcon'],
  repo: ['tb', 'TbBook', 'RepoIcon'],
  readMe: ['fa', 'FaReadme', 'ReadMeIcon'],
  emptyClock: ['fa', 'FaRegClock', 'EmptyClockIcon'],
  fire: ['fa', 'FaFire', 'FireIcon'],
  terminal: ['fa', 'FaTerminal', 'TerminalIcon'],
  hacker: ['fa', 'FaUserSecret', 'HackerIcon'],
  shield: ['fa', 'FaShieldAlt', 'ShieldIcon'],
  assistant: ['md', 'MdAssistant', 'AssistantIcon'],
  sync: ['go', 'GoSync', 'SyncIcon'],
  error: ['vsc', 'VscError', 'ErrorIcon'],
  vscWarning: ['vsc', 'VscWarning', 'WarningIcon'],
  x: ['go', 'GoX', 'XIcon'],
  chevronUp: ['go', 'GoChevronUp', 'ChevronUpIcon'],
  // ---- the explorer and element types ----
  fileImport: ['fa', 'FaFileImport', 'FileImportIcon'],
  settingsEthernet: ['md', 'MdSettingsEthernet', 'SettingsEthernetIcon'],
  lock: ['fa', 'FaLock', 'LockIcon'],
  folder: ['fa', 'FaFolder', 'FolderIcon'],
  folderOpen: ['fa', 'FaFolderOpen', 'FolderOpenIcon'],
  package: ['fi', 'FiPackage', 'PackageIcon'],
  function: ['tb', 'TbMathFunction', 'FunctionIcon'],
  map: ['fa', 'FaMap', 'MapIcon'],
  businessTime: ['fa', 'FaBusinessTime', 'BusinessTimeIcon'],
  connection: ['md', 'MdLink', 'LinkIcon'],
  database: ['fa', 'FaDatabase', 'DatabaseIcon'],
  layerGroup: ['fa', 'FaLayerGroup', 'LayerGroupIcon'],
  fileCode: ['fa', 'FaFileCode', 'FileCodeIcon'],
  dataFile: ['bs', 'BsFillFileEarmarkSpreadsheetFill', 'TabulatedDataFileIcon'],
  questionSquare: ['bs', 'BsQuestionSquare', 'QuestionSquareIcon'],
  server: ['fa', 'FaServer', 'ServerIcon'],
  table: ['vsc', 'VscTable', 'TableIcon'],
  shapeLine: ['ri', 'RiShapeLine', 'ShapeLineIcon'],
  shapes: ['fa', 'FaShapes', 'ShapesIcon'],
  file: ['fa', 'FaFile', 'FileIcon'],
  robot: ['fa', 'FaRobot', 'RobotIcon'],
  swagger: ['si', 'SiSwagger', 'SwaggerIcon'],
  meteor: ['fa', 'FaMeteor', 'MeteorIcon'],
  puzzlePiece: ['fa', 'FaPuzzlePiece', 'PuzzlePieceIcon'],
  // ---- tabs, menus, panels, dialogs, toasts ----
  pushPin: ['md', 'MdOutlinePushPin', 'PushPinIcon'],
  textFile: ['bs', 'BsTextLeft', 'GenericTextFileIcon'],
  arrowsAltH: ['fa', 'FaArrowsAltH', 'ArrowsAltHIcon'],
  check: ['fa', 'FaCheck', 'CheckIcon'],
  circleNotch: ['fa', 'FaCircleNotch', 'CircleNotchIcon'],
  download: ['fa', 'FaDownload', 'DownloadIcon'],
  upload: ['fa', 'FaUpload', 'UploadIcon'],
  refresh: ['md', 'MdRefresh', 'RefreshIcon'],
  gitMergeShort: ['fi', 'FiGitMerge', 'TruncatedGitMergeIcon'],
  externalLinkSquare: ['fa', 'FaExternalLinkSquareAlt', 'ExternalLinkSquareIcon'],
  share: ['fa', 'FaShare', 'ShareIcon'],
  externalLink: ['fa', 'FaExternalLinkAlt', 'ExternalLinkIcon'],
  user: ['fa', 'FaUser', 'UserIcon'],
  users: ['fa', 'FaUserFriends', 'UsersIcon'],
  pencil: ['md', 'MdModeEdit', 'PencilIcon'],
  gitBranch: ['go', 'GoGitBranch', 'GitBranchIcon'],
  longArrowRight: ['fa', 'FaLongArrowAltRight', 'LongArrowRightIcon'],
  open: ['io5', 'IoOpenOutline', 'OpenIcon'],
  history: ['fa', 'FaHistory', 'HistoryIcon'],
  arrowCircleRight: ['fa', 'FaArrowAltCircleRight', 'ArrowCircleRightIcon'],
  exclamationCircle: ['fa', 'FaExclamationCircle', 'ExclamationCircleIcon'],
  questionCircle: ['fa', 'FaQuestionCircle', 'QuestionCircleIcon'],
  moreHoriz: ['md', 'MdMoreHoriz', 'MoreHorizontalIcon'],
  add: ['md', 'MdAdd', 'AddIcon'],
  edit: ['md', 'MdEdit', 'EditIcon'],
  checkCircle: ['fa', 'FaCheckCircle', 'CheckCircleIcon'],
  timesCircle: ['fa', 'FaTimesCircle', 'TimesCircleIcon'],
  bug: ['fa', 'FaBug', 'BugIcon'],
  copy: ['fa', 'FaRegCopy', 'CopyIcon'],
  emptySquare: ['fa', 'FaRegSquare', 'EmptySquareIcon'],
};

// SVG keeps a few attributes camelCase (viewBox); the presentation attributes are hyphenated
const kebab = (s) => (s === 'viewBox' ? s : s.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`));
const attrs = (a) => Object.entries(a ?? {}).map(([k, v]) => ` ${kebab(k)}="${String(v).replace(/"/g, '&quot;')}"`).join('');
const render = (node) => `<${node.tag}${attrs(node.attr)}>${(node.child ?? []).map(render).join('')}</${node.tag}>`;

const sources = new Map();
const tree = (set, name) => {
  if (!sources.has(set)) sources.set(set, readFileSync(join(root, set, 'index.mjs'), 'utf8'));
  const m = new RegExp(`export function ${name} \\(props\\) \\{\\s*return GenIcon\\((\\{.*?\\})\\)\\(props\\);`, 's').exec(sources.get(set));
  if (!m) throw new Error(`react-icons/${set} has no ${name}`);
  return JSON.parse(m[1]);
};

const lines = [
  '// GENERATED by legend-art/tools/icons.mjs from react-icons 5.5.0 (MIT) -- do not edit by hand. VENDORED: no app',
  '// depends on react-icons; this file is the data. Source: https://registry.npmjs.org/react-icons/-/react-icons-5.5.0.tgz',
  '// (npm integrity sha512-MEFcXdkP3dLo8uumGI5xN3lDFNsRtrjbOEKDLD7yv76v4wpnEq2Lt2qeHaQOr34I/wPN3s3+N08WkQ+CW37Xiw==).',
  '// The icons upstream Legend shows (legend-art src/icon/Icon.ts), the same SVG paths: Font Awesome Free (fa;',
  '// icons CC BY 4.0), Material Design icons (md; Apache-2.0), GitHub Octicons (go; MIT), Feather (fi; MIT),',
  '// Ionicons 5 (io5; MIT), Tabler (tb; MIT), Bootstrap (bs; MIT), VS Code Codicons (vsc; CC BY 4.0),',
  '// Remix (ri; Apache-2.0), Simple Icons (si; CC0).',
  '',
  '/** Each icon as SVG markup: 1em square, painted in the current text colour (react-icons\' GenIcon defaults). */',
  'export const ICONS = {',
];
for (const [key, [set, name, upstream]] of Object.entries(ICONS)) {
  const svg = tree(set, name);
  // GenIcon's defaults, under the icon's own attributes (an outline icon sets fill="none")
  svg.attr = { stroke: 'currentColor', fill: 'currentColor', strokeWidth: '0', ...svg.attr, height: '1em', width: '1em' };
  lines.push(`  /** ${upstream} = ${name} */`, `  ${key}: '${render(svg).replace(/'/g, "\\'")}',`);
}
lines.push('} as const;', '', 'export type IconName = keyof typeof ICONS;', '');
process.stdout.write(lines.join('\n'));
