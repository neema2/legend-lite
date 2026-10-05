# legend-art, lite

The look Query, Studio and (later) DataCube share, modelled on upstream's `@finos/legend-art`: colour tokens, icons,
type. No app takes an icon or font package from npm, and this package has no npm dependencies: what those packages
supplied comes from archives Bazel pins by their registry integrity (MODULE.bazel), and nothing binary is checked in.

## Colours: `src/tokens.css`, `src/icons.css`

Upstream's semantic tokens resolved -- `default-dark` on `:root`, the light themes by `data-theme` (`default-light`,
Studio's; `legacy-light`, Query's) -- and upstream's element-type colours. Each site serves them from
`./vendor/legend-art/` (`//legend-art:styles`) and links them before its own stylesheet.

## Icons: `src/icons.ts`, `src/icon.ts`, `src/type-icon.ts`

Upstream Legend's icons, under upstream's names (`legend-art/src/icon/Icon.ts`), as SVG strings with the same paths
react-icons draws, generated from MODULE.bazel's `@react_icons` (react-icons 5.5.0, the version legend-studio pins)
by `tools/icons.mjs`:

```
bazel run //legend-art:update_generated     # write src/icons.ts
bazel test //:generated                     # fails when the committed copy is stale
```

To add an icon: add its row to `ICONS` in `tools/icons.mjs` (our name, the react-icons set and name, upstream's name),
add its set to `_ICON_SETS` in `BUILD.bazel` if it is a new one, run the above, and read the diff. `icon()` makes one a
DOM element; `typeIcon()` is upstream's TypeIcon (the element kinds' letters and icons, in their colours).

## Fonts: `//legend-art:fonts`

Upstream's type (`legend-art` `_fonts.scss`): Roboto (300, 400, 500, 700, 900; latin, and latin-ext at 400, 500 and
700 for DataCube), Roboto Mono (400, 700) and Raleway 900, as `.woff2`, from MODULE.bazel's `@fontsource_roboto`,
`@fontsource_roboto_mono` and `@fontsource_raleway` (@fontsource 5.3.0), with each family's SIL Open Font License 1.1
beside them as `LICENSE-<family>.txt`. Each site serves them from `./vendor/fonts/`, never a font CDN.
