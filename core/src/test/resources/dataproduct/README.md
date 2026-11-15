# Legend's data product JSON, as test input

Copied from legend-studio (Apache License 2.0, Copyright Goldman Sachs) at `9775e72` (2026-11-15), the data product
elements' `content` only, from `packages/legend-graph/src/graph-manager/__tests__/roundtripTestData/
TEST_DATA__DataProductRoundtrip.ts`:

- `studio-data-products.json`: `TEST_DATA__DATAPRODUCT_GROUPS`'s product (two default groups, one with a stereotype),
  and `TEST_DATA__DATAPRODUCT__MODEL_ACCESS_GROUPS`'s product with its `featuredElements` and `secureViews` left out
  (lite does not carry them yet: register S408). Written by hand in Studio's test: its empty lists left out.

Read by `com.legend.parser.DataProductTest`: Legend's closed grammar has no public parser, so its JSON is the spec
lite's reader and writer are held to (docs/DEPLOYMENT_DESIGN_2026_11_14.md §7).
