// THE SHAPES A MAP DRAWS, and how a row finds its region (docs/DATACUBE_CHARTS_DESIGN_2026_10_09.md §4, items 1-3):
// Natural Earth's countries, prepared once at build time (tools/maps/prepare.py, committed as world-110m.ts), and the
// rule a column's value is matched by -- an ISO 3166 code exactly (alpha-3 "USA", alpha-2 "US"), else one of Natural
// Earth's own names for the country ("United States", "United States of America"), case aside. A value matching no
// region is said, never placed by a guess. Library-free, as chart-kinds.ts is; the shapes are a chunk of their own.

/** One country as prepared: its codes (null where Natural Earth gives none), its names, and its outline. */
export interface WorldFeature {
  readonly type: 'Feature';
  readonly properties: {
    readonly iso3: string | null;
    readonly iso2: string | null;
    /** Natural Earth's short name: the region's name on the map (ECharts' `name` property), unique among them. */
    readonly name: string;
    readonly long: string;
    readonly admin: string;
  };
  readonly geometry: { readonly type: 'Polygon' | 'MultiPolygon'; readonly coordinates: unknown };
}

export interface WorldShapes {
  readonly type: 'FeatureCollection';
  readonly features: readonly WorldFeature[];
}

/** The name ECharts registers the world's countries under. */
export const WORLD_MAP = 'world';

/**
 * How a column's value finds its country: by its code (alpha-3, alpha-2) exactly, else by a name, case and the spaces
 * around it aside. Returns the region's name on the map, or undefined: a value matching none is said by the drawing.
 */
export function regionFinder(shapes: WorldShapes): (value: string) => string | undefined {
  const byCode = new Map<string, string>();
  const byName = new Map<string, string>();
  for (const f of shapes.features) {
    const p = f.properties;
    for (const c of [p.iso3, p.iso2]) if (c !== null) byCode.set(c, p.name);
    for (const n of [p.name, p.long, p.admin]) byName.set(n.trim().toLowerCase(), p.name);
  }
  return (value) => byCode.get(value) ?? byName.get(value.trim().toLowerCase());
}

// ---- the projection: Equal Earth (Šavrič, Patterson and Jenny, 2018), areas kept honest (§4 item 5) ----

const A1 = 1.340264;
const A2 = -0.081106;
const A3 = 0.000893;
const A4 = 0.003796;
const M = Math.sqrt(3) / 2;
const RAD = Math.PI / 180;

/** A longitude and latitude (degrees) as a place on the plane, y downward as a screen's. */
export function equalEarth([lon, lat]: readonly number[]): number[] {
  const t = Math.asin(M * Math.sin(lat! * RAD));
  const t2 = t * t;
  const t6 = t2 * t2 * t2;
  const x = (lon! * RAD * Math.cos(t)) / (M * (A1 + 3 * A2 * t2 + t6 * (7 * A3 + 9 * A4 * t2)));
  const y = t * (A1 + A2 * t2 + t6 * (A3 + A4 * t2));
  return [x, -y];
}

/** A place on the plane back to its longitude and latitude (degrees): Newton's method for the parametric latitude. */
export function equalEarthInverse([x, yDown]: readonly number[]): number[] {
  const y = -yDown!;
  let t = y;
  for (let i = 0; i < 20; i++) {
    const t2 = t * t;
    const t6 = t2 * t2 * t2;
    const step = (t * (A1 + A2 * t2 + t6 * (A3 + A4 * t2)) - y) / (A1 + 3 * A2 * t2 + t6 * (7 * A3 + 9 * A4 * t2));
    t -= step;
    if (Math.abs(step) < 1e-12) break;
  }
  const t2 = t * t;
  const t6 = t2 * t2 * t2;
  const lon = (M * x! * (A1 + 3 * A2 * t2 + t6 * (7 * A3 + 9 * A4 * t2))) / Math.cos(t);
  const lat = Math.asin(Math.sin(t) / M);
  return [lon / RAD, lat / RAD];
}
