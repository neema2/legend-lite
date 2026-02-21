#!/usr/bin/env python3
"""
Convert Goldman Sachs gs-quant dataclass models to a Pure semantic model file.

Reads from: /Users/neema/legend/gs-quant/gs_quant/target/
Outputs to: gsquant-model.pure

GS-Quant uses Python dataclasses with field(metadata=field_metadata).
Classes inherit from Base, Instrument, Scenario, Priceable, etc.
"""

import os
import re
from collections import defaultdict

GSQUANT_DIR = "/Users/neema/legend/gs-quant/gs_quant/target"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "gsquant-model.pure")

# ─── Domain classification by source file ───
FILE_TO_DOMAIN = {
    'instrument.py': 'instrument',
    'common.py': 'common',
    'assets.py': 'asset',
    'assets_screener.py': 'asset',
    'backtests.py': 'backtest',
    'base_screener.py': 'screener',
    'charts.py': 'visualization',
    'content.py': 'content',
    'coordinates.py': 'coordinates',
    'countries.py': 'country',
    'data.py': 'dataset',
    'data_screen.py': 'dataset',
    'groups.py': 'group',
    'hedge.py': 'hedge',
    'indices.py': 'index',
    'measures.py': 'risk',
    'monitor.py': 'monitor',
    'portfolios.py': 'portfolio',
    'positions_v2_pricing.py': 'portfolio',
    'price.py': 'pricing',
    'reports.py': 'report',
    'risk.py': 'risk',
    'risk_models.py': 'riskmodel',
    'scenarios.py': 'scenario',
    'screens.py': 'screener',
    'secmaster.py': 'secmaster',
    'trades.py': 'trade',
    'workflow_quote.py': 'workflow',
    'workspaces_markets.py': 'workspace',
}

DOMAIN_DISPLAY = {
    'instrument': 'Financial Instruments',
    'common': 'Common Data Types',
    'asset': 'Assets & Securities',
    'backtest': 'Backtesting',
    'screener': 'Screening & Filtering',
    'visualization': 'Charts & Visualization',
    'content': 'Content & Research',
    'coordinates': 'Market Coordinates',
    'country': 'Country & Region Data',
    'dataset': 'Datasets & Data Queries',
    'group': 'Groups & Entitlements',
    'hedge': 'Hedging & Optimization',
    'index': 'Indices & Baskets',
    'monitor': 'Monitors & Alerts',
    'portfolio': 'Portfolios & Positions',
    'pricing': 'Pricing',
    'report': 'Reports & Analytics',
    'risk': 'Risk Measures & Scenarios',
    'riskmodel': 'Risk Models',
    'scenario': 'Market Scenarios',
    'secmaster': 'Security Master',
    'trade': 'Trade Execution',
    'workflow': 'Workflow & Quotes',
    'workspace': 'Workspaces & Markets',
}

WHEN_TO_USE = {
    'instrument': 'Use for derivative instruments like swaps, options, forwards, bonds, and structured products',
    'common': 'Use for shared data types like positions, markets, links, and entitlements',
    'asset': 'Use for securities, stocks, ETFs, funds, and asset classification',
    'backtest': 'Use for backtesting strategies, triggers, and historical performance analysis',
    'screener': 'Use for screening and filtering assets by criteria',
    'dataset': 'Use for querying market data, time series, and dataset metadata',
    'hedge': 'Use for hedging strategies, portfolio optimization, and risk reduction',
    'index': 'Use for market indices, custom baskets, and index composition',
    'portfolio': 'Use for portfolio construction, positions, holdings, and rebalancing',
    'pricing': 'Use for instrument pricing, valuation, and price snapshots',
    'report': 'Use for performance reports, factor analysis, and portfolio analytics',
    'risk': 'Use for risk measures like delta, gamma, vega, VaR, and scenario analysis',
    'riskmodel': 'Use for factor risk models, covariance matrices, and risk decomposition',
    'secmaster': 'Use for security master data, identifiers, and reference data lookups',
    'trade': 'Use for trade execution, order management, and trade lifecycle',
}

# ─── Collect all enums for type resolution ───
ALL_ENUMS = set()

def collect_enums():
    """Pre-scan all files to find enum class names."""
    for fname in os.listdir(GSQUANT_DIR):
        if not fname.endswith('.py'):
            continue
        with open(os.path.join(GSQUANT_DIR, fname)) as f:
            content = f.read()
        for m in re.finditer(r'class\s+(\w+)\((?:EnumBase,\s*)?Enum\)', content):
            ALL_ENUMS.add(m.group(1))


# ─── Known dataclass names for reference resolution ───
ALL_DATACLASSES = set()

def collect_dataclasses():
    for fname in os.listdir(GSQUANT_DIR):
        if not fname.endswith('.py') or fname == '__init__.py':
            continue
        with open(os.path.join(GSQUANT_DIR, fname)) as f:
            content = f.read()
        for m in re.finditer(r'class\s+(\w+)\((\w+)\)', content):
            if m.group(2) not in ('Enum', 'EnumBase'):
                ALL_DATACLASSES.add(m.group(1))


def python_type_to_pure(type_str, field_name):
    """Convert Python type annotation to (pure_type, multiplicity, is_reference)."""
    type_str = type_str.strip()

    # Handle Optional[X]
    opt_match = re.match(r'Optional\[(.+)\]$', type_str)
    is_optional = bool(opt_match)
    if opt_match:
        type_str = opt_match.group(1).strip()

    # Handle Tuple[X, ...] → to-many
    tuple_match = re.match(r'Tuple\[(.+),\s*\.\.\.\]$', type_str)
    if tuple_match:
        inner = tuple_match.group(1).strip()
        # Handle Union inside tuple
        union_match = re.match(r'Union\[(.+)\]$', inner)
        if union_match:
            inner = pick_primary(union_match.group(1))
        pure_type, is_ref = scalar_to_pure(inner)
        return pure_type, "[*]", is_ref

    # Handle Union[X, Y]
    union_match = re.match(r'Union\[(.+)\]$', type_str)
    if union_match:
        type_str = pick_primary(union_match.group(1))

    # Handle Dict → String (we can't model dicts in Pure)
    if type_str.startswith('Dict') or type_str.startswith('dict'):
        mult = "[0..1]" if is_optional else "[1]"
        return "String", mult, False

    pure_type, is_ref = scalar_to_pure(type_str)
    mult = "[0..1]" if is_optional else "[1]"
    return pure_type, mult, is_ref


def pick_primary(union_parts_str):
    """Pick primary type from 'float, str' or 'DictBase, str'."""
    parts = [p.strip() for p in union_parts_str.split(',')]
    for prefer in ['float', 'int', 'str', 'datetime.date', 'datetime.datetime', 'bool']:
        if prefer in parts:
            return prefer
    return parts[0]


def scalar_to_pure(py_type):
    """Map scalar Python type to (Pure type, is_reference)."""
    mapping = {
        'float': ('Float', False),
        'int': ('Integer', False),
        'str': ('String', False),
        'bool': ('Boolean', False),
        'datetime.date': ('StrictDate', False),
        'datetime.datetime': ('DateTime', False),
        'date': ('StrictDate', False),
        'datetime': ('DateTime', False),
        'Decimal': ('Float', False),
    }
    if py_type in mapping:
        return mapping[py_type]
    # Enum types → String (we'll use sampleValues)
    if py_type in ALL_ENUMS:
        return ('String', False)
    # Known dataclass references → skip (we model them as associations instead)
    if py_type in ALL_DATACLASSES:
        return ('String', False)  # Flatten references to String for now
    # Fallback
    return ('String', False)


def to_camel(snake):
    """Convert snake_case to camelCase."""
    parts = snake.rstrip('_').split('_')
    return parts[0] + ''.join(w.capitalize() for w in parts[1:])


def humanize(snake):
    """Convert snake_case to human-readable description."""
    return snake.rstrip('_').replace('_', ' ').title()


def parse_file(filepath):
    """Parse a gs-quant target file and extract all dataclass definitions."""
    with open(filepath) as f:
        content = f.read()

    models = []
    # Match class definitions
    class_iter = list(re.finditer(r'class\s+(\w+)\((\w+)\):\s*\n', content))

    for i, cm in enumerate(class_iter):
        class_name = cm.group(1)
        parent = cm.group(2)

        # Skip enums
        if parent in ('Enum', 'EnumBase'):
            continue

        # Get class body until next class or EOF
        start = cm.end()
        end = class_iter[i+1].start() if i+1 < len(class_iter) else len(content)
        body = content[start:end]

        # Extract docstring
        doc_match = re.search(r'"""(.+?)"""', body, re.DOTALL)
        docstring = ""
        if doc_match:
            docstring = doc_match.group(1).strip().split('\n')[0].strip()

        # Extract fields
        fields = []
        for fm in re.finditer(r'^\s+(\w+):\s+(.+?)\s*=\s*field\((.+?)\)\s*$', body, re.MULTILINE):
            field_name = fm.group(1)
            field_type = fm.group(2).strip()
            field_args = fm.group(3)

            # Skip init=False fields (type discriminators, computed)
            if 'init=False' in field_args:
                continue

            # Skip 'name' metadata fields (present on every class)
            if 'name_metadata' in field_args and field_name == 'name':
                # Keep name — it's useful
                pass

            fields.append({
                'name': field_name,
                'type': field_type,
            })

        if fields:
            models.append({
                'class_name': class_name,
                'parent': parent,
                'docstring': docstring,
                'fields': fields,
            })

    return models


def classify_instrument(class_name):
    """Sub-classify instrument classes into asset class domains."""
    prefixes = {
        'Eq': 'equity',
        'FX': 'fx',
        'IR': 'rates',
        'Commod': 'commodities',
        'CD': 'credit',
        'Bond': 'rates',
        'FRA': 'rates',
        'Forward': 'rates',
        'Inflation': 'rates',
        'Invoice': 'rates',
        'Metal': 'commodities',
        'Macro': 'macro',
    }
    for prefix, domain in prefixes.items():
        if class_name.startswith(prefix):
            return domain
    return 'instrument'


def generate():
    """Main: parse all gs-quant target files and generate Pure model."""
    collect_enums()
    collect_dataclasses()

    domain_models = defaultdict(list)

    for fname in sorted(os.listdir(GSQUANT_DIR)):
        if not fname.endswith('.py') or fname == '__init__.py':
            continue

        filepath = os.path.join(GSQUANT_DIR, fname)
        base_domain = FILE_TO_DOMAIN.get(fname, 'other')
        parsed = parse_file(filepath)

        for model in parsed:
            # Sub-classify instruments by asset class
            if base_domain == 'instrument':
                domain = classify_instrument(model['class_name'])
            else:
                domain = base_domain

            model['domain'] = domain
            domain_models[domain].append(model)

    # ─── Generate Pure ───
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// Goldman Sachs gs-quant — Auto-generated from gs_quant/target/")
    total_classes = sum(len(v) for v in domain_models.values())
    lines.append(f"// Source: {total_classes} dataclasses from gs-quant Python SDK")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")

    # NlqProfile
    lines.append("Profile nlq::NlqProfile")
    lines.append("{")
    lines.append("    stereotypes: [dimension, metric];")
    lines.append("    tags: [description, synonyms, sampleValues, unit, importance, businessDomain, exampleQuestions, whenToUse];")
    lines.append("}")
    lines.append("")

    total_props = 0
    class_count = 0
    seen_class_names = set()

    # Updated domain display for instrument sub-domains
    all_domain_display = dict(DOMAIN_DISPLAY)
    all_domain_display.update({
        'equity': 'Equity Derivatives',
        'fx': 'FX Derivatives',
        'rates': 'Interest Rate Products',
        'commodities': 'Commodity Derivatives',
        'credit': 'Credit Derivatives',
        'macro': 'Macro Products',
    })
    all_when_to_use = dict(WHEN_TO_USE)
    all_when_to_use.update({
        'equity': 'Use for equity options, variance swaps, forwards, convertible bonds, and equity derivatives',
        'fx': 'Use for FX options, forwards, accumulators, barriers, knockouts, and FX derivatives',
        'rates': 'Use for interest rate swaps, swaptions, caps, floors, bonds, and rate derivatives',
        'commodities': 'Use for commodity swaps, options, forwards, and commodity derivatives',
        'credit': 'Use for credit default swaps, CDS indices, and credit derivatives',
        'macro': 'Use for macro baskets and macro-level instruments',
    })

    for domain in sorted(domain_models.keys()):
        models = domain_models[domain]
        display = all_domain_display.get(domain, domain.title())
        wtu = all_when_to_use.get(domain, '')

        lines.append(f"// ─── {display} ({len(models)} models) ───")
        lines.append("")

        for model in models:
            # Deduplicate class names across files
            qualified = f"{domain}::{model['class_name']}"
            if qualified in seen_class_names:
                continue
            seen_class_names.add(qualified)
            class_count += 1

            # Determine stereotype
            numeric_fields = sum(1 for f in model['fields']
                                 if python_type_to_pure(f['type'], f['name'])[0] in ('Float', 'Integer'))
            stereotype = "metric" if numeric_fields > len(model['fields']) / 2 else "dimension"

            desc = model['docstring'] or humanize(model['class_name'])
            desc = desc.replace("'", "\\'")
            if len(desc) > 120:
                desc = desc[:117] + "..."

            lines.append(f"Class <<nlq::NlqProfile.{stereotype}>>")
            tag_line = f"  {{nlq::NlqProfile.description = '{desc}'"
            tag_line += f",\n   nlq::NlqProfile.businessDomain = '{display}'"
            if wtu:
                tag_line += f",\n   nlq::NlqProfile.whenToUse = '{wtu}'"
            tag_line += "}"
            lines.append(tag_line)
            lines.append(qualified)
            lines.append("{")

            seen_props = set()
            for field in model['fields']:
                prop_name = to_camel(field['name'])
                if prop_name in seen_props:
                    continue
                seen_props.add(prop_name)

                pure_type, mult, _ = python_type_to_pure(field['type'], field['name'])
                desc = humanize(field['name'])
                desc = desc.replace("'", "\\'")

                # Property tags
                ptags = f"nlq::NlqProfile.description = '{desc}'"

                # Add unit hints
                fname_lower = field['name'].lower()
                if any(x in fname_lower for x in ('price', 'strike', 'premium', 'notional', 'fee', 'spread')):
                    ptags += ", nlq::NlqProfile.unit = 'currency'"
                elif any(x in fname_lower for x in ('rate', 'yield', 'coupon', 'percent')):
                    ptags += ", nlq::NlqProfile.unit = 'percent'"
                elif 'quantity' in fname_lower or 'weight' in fname_lower:
                    ptags += ", nlq::NlqProfile.unit = 'units'"

                lines.append(f"    {{{ptags}}}")
                lines.append(f"    {prop_name}: {pure_type}{mult};")
                total_props += 1

            lines.append("}")
            lines.append("")

    # Write
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Classes:    {class_count}")
    print(f"  Properties: {total_props}")
    print(f"  Domains:    {len(domain_models)}")


if __name__ == "__main__":
    generate()
