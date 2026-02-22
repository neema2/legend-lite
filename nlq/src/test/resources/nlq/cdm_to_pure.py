#!/usr/bin/env python3
"""
Convert ISDA Common Domain Model (CDM) Rosetta types to a Pure semantic model file.

Reads from: /Users/neema/legend/cdm/rosetta-source/src/main/rosetta/*-type.rosetta
Outputs to: cdm-model.pure

CDM uses Rosetta DSL with:
  type Name extends Base: <"description">
      attrName Type (multiplicity) <"description">
"""

import os
import re
from collections import defaultdict

ROSETTA_DIR = "/Users/neema/legend/cdm/rosetta-source/src/main/rosetta"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "cdm-model.pure")

# ─── Domain classification by rosetta filename prefix ───
FILE_TO_DOMAIN = {
    'base-datetime': 'datetime',
    'base-math': 'math',
    'base-staticdata-asset-common': 'asset',
    'base-staticdata-asset-credit': 'credit',
    'base-staticdata-codelist': 'codelist',
    'base-staticdata-identifier': 'identifier',
    'base-staticdata-party': 'party',
    'event-common': 'event',
    'event-position': 'position',
    'event-workflow': 'workflow',
    'legaldocumentation-common': 'legaldoc',
    'legaldocumentation-csa': 'csa',
    'legaldocumentation-master-icma': 'legaldoc',
    'legaldocumentation-master-isda': 'legaldoc',
    'legaldocumentation-master-isla': 'legaldoc',
    'legaldocumentation-master': 'legaldoc',
    'legaldocumentation-transaction-additionalterms': 'legaldoc',
    'legaldocumentation-transaction': 'legaldoc',
    'margin-schedule': 'margin',
    'observable-asset-calculatedrate': 'observable',
    'observable-asset-fro': 'observable',
    'observable-asset': 'observable',
    'observable-common': 'observable',
    'observable-event': 'observable',
    'product-asset-floatingrate': 'product',
    'product-asset': 'product',
    'product-collateral': 'collateral',
    'product-common-schedule': 'schedule',
    'product-common-settlement': 'settlement',
    'product-common': 'product',
    'product-template': 'template',
    'regulation': 'regulation',
}

DOMAIN_DISPLAY = {
    'datetime': 'Date & Time',
    'math': 'Mathematics & Quantities',
    'asset': 'Asset & Security Reference Data',
    'credit': 'Credit Reference Data',
    'codelist': 'Code Lists & Classifications',
    'identifier': 'Identifiers',
    'party': 'Party & Counterparty',
    'event': 'Business Events & Lifecycle',
    'position': 'Positions & Portfolio',
    'workflow': 'Workflow & Processing',
    'legaldoc': 'Legal Documentation',
    'csa': 'Credit Support Annex (CSA)',
    'margin': 'Margin & Collateral Scheduling',
    'observable': 'Market Observables & Indices',
    'product': 'Product & Payout Definitions',
    'collateral': 'Collateral Management',
    'schedule': 'Calculation Schedules',
    'settlement': 'Settlement & Delivery',
    'template': 'Product Templates',
    'regulation': 'Regulatory Reporting',
}

WHEN_TO_USE = {
    'datetime': 'Use for date adjustments, business day conventions, calculation periods, and date schedules',
    'math': 'Use for quantities, measures, rounding rules, and mathematical concepts',
    'asset': 'Use for asset identification, classification, taxonomy, and reference data',
    'credit': 'Use for credit obligations, credit entities, and credit-specific reference data',
    'codelist': 'Use for standardized code lists and field value classifications',
    'identifier': 'Use for trade identifiers, product identifiers, and identifier schemes',
    'party': 'Use for parties, counterparties, accounts, and party roles in transactions',
    'event': 'Use for trade lifecycle events like execution, amendment, termination, novation, and exercise',
    'position': 'Use for portfolio positions, position states, and aggregated holdings',
    'workflow': 'Use for workflow steps, event timestamps, message headers, and processing states',
    'legaldoc': 'Use for master agreements, legal documentation, and contractual terms',
    'csa': 'Use for credit support annexes, collateral eligibility, and margin terms',
    'margin': 'Use for margin schedules, initial margin, and variation margin calculations',
    'observable': 'Use for market observables, floating rate indices, price sources, and calculated rates',
    'product': 'Use for derivative product definitions, payouts, economic terms, and asset payoffs',
    'collateral': 'Use for collateral portfolios, eligibility criteria, and collateral treatment',
    'schedule': 'Use for payment schedules, calculation period schedules, and date generation',
    'settlement': 'Use for settlement instructions, delivery methods, and cash/physical settlement',
    'template': 'Use for product templates, economic terms, and transferable product definitions',
    'regulation': 'Use for regulatory reporting fields, EMIR, MiFID, Dodd-Frank, and other regulations',
}

# Rosetta built-in types → Pure types
ROSETTA_TO_PURE = {
    'string': 'String',
    'int': 'Integer',
    'number': 'Float',
    'boolean': 'Boolean',
    'date': 'StrictDate',
    'time': 'String',
    'zonedDateTime': 'DateTime',
    'calculation': 'String',
    'productType': 'String',
    'eventType': 'String',
}

# Collect all CDM type names for reference resolution
ALL_CDM_TYPES = set()
ALL_CDM_ENUMS = set()


def collect_all_types():
    """Pre-scan all rosetta files to collect type and enum names."""
    for fname in os.listdir(ROSETTA_DIR):
        if not fname.endswith('.rosetta'):
            continue
        with open(os.path.join(ROSETTA_DIR, fname)) as f:
            content = f.read()
        for m in re.finditer(r'^type\s+(\w+)', content, re.MULTILINE):
            ALL_CDM_TYPES.add(m.group(1))
        for m in re.finditer(r'^enum\s+(\w+)', content, re.MULTILINE):
            ALL_CDM_ENUMS.add(m.group(1))


def rosetta_type_to_pure(type_name):
    """Convert a Rosetta type to Pure type."""
    if type_name in ROSETTA_TO_PURE:
        return ROSETTA_TO_PURE[type_name], False
    if type_name in ALL_CDM_ENUMS:
        return 'String', False  # Enums become String
    if type_name in ALL_CDM_TYPES:
        return type_name, True  # Reference to another CDM type
    return 'String', False  # Unknown → String


def rosetta_mult_to_pure(mult_str):
    """Convert Rosetta multiplicity (0..1), (1..1), (0..*), (1..*) to Pure."""
    mult_str = mult_str.strip()
    mapping = {
        '(0..1)': '[0..1]',
        '(1..1)': '[1]',
        '(0..*)': '[*]',
        '(1..*)': '[1..*]',
    }
    return mapping.get(mult_str, '[0..1]')


def to_camel(name):
    """Ensure camelCase (first letter lowercase)."""
    if not name:
        return name
    return name[0].lower() + name[1:]


def classify_file(fname):
    """Classify a rosetta file into a domain."""
    base = fname.replace('.rosetta', '')
    # Remove -type, -enum, -func suffixes
    base = re.sub(r'-(type|enum|func)$', '', base)
    return FILE_TO_DOMAIN.get(base, 'other')


def parse_rosetta_types(filepath):
    """Parse a Rosetta file and extract type definitions with their attributes."""
    with open(filepath) as f:
        content = f.read()

    # Extract namespace
    ns_match = re.search(r'^namespace\s+([\w.]+)', content, re.MULTILINE)
    namespace = ns_match.group(1) if ns_match else ''

    types = []
    # Pattern for type definitions:
    # type Name extends Parent: <"description">
    # or: type Name: <"description">
    type_pattern = re.compile(
        r'^type\s+(\w+)(?:\s+extends\s+(\w+))?.*?(?:<"([^"]*?)">)?\s*$',
        re.MULTILINE
    )

    type_matches = list(type_pattern.finditer(content))

    for i, tm in enumerate(type_matches):
        type_name = tm.group(1)
        parent = tm.group(2) or ''
        description = tm.group(3) or ''

        # Get body until next type/enum/func or EOF
        start = tm.end()
        # Find next top-level declaration
        next_decl = re.search(r'^(?:type|enum|func|isProduct|namespace)\s', content[start:], re.MULTILINE)
        end = start + next_decl.start() if next_decl else len(content)
        body = content[start:end]

        # Extract attributes
        # Pattern: attrName Type (multiplicity) <"description">
        attr_pattern = re.compile(
            r'^\s+(\w+)\s+(\w+)\s+\((\d+\.\.[*\d]+)\)\s*(?:<"([^"]*?)">)?',
            re.MULTILINE
        )

        attrs = []
        for am in attr_pattern.finditer(body):
            attr_name = am.group(1)
            attr_type = am.group(2)
            attr_mult = f'({am.group(3)})'
            attr_desc = am.group(4) or ''

            # Skip if it's a condition, metadata annotation, etc.
            if attr_name in ('condition', 'metadata', 'docReference'):
                continue

            attrs.append({
                'name': attr_name,
                'type': attr_type,
                'multiplicity': attr_mult,
                'description': attr_desc,
            })

        if attrs:  # Only include types with attributes
            types.append({
                'name': type_name,
                'parent': parent,
                'namespace': namespace,
                'description': description,
                'attrs': attrs,
            })

    return types


def generate():
    """Main: parse all CDM rosetta files and generate Pure model."""
    collect_all_types()

    print(f"CDM types found: {len(ALL_CDM_TYPES)}")
    print(f"CDM enums found: {len(ALL_CDM_ENUMS)}")

    domain_models = defaultdict(list)

    for fname in sorted(os.listdir(ROSETTA_DIR)):
        if not fname.endswith('.rosetta'):
            continue
        # Only process type files (skip func and enum files)
        if '-func.rosetta' in fname or '-enum.rosetta' in fname:
            # But some func files define types too, so check
            pass

        filepath = os.path.join(ROSETTA_DIR, fname)
        domain = classify_file(fname)
        parsed = parse_rosetta_types(filepath)

        for t in parsed:
            t['domain'] = domain
            domain_models[domain].append(t)

    # ─── Generate Pure output ───
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// ISDA Common Domain Model (CDM) — Auto-generated from Rosetta DSL")
    total = sum(len(v) for v in domain_models.values())
    lines.append(f"// Source: {total} types from finos/common-domain-model")
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
    seen_classes = set()

    # Track which CDM types we actually emit (for reference resolution)
    emitted_types = set()

    for domain in sorted(domain_models.keys()):
        models = domain_models[domain]
        display = DOMAIN_DISPLAY.get(domain, domain.title())
        wtu = WHEN_TO_USE.get(domain, '')

        lines.append(f"// ─── {display} ({len(models)} types) ───")
        lines.append("")

        for model in models:
            qualified = f"{domain}::{model['name']}"
            if qualified in seen_classes:
                continue
            seen_classes.add(qualified)
            emitted_types.add(model['name'])
            class_count += 1

            # Determine stereotype
            numeric_attrs = sum(1 for a in model['attrs']
                                if rosetta_type_to_pure(a['type'])[0] in ('Float', 'Integer'))
            stereotype = "metric" if numeric_attrs > len(model['attrs']) / 2 else "dimension"

            desc = model['description']
            if not desc:
                desc = model['name']
            # Escape single quotes and truncate
            desc = desc.replace("'", "\\'").replace('\n', ' ').strip()
            if len(desc) > 150:
                desc = desc[:147] + "..."

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
            for attr in model['attrs']:
                prop_name = to_camel(attr['name'])
                if prop_name in seen_props:
                    continue
                seen_props.add(prop_name)

                pure_type, is_ref = rosetta_type_to_pure(attr['type'])
                pure_mult = rosetta_mult_to_pure(attr['multiplicity'])

                # If it's a reference to another CDM type, flatten to String
                # (we don't want cross-references in the Pure model for NLQ)
                if is_ref:
                    pure_type = 'String'

                attr_desc = attr['description']
                if not attr_desc:
                    attr_desc = attr['name']
                attr_desc = attr_desc.replace("'", "\\'").replace('\n', ' ').strip()
                if len(attr_desc) > 120:
                    attr_desc = attr_desc[:117] + "..."

                ptags = f"nlq::NlqProfile.description = '{attr_desc}'"

                # Unit hints
                name_lower = attr['name'].lower()
                if any(x in name_lower for x in ('amount', 'price', 'notional', 'fee', 'premium', 'value', 'strike')):
                    if pure_type == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'currency'"
                elif any(x in name_lower for x in ('rate', 'spread', 'coupon', 'percent')):
                    if pure_type == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'percent'"
                elif 'quantity' in name_lower or 'weight' in name_lower:
                    if pure_type == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'units'"

                lines.append(f"    {{{ptags}}}")
                lines.append(f"    {prop_name}: {pure_type}{pure_mult};")
                total_props += 1

            lines.append("}")
            lines.append("")

    # ─── Generate Associations from type references ───
    # Each Rosetta attribute whose type is another CDM type → Association
    assoc_count = 0
    seen_assocs = set()

    # Build type_name → qualified Pure name lookup
    type_to_qualified = {}
    for domain in domain_models:
        for model in domain_models[domain]:
            qn = f"{model['domain']}::{model['name']}"
            if qn in seen_classes:
                type_to_qualified[model['name']] = qn

    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// Associations — derived from Rosetta type references")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")

    for domain in sorted(domain_models.keys()):
        for model in domain_models[domain]:
            src_qn = f"{model['domain']}::{model['name']}"
            if src_qn not in seen_classes:
                continue
            for attr in model['attrs']:
                attr_type = attr['type']
                if attr_type not in type_to_qualified:
                    continue
                tgt_qn = type_to_qualified[attr_type]
                if src_qn == tgt_qn:
                    continue

                assoc_key = f"{src_qn}_{tgt_qn}_{attr['name']}"
                if assoc_key in seen_assocs:
                    continue
                seen_assocs.add(assoc_key)

                src_simple = model['name']
                src_prop = src_simple[0].lower() + src_simple[1:]
                tgt_prop = to_camel(attr['name'])
                assoc_name = f"{src_simple}_{attr_type}_{attr['name']}"

                lines.append(f"Association cdm_assoc::{assoc_name}")
                lines.append("{")
                lines.append(f"    {src_prop}: {src_qn}[*];")
                lines.append(f"    {tgt_prop}: {tgt_qn}[*];")
                lines.append("}")
                lines.append("")
                assoc_count += 1

    # Write
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\nGenerated {OUTPUT_FILE}")
    print(f"  Classes:      {class_count}")
    print(f"  Properties:   {total_props}")
    print(f"  Associations: {assoc_count}")
    print(f"  Domains:      {len(domain_models)}")


if __name__ == "__main__":
    generate()
