#!/usr/bin/env python3
"""
Convert ACTUS (Algorithmic Contract Types Unified Standards) JSON dictionaries to Pure.

Reads from: /Users/neema/legend/actus-dictionary/
Outputs to: actus-model.pure
"""

import os
import re
import json
from collections import defaultdict

DICT_DIR = "/Users/neema/legend/actus-dictionary"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "actus-model.pure")

ACTUS_TYPE_TO_PURE = {
    'Real': 'Float',
    'Integer': 'Integer',
    'Varchar': 'String',
    'Timestamp': 'DateTime',
    'Enum': 'String',
    'Period': 'String',
    'Cycle': 'String',
    'ContractReference': 'String',
}

FAMILY_TO_DOMAIN = {
    'Basic': 'basic',
    'Combined': 'combined',
    'Credit': 'credit',
}

CLASS_TO_DOMAIN = {
    'Fixed Income': 'fixed_income',
    'Asymmetric': 'asymmetric',
    'Symmetric': 'symmetric',
    'Ownership': 'ownership',
    'Credit Enhancement': 'credit_enhancement',
}

DOMAIN_DISPLAY = {
    'fixed_income': 'Fixed Income Contracts',
    'asymmetric': 'Asymmetric (Option-like) Contracts',
    'symmetric': 'Symmetric (Swap-like) Contracts',
    'ownership': 'Ownership Contracts',
    'credit_enhancement': 'Credit Enhancement Contracts',
    'other': 'Other Contract Types',
}

WHEN_TO_USE = {
    'fixed_income': 'Use for bonds, annuities, loans, mortgages, and fixed income instruments',
    'asymmetric': 'Use for options, caps, floors, and other asymmetric payoff structures',
    'symmetric': 'Use for swaps, forwards, futures, and symmetric derivative contracts',
    'ownership': 'Use for cash positions, stock ownership, and direct asset holdings',
    'credit_enhancement': 'Use for guarantees, credit default swaps, and credit enhancement instruments',
    'other': 'Use for other financial contract types',
}


def to_camel(name):
    if not name:
        return name
    return name[0].lower() + name[1:]


def generate():
    # Load terms
    with open(os.path.join(DICT_DIR, 'actus-dictionary-terms.json')) as f:
        terms_data = json.load(f)
    terms = terms_data['terms']
    print(f"ACTUS terms: {len(terms)}")

    # Load taxonomy (contract types)
    with open(os.path.join(DICT_DIR, 'actus-dictionary-taxonomy.json')) as f:
        tax_data = json.load(f)
    taxonomy = tax_data['taxonomy']
    print(f"ACTUS contract types: {len(taxonomy)}")

    # Load applicability (which terms apply to which contract type)
    with open(os.path.join(DICT_DIR, 'actus-dictionary-applicability.json')) as f:
        app_data = json.load(f)
    applicability = app_data['applicability']

    # Build term lookup
    term_lookup = {}
    for tid, tinfo in terms.items():
        pure_type = ACTUS_TYPE_TO_PURE.get(tinfo.get('type', ''), 'String')
        term_lookup[tid] = {
            'name': to_camel(tinfo.get('identifier', tid)),
            'type': pure_type,
            'description': tinfo.get('description', tinfo.get('name', tid)),
            'group': tinfo.get('group', ''),
            'acronym': tinfo.get('acronym', ''),
        }

    # Build contract type classes
    contract_types = []
    for ct_id, ct_info in taxonomy.items():
        ct_class = ct_info.get('class', 'Other')
        domain = CLASS_TO_DOMAIN.get(ct_class, 'other')

        # Get applicable terms
        applicable_terms = []
        if ct_id in applicability:
            app_entry = applicability[ct_id]
            for term_id in terms.keys():
                if term_id in app_entry:
                    val = app_entry[term_id]
                    if val and val not in ('', 'n/a', 'NA'):
                        if term_id in term_lookup:
                            applicable_terms.append(term_lookup[term_id])

        contract_types.append({
            'name': ct_info.get('name', ct_id),
            'acronym': ct_info.get('acronym', ''),
            'description': ct_info.get('description', ''),
            'coverage': ct_info.get('coverage', ''),
            'domain': domain,
            'family': ct_info.get('family', ''),
            'class': ct_class,
            'terms': applicable_terms,
        })

    # Group by domain
    domain_types = defaultdict(list)
    for ct in contract_types:
        domain_types[ct['domain']].append(ct)

    # Generate Pure
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// ACTUS (Algorithmic Contract Types Unified Standards) — Auto-generated")
    lines.append(f"// Source: {len(contract_types)} contract types, {len(terms)} terms")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")
    lines.append("Profile nlq::NlqProfile")
    lines.append("{")
    lines.append("    stereotypes: [dimension, metric];")
    lines.append("    tags: [description, synonyms, sampleValues, unit, importance, businessDomain, exampleQuestions, whenToUse];")
    lines.append("}")
    lines.append("")

    total_props = 0
    class_count = 0

    for domain in sorted(domain_types.keys()):
        entries = domain_types[domain]
        display = DOMAIN_DISPLAY.get(domain, domain.title())
        wtu = WHEN_TO_USE.get(domain, '')

        lines.append(f"// ─── {display} ({len(entries)} types) ───")
        lines.append("")

        for ct in entries:
            class_name = re.sub(r'[^a-zA-Z0-9]', '', ct['name'].replace(' ', ''))
            if not class_name:
                continue
            qualified = f"{domain}::{class_name}"
            class_count += 1

            desc = ct['description'] or ct['name']
            desc = desc.replace("'", "\\'").replace('\n', ' ').strip()
            if len(desc) > 150:
                desc = desc[:147] + "..."

            coverage = ct.get('coverage', '')
            if coverage:
                coverage = coverage.replace("'", "\\'").replace('\n', ' ').strip()

            ct_terms = ct['terms']
            numeric = sum(1 for t in ct_terms if t['type'] in ('Float', 'Integer'))
            stereotype = "metric" if numeric > len(ct_terms) / 2 else "dimension"

            lines.append(f"Class <<nlq::NlqProfile.{stereotype}>>")
            tag_line = f"  {{nlq::NlqProfile.description = '{desc}'"
            tag_line += f",\n   nlq::NlqProfile.businessDomain = '{display}'"
            if wtu:
                tag_line += f",\n   nlq::NlqProfile.whenToUse = '{wtu}'"
            if coverage:
                if len(coverage) > 100:
                    coverage = coverage[:97] + "..."
                tag_line += f",\n   nlq::NlqProfile.synonyms = '{coverage}'"
            tag_line += "}"
            lines.append(tag_line)
            lines.append(qualified)
            lines.append("{")

            seen_props = set()
            for term in ct_terms:
                pname = term['name']
                if not pname or pname in seen_props:
                    continue
                pname = re.sub(r'[^a-zA-Z0-9]', '', pname)
                if not pname or not pname[0].isalpha():
                    continue
                pname = pname[0].lower() + pname[1:]
                if pname in seen_props:
                    continue
                seen_props.add(pname)

                pdesc = ' '.join(term['description'].replace("'", "\\'").split())
                if len(pdesc) > 120:
                    pdesc = pdesc[:117] + "..."

                ptags = f"nlq::NlqProfile.description = '{pdesc}'"
                nl = pname.lower()
                if any(x in nl for x in ('amount', 'notional', 'price', 'value', 'fee', 'premium')):
                    if term['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'currency'"
                elif any(x in nl for x in ('rate', 'spread', 'coupon', 'yield')):
                    if term['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'percent'"

                lines.append(f"    {{{ptags}}}")
                lines.append(f"    {pname}: {term['type']}[0..1];")
                total_props += 1

            lines.append("}")
            lines.append("")

    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\nGenerated {OUTPUT_FILE}")
    print(f"  Classes:    {class_count}")
    print(f"  Properties: {total_props}")
    print(f"  Domains:    {len(domain_types)}")


if __name__ == "__main__":
    generate()
