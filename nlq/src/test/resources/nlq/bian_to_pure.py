#!/usr/bin/env python3
"""
Convert BIAN (Banking Industry Architecture Network) OpenAPI schemas to Pure.

Reads from: /Users/neema/legend/bian/release14.0.0/semantic-apis/oas3 /yamls/*.yaml
Outputs to: bian-model.pure
"""

import os
import re
import yaml
from collections import defaultdict

BIAN_DIR = "/Users/neema/legend/bian/release14.0.0/semantic-apis/oas3 /yamls"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "bian-model.pure")

# Classify API files into banking domains
DOMAIN_KEYWORDS = {
    'account': 'accounts',
    'loan': 'lending',
    'mortgage': 'lending',
    'credit': 'credit',
    'payment': 'payments',
    'card': 'cards',
    'deposit': 'deposits',
    'saving': 'deposits',
    'trade': 'trading',
    'order': 'trading',
    'market': 'trading',
    'position': 'trading',
    'settlement': 'settlement',
    'clearing': 'settlement',
    'collateral': 'collateral',
    'compliance': 'compliance',
    'regulatory': 'compliance',
    'fraud': 'compliance',
    'risk': 'risk',
    'customer': 'customer',
    'party': 'customer',
    'product': 'product',
    'channel': 'channels',
    'branch': 'channels',
    'atm': 'channels',
    'investment': 'investment',
    'portfolio': 'investment',
    'asset': 'investment',
    'fund': 'investment',
    'insurance': 'insurance',
    'underwriting': 'insurance',
    'document': 'operations',
    'correspondence': 'operations',
    'archive': 'operations',
    'leasing': 'lending',
    'factoring': 'lending',
    'securities': 'securities',
    'custody': 'securities',
    'broker': 'securities',
}

DOMAIN_DISPLAY = {
    'accounts': 'Account Management',
    'lending': 'Lending & Credit Products',
    'credit': 'Credit Assessment & Scoring',
    'payments': 'Payments & Transfers',
    'cards': 'Card Services',
    'deposits': 'Deposits & Savings',
    'trading': 'Trading & Order Management',
    'settlement': 'Settlement & Clearing',
    'collateral': 'Collateral Management',
    'compliance': 'Compliance & Regulatory',
    'risk': 'Risk Management',
    'customer': 'Customer Management',
    'product': 'Product Management',
    'channels': 'Channels & Distribution',
    'investment': 'Investment Management',
    'insurance': 'Insurance Products',
    'operations': 'Operations & Administration',
    'securities': 'Securities Services',
    'other': 'Other Banking Services',
}

WHEN_TO_USE = {
    'accounts': 'Use for bank accounts, account reconciliation, account recovery, and account statements',
    'lending': 'Use for loans, mortgages, leasing, factoring, and credit facilities',
    'credit': 'Use for credit assessment, credit scoring, credit bureau, and creditworthiness',
    'payments': 'Use for payment processing, transfers, direct debits, and payment orders',
    'cards': 'Use for credit cards, debit cards, card authorization, and card transactions',
    'deposits': 'Use for current accounts, savings accounts, term deposits, and deposit products',
    'trading': 'Use for market orders, program trading, trader positions, and trade execution',
    'settlement': 'Use for trade settlement, clearing, and post-trade processing',
    'collateral': 'Use for collateral management, margin calls, and collateral valuation',
    'compliance': 'Use for regulatory compliance, fraud detection, KYC, and AML',
    'risk': 'Use for risk assessment, risk models, and operational risk',
    'customer': 'Use for customer profiles, customer relationships, and party management',
    'product': 'Use for banking product design, product directory, and product features',
    'channels': 'Use for branch operations, ATM networks, online banking, and service channels',
    'investment': 'Use for investment portfolios, asset management, fund administration, and wealth management',
    'insurance': 'Use for insurance products, claims, underwriting, and policy management',
    'operations': 'Use for document management, correspondence, archiving, and back-office operations',
    'securities': 'Use for securities custody, brokerage, and securities lending',
    'other': 'Use for miscellaneous banking services',
}

OPENAPI_TO_PURE = {
    'string': 'String',
    'number': 'Float',
    'integer': 'Integer',
    'boolean': 'Boolean',
}


def classify_api(filename):
    """Classify an API file into a banking domain."""
    name = filename.replace('.yaml', '').lower()
    for keyword, domain in DOMAIN_KEYWORDS.items():
        if keyword in name:
            return domain
    return 'other'


def to_camel(name):
    """Ensure camelCase."""
    if not name:
        return name
    name = re.sub(r'[^a-zA-Z0-9]', '', name)
    if not name:
        return 'value'
    return name[0].lower() + name[1:]


def generate():
    """Parse BIAN OpenAPI specs and generate Pure model."""
    unique_schemas = {}  # name -> {props, domain, source, description}

    files = sorted([f for f in os.listdir(BIAN_DIR) if f.endswith('.yaml')])
    print(f"Processing {len(files)} BIAN API specs...")

    for fname in files:
        filepath = os.path.join(BIAN_DIR, fname)
        domain = classify_api(fname)

        try:
            with open(filepath) as fh:
                spec = yaml.safe_load(fh)
        except:
            continue

        schemas = spec.get('components', {}).get('schemas', {})
        api_desc = spec.get('info', {}).get('description', '')

        for schema_name, schema_def in schemas.items():
            props = schema_def.get('properties', {})
            if len(props) < 2:
                continue  # Skip trivial schemas

            # Skip HTTP/error/generic schemas
            if schema_name in ('HTTPError',):
                continue

            # Deduplicate - keep first occurrence
            if schema_name in unique_schemas:
                continue

            prop_list = []
            for pname, pdef in props.items():
                ptype = pdef.get('type', '')
                pdesc = pdef.get('description', pname)
                if '$ref' in pdef:
                    ptype = 'string'  # Flatten refs
                if not ptype:
                    ptype = 'string'

                pure_type = OPENAPI_TO_PURE.get(ptype, 'String')
                prop_list.append({
                    'name': to_camel(pname),
                    'type': pure_type,
                    'description': pdesc or pname,
                })

            if prop_list:
                unique_schemas[schema_name] = {
                    'props': prop_list,
                    'domain': domain,
                    'source': fname.replace('.yaml', ''),
                    'description': schema_def.get('description', schema_name),
                }

    print(f"Unique schemas: {len(unique_schemas)}")

    # Group by domain
    domain_schemas = defaultdict(list)
    for name, info in unique_schemas.items():
        domain_schemas[info['domain']].append((name, info))

    # Generate Pure
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// BIAN (Banking Industry Architecture Network) — Auto-generated from OpenAPI specs")
    lines.append(f"// Source: {len(unique_schemas)} schemas from bian-official/public release 14.0.0")
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
    seen = set()

    for domain in sorted(domain_schemas.keys()):
        entries = domain_schemas[domain]
        display = DOMAIN_DISPLAY.get(domain, domain.title())
        wtu = WHEN_TO_USE.get(domain, '')

        lines.append(f"// ─── {display} ({len(entries)} schemas) ───")
        lines.append("")

        for schema_name, info in entries:
            # Create clean class name
            class_name = re.sub(r'[^a-zA-Z0-9]', '', schema_name)
            if not class_name or not class_name[0].isalpha():
                continue
            qualified = f"{domain}::{class_name}"
            if qualified in seen:
                continue
            seen.add(qualified)
            class_count += 1

            desc = (info['description'] or schema_name).replace("'", "\\'").replace('\n', ' ').strip()
            if len(desc) > 150:
                desc = desc[:147] + "..."

            props = info['props']
            numeric = sum(1 for p in props if p['type'] in ('Float', 'Integer'))
            stereotype = "metric" if numeric > len(props) / 2 else "dimension"

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
            for prop in props:
                pname = prop['name']
                if not pname or pname in seen_props:
                    continue
                if not pname[0].isalpha():
                    continue
                seen_props.add(pname)

                pdesc = prop['description'].replace("'", "\\'").replace('\n', ' ').strip()
                if len(pdesc) > 120:
                    pdesc = pdesc[:117] + "..."

                ptags = f"nlq::NlqProfile.description = '{pdesc}'"
                nl = pname.lower()
                if any(x in nl for x in ('amount', 'price', 'value', 'fee', 'balance')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'currency'"
                elif any(x in nl for x in ('rate', 'percent', 'yield')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'percent'"

                lines.append(f"    {{{ptags}}}")
                lines.append(f"    {pname}: {prop['type']}[0..1];")
                total_props += 1

            lines.append("}")
            lines.append("")

    # ─── Generate Associations from $ref relationships ───
    # Re-parse YAML files to find $ref links between schemas we emitted
    assoc_count = 0
    seen_assocs = set()

    # Build schema_name → qualified Pure name lookup
    schema_to_qn = {}
    for name, info in unique_schemas.items():
        cn = re.sub(r'[^a-zA-Z0-9]', '', name)
        if cn:
            qn = f"{info['domain']}::{cn}"
            if qn in seen:
                schema_to_qn[name] = qn

    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// Associations — derived from OpenAPI $ref relationships")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")

    for fname in files:
        filepath = os.path.join(BIAN_DIR, fname)
        try:
            with open(filepath) as fh:
                spec = yaml.safe_load(fh)
        except:
            continue
        schemas = spec.get('components', {}).get('schemas', {})
        for src_name, schema_def in schemas.items():
            if src_name not in schema_to_qn:
                continue
            src_qn = schema_to_qn[src_name]
            props = schema_def.get('properties', {})
            for pname, pdef in props.items():
                ref = pdef.get('$ref', '')
                if not ref:
                    items = pdef.get('items', {})
                    ref = items.get('$ref', '') if items else ''
                if not ref:
                    continue
                tgt_name = ref.split('/')[-1]
                if tgt_name not in schema_to_qn:
                    continue
                tgt_qn = schema_to_qn[tgt_name]
                if src_qn == tgt_qn:
                    continue
                assoc_key = f"{src_qn}_{tgt_qn}_{pname}"
                if assoc_key in seen_assocs:
                    continue
                seen_assocs.add(assoc_key)

                src_simple = src_qn.split('::')[1]
                src_prop = src_simple[0].lower() + src_simple[1:]
                tgt_prop = to_camel(pname)
                if not tgt_prop:
                    continue
                assoc_nm = f"{src_simple}_{tgt_name}_{pname}"
                assoc_nm = re.sub(r'[^a-zA-Z0-9_]', '', assoc_nm)

                lines.append(f"Association bian_assoc::{assoc_nm}")
                lines.append("{")
                lines.append(f"    {src_prop}: {src_qn}[*];")
                lines.append(f"    {tgt_prop}: {tgt_qn}[*];")
                lines.append("}")
                lines.append("")
                assoc_count += 1

    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\nGenerated {OUTPUT_FILE}")
    print(f"  Classes:      {class_count}")
    print(f"  Properties:   {total_props}")
    print(f"  Associations: {assoc_count}")
    print(f"  Domains:      {len(domain_schemas)}")


if __name__ == "__main__":
    generate()
