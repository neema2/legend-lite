#!/usr/bin/env python3
"""
Convert FIBO (Financial Industry Business Ontology) OWL/RDF to a Pure semantic model.

Reads from: /Users/neema/legend/fibo/**/*.rdf
Outputs to: fibo-model.pure
"""

import os
import re
from collections import defaultdict

FIBO_DIR = "/Users/neema/legend/fibo"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "fibo-model.pure")

# Map FIBO top-level directories to Pure package names and display names
DOMAIN_MAP = {
    'SEC': ('securities', 'Securities & Financial Instruments'),
    'DER': ('derivatives', 'Derivatives'),
    'FBC': ('banking', 'Financial Business & Commerce'),
    'FND': ('foundations', 'Foundations & Common Concepts'),
    'BE': ('business_entities', 'Business Entities & Organizations'),
    'BP': ('business_processes', 'Business Processes'),
    'LOAN': ('loans', 'Loans & Lending'),
    'IND': ('indicators', 'Market Indicators & Indices'),
    'MD': ('market_data', 'Market Data'),
    'CAE': ('corporate_actions', 'Corporate Actions & Events'),
}

WHEN_TO_USE = {
    'securities': 'Use for securities, bonds, equities, funds, and financial instrument classification',
    'derivatives': 'Use for derivative contracts, options, swaps, forwards, and structured products',
    'banking': 'Use for banking products, financial intermediaries, regulatory compliance, and financial services',
    'foundations': 'Use for common financial concepts like amounts, currencies, dates, quantities, and agreements',
    'business_entities': 'Use for legal entities, corporations, partnerships, trusts, and organizational structures',
    'business_processes': 'Use for business processes, transactions, settlements, and clearing workflows',
    'loans': 'Use for loan products, mortgages, credit facilities, and lending terms',
    'indicators': 'Use for market indices, interest rates, FX rates, and economic indicators',
    'market_data': 'Use for market data, price quotes, trading volumes, and market statistics',
    'corporate_actions': 'Use for corporate actions, dividends, stock splits, mergers, and reorganizations',
}

# XSD type → Pure type
XSD_TO_PURE = {
    'string': 'String',
    'boolean': 'Boolean',
    'decimal': 'Float',
    'float': 'Float',
    'double': 'Float',
    'integer': 'Integer',
    'int': 'Integer',
    'long': 'Integer',
    'nonNegativeInteger': 'Integer',
    'positiveInteger': 'Integer',
    'dateTime': 'DateTime',
    'date': 'StrictDate',
    'anyURI': 'String',
    'token': 'String',
}


def to_camel(name):
    """Convert label to camelCase property name."""
    # Remove special chars, split on spaces/hyphens
    words = re.split(r'[\s\-_/]+', name.strip())
    if not words:
        return name
    result = words[0].lower()
    for w in words[1:]:
        if w:
            result += w[0].upper() + w[1:]
    # Remove non-alphanum
    result = re.sub(r'[^a-zA-Z0-9]', '', result)
    return result


def to_class_name(label):
    """Convert label to PascalCase class name."""
    words = re.split(r'[\s\-_/]+', label.strip())
    result = ''.join(w.capitalize() for w in words if w)
    result = re.sub(r'[^a-zA-Z0-9]', '', result)
    return result


def extract_local_name(uri):
    """Extract local name from a URI."""
    if '#' in uri:
        return uri.split('#')[-1]
    return uri.split('/')[-1]


def parse_fibo():
    """Parse all FIBO RDF files and extract classes and properties."""
    classes = {}  # uri -> {label, definition, domain_key, sub_path}
    data_props = {}  # uri -> {label, definition, domain_uri, range_type}
    obj_props = {}  # uri -> {label, definition, domain_uri, range_uri}

    for root, dirs, files in os.walk(FIBO_DIR):
        dirs[:] = [x for x in dirs if x != '.git']
        for fname in files:
            if not fname.endswith('.rdf'):
                continue
            filepath = os.path.join(root, fname)
            rel_path = os.path.relpath(filepath, FIBO_DIR)
            parts = rel_path.split('/')
            domain_key = parts[0] if parts[0] in DOMAIN_MAP else None
            if not domain_key:
                continue

            try:
                content = open(filepath, errors='ignore').read()
            except:
                continue

            # Extract classes
            for m in re.finditer(
                r'<owl:Class rdf:about="([^"]+)"[^/]*?>(.*?)</owl:Class>',
                content, re.DOTALL
            ):
                uri = m.group(1)
                body = m.group(2)
                label_m = re.search(r'<rdfs:label[^>]*>([^<]+)', body)
                defn_m = re.search(r'<skos:definition[^>]*>([^<]+)', body)
                if label_m:
                    classes[uri] = {
                        'label': label_m.group(1).strip(),
                        'definition': defn_m.group(1).strip() if defn_m else '',
                        'domain_key': domain_key,
                        'sub_path': '/'.join(parts[1:-1]),
                    }

            # Extract data properties
            for m in re.finditer(
                r'<owl:DatatypeProperty rdf:about="([^"]+)"[^/]*?>(.*?)</owl:DatatypeProperty>',
                content, re.DOTALL
            ):
                uri = m.group(1)
                body = m.group(2)
                label_m = re.search(r'<rdfs:label[^>]*>([^<]+)', body)
                defn_m = re.search(r'<skos:definition[^>]*>([^<]+)', body)
                domain_m = re.search(r'<rdfs:domain rdf:resource="([^"]+)"', body)
                range_m = re.search(r'<rdfs:range rdf:resource="([^"]+)"', body)
                if label_m:
                    data_props[uri] = {
                        'label': label_m.group(1).strip(),
                        'definition': defn_m.group(1).strip() if defn_m else '',
                        'domain_uri': domain_m.group(1) if domain_m else '',
                        'range_type': range_m.group(1) if range_m else '',
                    }

            # Extract object properties
            for m in re.finditer(
                r'<owl:ObjectProperty rdf:about="([^"]+)"[^/]*?>(.*?)</owl:ObjectProperty>',
                content, re.DOTALL
            ):
                uri = m.group(1)
                body = m.group(2)
                label_m = re.search(r'<rdfs:label[^>]*>([^<]+)', body)
                defn_m = re.search(r'<skos:definition[^>]*>([^<]+)', body)
                domain_m = re.search(r'<rdfs:domain rdf:resource="([^"]+)"', body)
                range_m = re.search(r'<rdfs:range rdf:resource="([^"]+)"', body)
                if label_m:
                    obj_props[uri] = {
                        'label': label_m.group(1).strip(),
                        'definition': defn_m.group(1).strip() if defn_m else '',
                        'domain_uri': domain_m.group(1) if domain_m else '',
                        'range_uri': range_m.group(1) if range_m else '',
                    }

    # ─── Extract restriction-based relationships ───
    # OWL classes declare relationships via owl:Restriction inside rdfs:subClassOf
    # Pattern: Class → subClassOf → Restriction → onProperty + (onClass | someValuesFrom)
    # This is how FIBO encodes "ClassA relates to ClassB via PropertyP"
    class_restrictions = []  # list of (class_uri, property_uri, target_uri)

    for root2, dirs2, files2 in os.walk(FIBO_DIR):
        dirs2[:] = [x for x in dirs2 if x != '.git']
        for fname2 in files2:
            if not fname2.endswith('.rdf'):
                continue
            filepath2 = os.path.join(root2, fname2)
            try:
                content2 = open(filepath2, errors='ignore').read()
            except:
                continue

            # Find each class and its restriction blocks
            for cm in re.finditer(
                r'<owl:Class rdf:about="([^"]+)"[^/]*?>(.*?)</owl:Class>',
                content2, re.DOTALL
            ):
                class_uri = cm.group(1)
                class_body = cm.group(2)
                if class_uri not in classes:
                    continue

                # Find all Restriction blocks within this class
                for rm in re.finditer(r'<owl:Restriction>(.*?)</owl:Restriction>', class_body, re.DOTALL):
                    rbody = rm.group(1)
                    on_prop = re.search(r'owl:onProperty rdf:resource="([^"]+)"', rbody)
                    on_class = re.search(r'owl:onClass rdf:resource="([^"]+)"', rbody)
                    some_vals = re.search(r'owl:someValuesFrom rdf:resource="([^"]+)"', rbody)
                    target = on_class or some_vals
                    if on_prop and target:
                        target_uri = target.group(1)
                        prop_uri = on_prop.group(1)
                        # Only keep if target is a known FIBO class
                        if target_uri in classes:
                            class_restrictions.append((class_uri, prop_uri, target_uri))

    return classes, data_props, obj_props, class_restrictions


def map_range_to_pure(range_uri):
    """Map an XSD/OWL range URI to a Pure type."""
    if not range_uri:
        return 'String'
    local = extract_local_name(range_uri)
    if local in XSD_TO_PURE:
        return XSD_TO_PURE[local]
    return 'String'


def generate():
    """Main generation function."""
    print("Parsing FIBO RDF files...")
    classes, data_props, obj_props, class_restrictions = parse_fibo()
    print(f"  Classes: {len(classes)}")
    print(f"  Data properties: {len(data_props)}")
    print(f"  Object properties: {len(obj_props)}")
    print(f"  Class restrictions (class→class): {len(class_restrictions)}")

    # Build class → properties mapping
    class_props = defaultdict(list)  # class_uri -> [prop_info]

    for uri, prop in data_props.items():
        if prop['domain_uri'] and prop['domain_uri'] in classes:
            pure_type = map_range_to_pure(prop['range_type'])
            class_props[prop['domain_uri']].append({
                'name': to_camel(prop['label']),
                'type': pure_type,
                'description': prop['definition'] or prop['label'],
            })

    for uri, prop in obj_props.items():
        if prop['domain_uri'] and prop['domain_uri'] in classes:
            class_props[prop['domain_uri']].append({
                'name': to_camel(prop['label']),
                'type': 'String',  # Flatten object references to String
                'description': prop['definition'] or prop['label'],
            })

    # For classes with no explicit properties, add a name/identifier property
    for uri, cls in classes.items():
        if uri not in class_props:
            class_props[uri].append({
                'name': 'name',
                'type': 'String',
                'description': f'Name or identifier for {cls["label"]}',
            })

    # Group by domain
    domain_classes = defaultdict(list)
    for uri, cls in classes.items():
        pkg, _ = DOMAIN_MAP.get(cls['domain_key'], ('other', 'Other'))
        domain_classes[pkg].append((uri, cls))

    # Generate Pure
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// FIBO (Financial Industry Business Ontology) — Auto-generated from OWL/RDF")
    lines.append(f"// Source: {len(classes)} classes from edmcouncil/fibo")
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

    for pkg in sorted(domain_classes.keys()):
        entries = domain_classes[pkg]
        display = dict(DOMAIN_MAP.values()).get(pkg, pkg.title())
        # Find display from DOMAIN_MAP
        for k, (p, d) in DOMAIN_MAP.items():
            if p == pkg:
                display = d
                break
        wtu = WHEN_TO_USE.get(pkg, '')

        lines.append(f"// ─── {display} ({len(entries)} types) ───")
        lines.append("")

        for uri, cls in entries:
            class_name = to_class_name(cls['label'])
            qualified = f"{pkg}::{class_name}"
            if qualified in seen:
                continue
            seen.add(qualified)
            class_count += 1

            desc = cls['definition'] or cls['label']
            desc = desc.replace("'", "\\'").replace('\n', ' ').strip()
            if len(desc) > 150:
                desc = desc[:147] + "..."

            props = class_props.get(uri, [])
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
                # Ensure valid identifier
                pname = re.sub(r'[^a-zA-Z0-9]', '', pname)
                if not pname or not pname[0].isalpha():
                    continue
                pname = pname[0].lower() + pname[1:]
                if pname in seen_props:
                    continue
                seen_props.add(pname)

                pdesc = prop['description'].replace("'", "\\'").replace('\n', ' ').strip()
                if len(pdesc) > 120:
                    pdesc = pdesc[:117] + "..."

                ptags = f"nlq::NlqProfile.description = '{pdesc}'"

                # Unit hints
                nl = pname.lower()
                if any(x in nl for x in ('amount', 'price', 'value', 'fee', 'premium', 'notional')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'currency'"
                elif any(x in nl for x in ('rate', 'spread', 'percent', 'yield', 'coupon')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'percent'"

                lines.append(f"    {{{ptags}}}")
                lines.append(f"    {pname}: {prop['type']}[0..1];")
                total_props += 1

            lines.append("}")
            lines.append("")

    # ─── Generate Associations from ObjectProperties ───
    # Each OWL ObjectProperty with both domain and range pointing to emitted FIBO
    # classes becomes a Pure Association (following FIB-DM approach).
    # Build URI → qualified Pure name lookup
    uri_to_qualified = {}
    for uri, cls in classes.items():
        pkg, _ = DOMAIN_MAP.get(cls['domain_key'], ('other', 'Other'))
        qn = f"{pkg}::{to_class_name(cls['label'])}"
        if qn in seen:
            uri_to_qualified[uri] = qn

    assoc_count = 0
    seen_assocs = set()

    # Build property URI → label lookup for naming associations
    prop_uri_to_label = {}
    for uri, prop in obj_props.items():
        if prop.get('label'):
            prop_uri_to_label[uri] = prop['label']

    def emit_assoc(src_qn, tgt_qn, rel_label):
        """Emit a Pure Association between two qualified class names."""
        nonlocal assoc_count
        if src_qn == tgt_qn:
            return
        prop_label = to_camel(rel_label)
        prop_label = re.sub(r'[^a-zA-Z0-9]', '', prop_label)
        if not prop_label or not prop_label[0].isalpha():
            return
        assoc_key = f"{src_qn}_{tgt_qn}_{prop_label}"
        if assoc_key in seen_assocs:
            return
        seen_assocs.add(assoc_key)

        src_simple = src_qn.split('::')[1]
        src_prop = src_simple[0].lower() + src_simple[1:]
        assoc_name = f"{src_simple}_{tgt_qn.split('::')[1]}_{prop_label}"

        lines.append(f"Association fibo_assoc::{assoc_name}")
        lines.append("{")
        lines.append(f"    {src_prop}: {src_qn}[*];")
        lines.append(f"    {prop_label}: {tgt_qn}[*];")
        lines.append("}")
        lines.append("")
        assoc_count += 1

    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// Associations — from OWL ObjectProperties (domain/range)")
    lines.append("// + OWL Restrictions (onProperty + onClass/someValuesFrom)")
    lines.append("// Following FIB-DM: each ObjectProperty → Association")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")

    # Source 1: ObjectProperties with explicit domain/range
    for uri, prop in obj_props.items():
        d_uri = prop['domain_uri']
        r_uri = prop['range_uri']
        if d_uri in uri_to_qualified and r_uri in uri_to_qualified:
            emit_assoc(uri_to_qualified[d_uri], uri_to_qualified[r_uri], prop['label'])

    # Source 2: Class restrictions (the bulk of FIBO relationships)
    for class_uri, prop_uri, target_uri in class_restrictions:
        if class_uri not in uri_to_qualified or target_uri not in uri_to_qualified:
            continue
        # Get a name for this relationship from the property URI
        rel_name = prop_uri_to_label.get(prop_uri, '')
        if not rel_name:
            rel_name = extract_local_name(prop_uri).replace('&', '').replace(';', '')
        emit_assoc(uri_to_qualified[class_uri], uri_to_qualified[target_uri], rel_name)

    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\nGenerated {OUTPUT_FILE}")
    print(f"  Classes:      {class_count}")
    print(f"  Properties:   {total_props}")
    print(f"  Associations: {assoc_count} (from {len(obj_props)} ObjectProps + {len(class_restrictions)} Restrictions)")
    print(f"  Domains:      {len(domain_classes)}")


if __name__ == "__main__":
    generate()
