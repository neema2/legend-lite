#!/usr/bin/env python3
"""
Convert FIX Orchestra XML repository to a Pure semantic model.

Reads from: /Users/neema/legend/fix-orchestra/repository/src/test/resources/examples/mit_2016.xml
Outputs to: fix-model.pure
"""

import os
import re
import xml.etree.ElementTree as ET
from collections import defaultdict

FIX_XML = "/Users/neema/legend/fix-orchestra/repository/src/test/resources/examples/mit_2016.xml"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "fix-model.pure")

NS = {'fixr': 'http://fixprotocol.io/2022/orchestra/repository'}

CATEGORY_TO_DOMAIN = {
    'SingleGeneralOrderHandling': ('order', 'Order Management'),
    'CrossOrders': ('order', 'Order Management'),
    'MultilegOrders': ('order', 'Order Management'),
    'OrderMassHandling': ('order', 'Order Management'),
    'MarketData': ('market_data', 'Market Data'),
    'MarketStructureReferenceData': ('ref_data', 'Reference Data'),
    'SecuritiesReferenceData': ('ref_data', 'Reference Data'),
    'PartiesReferenceData': ('ref_data', 'Reference Data'),
    'Indication': ('indication', 'Indications of Interest'),
    'QuotationNegotiation': ('quote', 'Quotation & Negotiation'),
    'Allocation': ('allocation', 'Trade Allocation'),
    'Confirmation': ('confirmation', 'Trade Confirmation'),
    'TradeCapture': ('trade_capture', 'Trade Capture & Reporting'),
    'PositionMaintenance': ('position', 'Position Management'),
    'SettlementInstruction': ('settlement', 'Settlement'),
    'CollateralManagement': ('collateral', 'Collateral Management'),
    'MarginRequirementManagement': ('margin', 'Margin Requirements'),
    'RegistrationInstruction': ('registration', 'Registration'),
    'AccountReporting': ('account', 'Account Reporting'),
    'ProgramTrading': ('program', 'Program Trading'),
    'Network': ('network', 'Network & Session'),
    'Application': ('network', 'Network & Session'),
    'BusinessReject': ('network', 'Network & Session'),
    'UserManagement': ('network', 'Network & Session'),
    'EventCommunication': ('event', 'Event Communication'),
    'PartiesAction': ('parties', 'Party Management'),
}

WHEN_TO_USE = {
    'order': 'Use for new orders, order cancel/replace, order status, and execution reports',
    'market_data': 'Use for market data requests, snapshots, incremental updates, and statistics',
    'ref_data': 'Use for security definitions, trading session rules, and party reference data',
    'indication': 'Use for indications of interest and advertisements',
    'quote': 'Use for quote requests, quotes, mass quotes, and quote negotiation',
    'allocation': 'Use for trade allocation instructions and allocation reports',
    'confirmation': 'Use for trade confirmations and confirmation acknowledgments',
    'trade_capture': 'Use for trade capture reports and trade reporting',
    'position': 'Use for position reports and position maintenance',
    'settlement': 'Use for settlement instructions and settlement status',
    'collateral': 'Use for collateral assignments, reports, and inquiries',
    'margin': 'Use for margin requirement reports and margin calculations',
    'registration': 'Use for registration instructions and status',
    'account': 'Use for account-level reporting',
    'program': 'Use for program and list trading',
    'network': 'Use for session-level messages, logon, heartbeat, and test requests',
    'event': 'Use for event communication and news',
    'parties': 'Use for party actions and definitions',
}

FIX_TYPE_TO_PURE = {
    'String': 'String', 'char': 'String', 'MultipleCharValue': 'String',
    'MultipleStringValue': 'String', 'Country': 'String', 'Currency': 'String',
    'Exchange': 'String', 'Language': 'String', 'XMLData': 'String',
    'data': 'String', 'Pattern': 'String',
    'Price': 'Float', 'PriceOffset': 'Float', 'Amt': 'Float',
    'Qty': 'Float', 'Float': 'Float', 'Percentage': 'Float',
    'float': 'Float',
    'Int': 'Integer', 'int': 'Integer', 'Length': 'Integer',
    'NumInGroup': 'Integer', 'SeqNum': 'Integer', 'TagNum': 'Integer',
    'DayOfMonth': 'Integer',
    'Boolean': 'Boolean',
    'UTCTimestamp': 'DateTime', 'UTCTimeOnly': 'String', 'UTCDateOnly': 'StrictDate',
    'LocalMktDate': 'StrictDate', 'LocalMktTime': 'String',
    'MonthYear': 'String', 'TZTimeOnly': 'String', 'TZTimestamp': 'DateTime',
}


def to_camel(name):
    if not name:
        return name
    return name[0].lower() + name[1:]


def generate():
    tree = ET.parse(FIX_XML)
    root = tree.getroot()

    # Build field lookup: id -> {name, type, description}
    field_lookup = {}
    for field in root.findall('.//fixr:field', NS):
        fid = field.get('id')
        fname = field.get('name', '')
        ftype = field.get('type', 'String')
        # Get description from documentation
        doc = field.find('fixr:documentation', NS)
        fdesc = ''
        if doc is not None and doc.text:
            fdesc = doc.text.strip()
        # Resolve codeset types to String
        base_type = ftype.split('CodeSet')[0] if 'CodeSet' in ftype else ftype
        pure_type = FIX_TYPE_TO_PURE.get(base_type, 'String')
        field_lookup[fid] = {
            'name': fname,
            'type': pure_type,
            'description': fdesc or fname,
        }

    print(f"FIX fields: {len(field_lookup)}")

    # Build component lookup: id -> {name, field_ids}
    comp_lookup = {}
    for comp in root.findall('.//fixr:component', NS):
        cid = comp.get('id')
        cname = comp.get('name', '')
        field_ids = [fr.get('id') for fr in comp.findall('.//fixr:fieldRef', NS)]
        comp_lookup[cid] = {'name': cname, 'field_ids': field_ids}

    # Parse messages
    messages = []
    for msg in root.findall('.//fixr:message', NS):
        name = msg.get('name', '')
        category = msg.get('category', '')
        msg_type = msg.get('msgType', '')
        doc = msg.find('.//fixr:documentation', NS)
        desc = ''
        if doc is not None and doc.text:
            desc = doc.text.strip()

        # Collect all field refs (direct + from components)
        field_ids = set()
        for fr in msg.findall('.//fixr:fieldRef', NS):
            field_ids.add(fr.get('id'))
        for cr in msg.findall('.//fixr:componentRef', NS):
            cid = cr.get('id')
            if cid in comp_lookup:
                field_ids.update(comp_lookup[cid]['field_ids'])

        if field_ids:
            messages.append({
                'name': name,
                'category': category,
                'msgType': msg_type,
                'description': desc,
                'field_ids': field_ids,
            })

    # Deduplicate messages by name
    seen_msg = {}
    for msg in messages:
        if msg['name'] not in seen_msg:
            seen_msg[msg['name']] = msg
        else:
            # Merge field_ids
            seen_msg[msg['name']]['field_ids'].update(msg['field_ids'])

    messages = list(seen_msg.values())
    print(f"FIX messages (deduped): {len(messages)}")

    # Also create classes for significant components
    components = []
    for cid, comp in comp_lookup.items():
        if len(comp['field_ids']) >= 3:
            components.append({
                'name': comp['name'],
                'category': '',
                'description': f'FIX component: {comp["name"]}',
                'field_ids': set(comp['field_ids']),
            })

    # Deduplicate components by name and against messages
    msg_names = {m['name'] for m in messages}
    seen_comp = set()
    filtered_comps = []
    for comp in components:
        if comp['name'] not in msg_names and comp['name'] not in seen_comp:
            seen_comp.add(comp['name'])
            filtered_comps.append(comp)

    all_entries = messages + filtered_comps
    print(f"Total entries (messages + components): {len(all_entries)}")

    # Group by domain
    domain_entries = defaultdict(list)
    for entry in all_entries:
        cat = entry.get('category', '')
        domain, _ = CATEGORY_TO_DOMAIN.get(cat, ('other', 'Other'))
        domain_entries[domain].append(entry)

    # Generate Pure
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// FIX Orchestra — Auto-generated from FIX 5.0 SP2 repository")
    lines.append(f"// Source: {len(messages)} messages, {len(field_lookup)} fields")
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
    seen_classes = set()

    for domain in sorted(domain_entries.keys()):
        entries = domain_entries[domain]
        display = dict((v[0], v[1]) for v in CATEGORY_TO_DOMAIN.values()).get(domain, domain.replace('_', ' ').title())
        wtu = WHEN_TO_USE.get(domain, '')

        lines.append(f"// ─── {display} ({len(entries)} types) ───")
        lines.append("")

        for entry in entries:
            class_name = re.sub(r'[^a-zA-Z0-9]', '', entry['name'])
            if not class_name:
                continue
            qualified = f"{domain}::{class_name}"
            if qualified in seen_classes:
                continue
            seen_classes.add(qualified)
            class_count += 1

            desc = (entry['description'] or entry['name']).replace("'", "\\'").replace('\n', ' ').strip()
            if len(desc) > 150:
                desc = desc[:147] + "..."

            # Get properties from field_ids
            props = []
            for fid in sorted(entry['field_ids']):
                if fid in field_lookup:
                    f = field_lookup[fid]
                    props.append(f)

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
                pname = to_camel(prop['name'])
                if not pname or pname in seen_props:
                    continue
                pname = re.sub(r'[^a-zA-Z0-9]', '', pname)
                if not pname or not pname[0].isalpha():
                    continue
                seen_props.add(pname)

                pdesc = prop['description'].replace("'", "\\'").replace('\n', ' ').strip()
                if len(pdesc) > 120:
                    pdesc = pdesc[:117] + "..."

                ptags = f"nlq::NlqProfile.description = '{pdesc}'"
                nl = pname.lower()
                if any(x in nl for x in ('price', 'amt', 'amount', 'px', 'value', 'fee')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'currency'"
                elif any(x in nl for x in ('rate', 'pct', 'percent', 'yield')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'percent'"
                elif any(x in nl for x in ('qty', 'quantity', 'shares', 'size')):
                    if prop['type'] == 'Float':
                        ptags += ", nlq::NlqProfile.unit = 'units'"

                lines.append(f"    {{{ptags}}}")
                lines.append(f"    {pname}: {prop['type']}[0..1];")
                total_props += 1

            lines.append("}")
            lines.append("")

    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"\nGenerated {OUTPUT_FILE}")
    print(f"  Classes:    {class_count}")
    print(f"  Properties: {total_props}")
    print(f"  Domains:    {len(domain_entries)}")


if __name__ == "__main__":
    generate()
