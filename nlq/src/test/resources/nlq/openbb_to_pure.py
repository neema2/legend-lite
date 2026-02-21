#!/usr/bin/env python3
"""
Convert all OpenBB standard Pydantic data models to a Pure semantic model file.

Reads from: /Users/neema/legend/OpenBB/openbb_platform/core/openbb_core/provider/standard_models/
Outputs to: openbb-model.pure

Generates:
  - NlqProfile stereotype + tags
  - Pure Class per OpenBB *Data model
  - Associations via shared 'symbol' field
  - Domain-based packaging
"""

import os
import re
import sys
from collections import defaultdict

MODELS_DIR = "/Users/neema/legend/OpenBB/openbb_platform/core/openbb_core/provider/standard_models"
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "openbb-model.pure")

# ─── Domain classification by filename pattern ───
DOMAIN_RULES = [
    # (pattern_list, domain)
    (["equity_historical", "equity_quote", "equity_info", "equity_search", "equity_peers",
      "equity_screener", "equity_nbbo", "equity_ftd", "equity_ownership", "equity_short_interest",
      "equity_active", "equity_gainers", "equity_losers", "equity_performance",
      "equity_aggressive_small_caps", "equity_undervalued_growth", "equity_undervalued_large_caps",
      "growth_tech_equities", "market_snapshots", "share_statistics", "historical_market_cap",
      "price_performance", "recent_performance"], "equity"),
    (["balance_sheet", "income_statement", "cash_flow", "financial_ratios", "key_metrics",
      "balance_sheet_growth", "income_statement_growth", "cash_flow_growth",
      "reported_financials", "revenue_business_line", "revenue_geographic",
      "compare_company_facts", "compare_groups", "esg_score",
      "historical_employees", "key_executives", "executive_compensation",
      "management_discussion_analysis", "latest_financial_reports"], "fundamentals"),
    (["analyst_estimates", "analyst_search", "price_target", "price_target_consensus",
      "forward_eps_estimates", "forward_ebitda_estimates", "forward_pe_estimates",
      "forward_sales_estimates", "historical_eps"], "estimates"),
    (["insider_trading", "institutional_ownership", "form_13fhr", "government_trades",
      "institutions_search"], "ownership"),
    (["calendar_dividend", "calendar_earnings", "calendar_events", "calendar_ipo",
      "calendar_splits", "earnings_call_transcript", "historical_dividends",
      "historical_splits", "company_news", "world_news"], "events"),
    (["options_chains", "options_snapshots", "options_unusual"], "derivatives"),
    (["etf_active", "etf_gainers", "etf_losers", "etf_info", "etf_search", "etf_holdings",
      "etf_countries", "etf_sectors", "etf_equity_exposure", "etf_historical",
      "etf_price_performance", "nport_disclosure"], "etf"),
    (["bond_indices", "bond_prices", "bond_reference", "bond_trades",
      "treasury_auctions", "treasury_prices", "treasury_rates",
      "mortgage_indices", "high_quality_market_corporate_bond", "yield_curve"], "fixedincome"),
    (["crypto_historical", "crypto_search"], "crypto"),
    (["currency_historical", "currency_pairs", "currency_reference_rates", "currency_snapshots"], "fx"),
    (["futures_curve", "futures_historical", "futures_info", "futures_instruments",
      "commodity_spot_prices", "commodity_psd_data", "commodity_psd_report"], "commodities"),
    (["index_historical", "index_constituents", "index_search", "index_sectors",
      "index_snapshots", "available_indices"], "indices"),
    (["gdp_nominal", "gdp_real", "gdp_forecast", "consumer_price_index",
      "economic_calendar", "economic_indicators", "available_indicators",
      "ameribor", "sofr", "federal_funds_rate", "iorb", "euro_short_term_rate",
      "european_central_bank_interest_rates", "discount_window_primary_credit_rate",
      "overnight_bank_funding_rate", "country_interest_rates", "country_profile",
      "composite_leading_indicator", "non_farm_payrolls", "money_measures",
      "personal_consumption_expenditures", "balance_of_payments", "direction_of_trade",
      "house_price_index", "retail_prices", "commercial_paper", "primary_dealer_fails",
      "primary_dealer_positioning", "sp500_multiples", "projections",
      "manufacturing_outlook_ny", "manufacturing_outlook_texas",
      "petroleum_status_report", "central_bank_holdings", "risk_premium",
      "export_destinations"], "macro"),
    (["company_filings", "cik_map", "discovery_filings", "rss_litigation",
      "sec_filing", "sec_htm_file", "schema_files", "trade_report"], "regulatory"),
    (["fred_search", "fred_series", "fred_regional", "fred_release_table",
      "bls_search", "bls_series"], "freddata"),
    (["fama_french_breakpoints", "fama_french_factors", "fama_french_country_portfolio_returns",
      "fama_french_international_index_returns", "fama_french_regional_portfolio_returns",
      "fama_french_us_portfolio_returns"], "famafrench"),
    (["cot", "cot_search"], "cot"),
    (["historical_attributes", "latest_attributes", "search_attributes"], "intrinio"),
    (["maritime_choke_point_info", "maritime_choke_point_volume",
      "port_info", "port_volume"], "maritime"),
    (["otc_aggregate"], "otc"),
    (["congress_bills", "congress_bill_info", "fomc_documents"], "government"),
]

# ─── Python type → Pure type mapping ───
def python_type_to_pure(type_str):
    """Convert a Python type annotation to Pure type + multiplicity."""
    type_str = type_str.strip()

    # Handle list types → [*] multiplicity
    list_match = re.match(r'list\[(.+)\]', type_str)
    if list_match:
        inner = list_match.group(1)
        # Remove | None from inner
        inner = re.sub(r'\s*\|\s*None', '', inner).strip()
        pure_type = scalar_to_pure(inner)
        return pure_type, "[*]"

    # Handle optional: T | None
    if "| None" in type_str:
        base = re.sub(r'\s*\|\s*None', '', type_str).strip()
        # Handle union types like float | int
        base = pick_primary_type(base)
        pure_type = scalar_to_pure(base)
        return pure_type, "[0..1]"

    # Required scalar
    base = pick_primary_type(type_str)
    pure_type = scalar_to_pure(base)
    return pure_type, "[1]"


def pick_primary_type(type_str):
    """Pick the primary type from a union like 'float | int | str'."""
    parts = [p.strip() for p in type_str.split('|')]
    parts = [p for p in parts if p and p != 'None']
    if not parts:
        return "str"
    # Prefer: float > int > str > date > datetime > bool
    for prefer in ['float', 'int', 'str', 'date', 'datetime', 'bool']:
        for p in parts:
            if p == prefer:
                return p
    return parts[0]


def scalar_to_pure(py_type):
    """Map a scalar Python type name to Pure type."""
    mapping = {
        'float': 'Float',
        'int': 'Integer',
        'str': 'String',
        'date': 'StrictDate',
        'dateType': 'StrictDate',
        'datetime': 'DateTime',
        'bool': 'Boolean',
        'Decimal': 'Float',
        'NonNegativeInt': 'Integer',
        'PositiveInt': 'Integer',
        'NonNegativeFloat': 'Float',
        'PositiveFloat': 'Float',
    }
    return mapping.get(py_type, 'String')


def classify_domain(filename):
    """Classify a model file into a domain package."""
    base = filename.replace('.py', '')
    for patterns, domain in DOMAIN_RULES:
        if base in patterns:
            return domain
    return "other"


def to_class_name(filename):
    """Convert filename to PascalCase class name."""
    base = filename.replace('.py', '')
    # Split on underscores and capitalize
    return ''.join(word.capitalize() for word in base.split('_'))


def to_property_name(field_name):
    """Convert snake_case field to camelCase property name."""
    parts = field_name.split('_')
    return parts[0] + ''.join(w.capitalize() for w in parts[1:])


def is_metric(field_name, pure_type):
    """Determine if a field is a metric (numeric) or dimension."""
    if pure_type in ('Float', 'Integer'):
        return True
    return False


def parse_model_file(filepath):
    """Parse a Pydantic model file and extract Data class fields."""
    with open(filepath) as f:
        content = f.read()

    # Find all Data classes
    results = []
    # Match: class SomeData(Data): or class SomeData(SomeOtherData):
    class_pattern = re.compile(r'class\s+(\w+Data)\s*\([^)]*\)\s*:', re.MULTILINE)

    for class_match in class_pattern.finditer(content):
        class_name = class_match.group(1)
        class_start = class_match.end()

        # Find the docstring
        docstring = ""
        doc_match = re.search(r'"""(.+?)"""', content[class_start:class_start+200], re.DOTALL)
        if doc_match:
            docstring = doc_match.group(1).strip().split('\n')[0]

        # Find fields until next class or end of file
        next_class = re.search(r'\nclass\s+\w+', content[class_start:])
        class_end = class_start + next_class.start() if next_class else len(content)
        class_body = content[class_start:class_end]

        fields = []
        # Match field definitions: name: type = Field(...)
        field_pattern = re.compile(
            r'^\s+(\w+):\s+(.+?)\s*=\s*Field\(',
            re.MULTILINE
        )

        for fm in field_pattern.finditer(class_body):
            field_name = fm.group(1)
            field_type = fm.group(2).strip()
            field_pos = fm.start()

            # Extract description from the Field() call
            # Find the closing paren of Field(...)
            field_call_start = fm.end() - 1  # position of '('
            remaining = class_body[field_call_start:]

            # Simple extraction: find description="..." or description='...'
            desc = ""
            desc_match = re.search(r'description\s*=\s*["\']([^"\']*)["\']', remaining[:500])
            if not desc_match:
                # Try multi-line or concatenated descriptions
                desc_match = re.search(r'description\s*=\s*(?:DATA_DESCRIPTIONS|QUERY_DESCRIPTIONS)\.get\(\s*["\'](\w+)["\']', remaining[:500])
                if desc_match:
                    key = desc_match.group(1)
                    # Resolve from known descriptions
                    known = {
                        'symbol': 'Symbol representing the entity requested in the data.',
                        'date': 'The date of the data.',
                        'open': 'The open price.',
                        'high': 'The high price.',
                        'low': 'The low price.',
                        'close': 'The close price.',
                        'volume': 'The trading volume.',
                        'adj_close': 'The adjusted close price.',
                        'vwap': 'Volume Weighted Average Price over the period.',
                        'prev_close': 'The previous close price.',
                        'market_cap': 'Market capitalization.',
                        'cik': 'Central Index Key (CIK) for the requested entity.',
                    }
                    desc = known.get(key, key.replace('_', ' ').title())
            else:
                desc = desc_match.group(1)

            # Clean description
            desc = desc.replace("'", "\\'").replace('"', '\\"').strip()
            if len(desc) > 120:
                desc = desc[:117] + "..."

            fields.append({
                'name': field_name,
                'type': field_type,
                'description': desc,
            })

        if fields:
            results.append({
                'class_name': class_name,
                'docstring': docstring,
                'fields': fields,
            })

    return results


def generate_pure_model():
    """Main: parse all models and generate the Pure file."""
    # Collect all models grouped by domain
    domain_models = defaultdict(list)
    all_symbol_classes = []  # classes that have a 'symbol' field

    for fname in sorted(os.listdir(MODELS_DIR)):
        if not fname.endswith('.py') or fname == '__init__.py':
            continue

        filepath = os.path.join(MODELS_DIR, fname)
        domain = classify_domain(fname)
        class_name_base = to_class_name(fname)

        parsed = parse_model_file(filepath)
        if not parsed:
            continue

        # Use the last Data class (most specific)
        data_class = parsed[-1]

        model_info = {
            'file': fname,
            'domain': domain,
            'class_name': class_name_base,
            'docstring': data_class['docstring'],
            'fields': data_class['fields'],
            'has_symbol': any(f['name'] == 'symbol' for f in data_class['fields']),
            'has_date': any(f['name'] == 'date' for f in data_class['fields']),
        }

        domain_models[domain].append(model_info)
        if model_info['has_symbol']:
            all_symbol_classes.append(model_info)

    # ─── Generate Pure output ───
    lines = []

    # Header
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// OpenBB Financial Data Model — Auto-generated from OpenBB Platform")
    lines.append(f"// Source: openbb_core/provider/standard_models/ ({sum(len(v) for v in domain_models.values())} models)")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")

    # NlqProfile definition
    lines.append("Profile nlq::NlqProfile")
    lines.append("{")
    lines.append("    stereotypes: [dimension, metric];")
    lines.append("    tags: [description, synonyms, sampleValues, unit, importance, businessDomain, exampleQuestions, whenToUse];")
    lines.append("}")
    lines.append("")

    # Central Security hub class for cross-domain joins
    lines.append("// ─── Central Reference Data ───")
    lines.append("")
    lines.append("Class <<nlq::NlqProfile.dimension>>")
    lines.append("  {nlq::NlqProfile.description = 'A financial security or instrument identified by ticker symbol',")
    lines.append("   nlq::NlqProfile.synonyms = 'ticker, stock, instrument, asset',")
    lines.append("   nlq::NlqProfile.businessDomain = 'Reference Data',")
    lines.append("   nlq::NlqProfile.importance = 'high',")
    lines.append("   nlq::NlqProfile.whenToUse = 'Use when the question is about security master data or looking up symbols'}")
    lines.append("refdata::Security")
    lines.append("{")
    lines.append("    {nlq::NlqProfile.description = 'Ticker symbol'}")
    lines.append("    symbol: String[1];")
    lines.append("    {nlq::NlqProfile.description = 'Security name'}")
    lines.append("    name: String[0..1];")
    lines.append("    {nlq::NlqProfile.description = 'Asset type such as stock, ETF, bond, crypto', nlq::NlqProfile.sampleValues = 'STOCK, ETF, BOND, CRYPTO, INDEX, FUND'}")
    lines.append("    assetType: String[0..1];")
    lines.append("    {nlq::NlqProfile.description = 'Primary exchange where the security trades'}")
    lines.append("    exchange: String[0..1];")
    lines.append("    {nlq::NlqProfile.description = 'ISO currency code', nlq::NlqProfile.sampleValues = 'USD, EUR, GBP, JPY'}")
    lines.append("    currency: String[0..1];")
    lines.append("}")
    lines.append("")

    total_props = 0
    total_classes = 1  # counting Security

    # Domain pretty names for NlqProfile.businessDomain
    domain_display = {
        'equity': 'Equity Markets',
        'fundamentals': 'Fundamental Analysis',
        'estimates': 'Analyst Estimates',
        'ownership': 'Ownership & Insider Trading',
        'events': 'Corporate Events & News',
        'derivatives': 'Derivatives & Options',
        'etf': 'Exchange Traded Funds',
        'fixedincome': 'Fixed Income & Bonds',
        'crypto': 'Cryptocurrency',
        'fx': 'Foreign Exchange',
        'commodities': 'Commodities & Futures',
        'indices': 'Market Indices',
        'macro': 'Macroeconomics',
        'regulatory': 'Regulatory & Filings',
        'freddata': 'FRED Economic Data',
        'famafrench': 'Fama-French Factors',
        'cot': 'Commitment of Traders',
        'intrinio': 'Data Attributes',
        'maritime': 'Maritime & Shipping',
        'otc': 'Over The Counter',
        'government': 'Government & Congress',
        'other': 'Other',
    }

    # whenToUse hints for key domains
    when_to_use = {
        'equity': 'Use for stock prices, quotes, market movers, and equity screening',
        'fundamentals': 'Use for balance sheets, income statements, cash flows, financial ratios, and company metrics',
        'estimates': 'Use for analyst estimates, price targets, forward earnings, and revenue forecasts',
        'ownership': 'Use for insider trading, institutional ownership, and 13F filings',
        'events': 'Use for earnings calendars, dividends, IPOs, stock splits, and company news',
        'derivatives': 'Use for options chains, options snapshots, and unusual options activity',
        'etf': 'Use for ETF holdings, sectors, performance, and ETF screening',
        'fixedincome': 'Use for bond prices, treasury rates, yield curves, and mortgage indices',
        'crypto': 'Use for cryptocurrency prices and crypto pair searching',
        'fx': 'Use for currency exchange rates, FX pairs, and reference rates',
        'commodities': 'Use for commodity prices, futures curves, and agricultural data',
        'indices': 'Use for market index levels, constituents, and index performance',
        'macro': 'Use for GDP, CPI, interest rates, employment, and economic indicators',
        'regulatory': 'Use for SEC filings, company filings, and regulatory documents',
        'freddata': 'Use for FRED economic time series and BLS labor statistics',
    }

    # Generate classes by domain
    for domain in sorted(domain_models.keys()):
        models = domain_models[domain]
        display = domain_display.get(domain, domain.title())
        lines.append(f"// ─── {display} ({len(models)} models) ───")
        lines.append("")

        for model in models:
            total_classes += 1
            qualified_name = f"{domain}::{model['class_name']}"

            # Determine stereotype: metric if most fields are numeric
            numeric_count = sum(1 for f in model['fields']
                                if python_type_to_pure(f['type'])[0] in ('Float', 'Integer'))
            stereotype = "metric" if numeric_count > len(model['fields']) / 2 else "dimension"

            # Class-level tags
            desc = model['docstring'] or model['class_name']
            desc = desc.replace("'", "\\'")
            importance = "high" if model['has_symbol'] else "medium"
            wtu = when_to_use.get(domain, "")

            lines.append(f"Class <<nlq::NlqProfile.{stereotype}>>")
            tag_line = f"  {{nlq::NlqProfile.description = '{desc}'"
            tag_line += f",\n   nlq::NlqProfile.businessDomain = '{display}'"
            tag_line += f",\n   nlq::NlqProfile.importance = '{importance}'"
            if wtu:
                tag_line += f",\n   nlq::NlqProfile.whenToUse = '{wtu}'"
            tag_line += "}"
            lines.append(tag_line)
            lines.append(qualified_name)
            lines.append("{")

            seen_names = set()
            for field in model['fields']:
                prop_name = to_property_name(field['name'])
                if prop_name in seen_names:
                    continue
                seen_names.add(prop_name)

                pure_type, multiplicity = python_type_to_pure(field['type'])
                desc = field['description']
                if not desc:
                    desc = field['name'].replace('_', ' ').title()

                # Escape single quotes in description
                desc = desc.replace("'", "\\'")

                # Property-level tags
                prop_tags = f"nlq::NlqProfile.description = '{desc}'"

                # Add unit hints for known patterns
                if 'percent' in field['name'].lower() or 'pct' in field['name'].lower():
                    prop_tags += ", nlq::NlqProfile.unit = 'percent'"
                elif 'price' in field['name'].lower() or field['name'] in ('open', 'high', 'low', 'close', 'bid', 'ask', 'vwap', 'mark'):
                    prop_tags += ", nlq::NlqProfile.unit = 'currency'"
                elif 'volume' in field['name'].lower():
                    prop_tags += ", nlq::NlqProfile.unit = 'shares'"
                elif 'ratio' in field['name'].lower():
                    prop_tags += ", nlq::NlqProfile.unit = 'ratio'"

                lines.append(f"    {{{prop_tags}}}")
                lines.append(f"    {prop_name}: {pure_type}{multiplicity};")
                total_props += 1

            lines.append("}")
            lines.append("")

    # ─── Associations ───
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// Associations — linking models via shared 'symbol' key")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")

    assoc_count = 0

    # 1. Link every symbol-bearing class to refdata::Security
    for model in all_symbol_classes:
        qualified = f"{model['domain']}::{model['class_name']}"
        assoc_name = f"Security_{model['class_name']}"
        # Determine multiplicity: security has many of each data type
        prop_name = model['class_name'][0].lower() + model['class_name'][1:]

        lines.append(f"Association refdata::{assoc_name}")
        lines.append("{")
        lines.append(f"    security: refdata::Security[1];")
        lines.append(f"    {prop_name}: {qualified}[*];")
        lines.append("}")
        lines.append("")
        assoc_count += 1

    # 2. Intra-domain associations for closely related models
    # E.g., EquityHistorical ↔ EquityQuote in equity domain
    intra_pairs = [
        ("equity", "EquityHistorical", "EquityQuote", "historicalPrices", "latestQuote"),
        ("equity", "EquityInfo", "EquityHistorical", "companyInfo", "priceHistory"),
        ("fundamentals", "BalanceSheet", "IncomeStatement", "balanceSheet", "incomeStatement"),
        ("fundamentals", "IncomeStatement", "CashFlow", "incomeStatement", "cashFlow"),
        ("fundamentals", "KeyMetrics", "FinancialRatios", "keyMetrics", "financialRatios"),
        ("events", "CalendarEarnings", "EarningsCallTranscript", "earningsCalendar", "transcripts"),
        ("events", "CalendarDividend", "HistoricalDividends", "dividendCalendar", "dividendHistory"),
        ("derivatives", "OptionsChains", "OptionsSnapshots", "optionsChain", "optionsSnapshot"),
        ("fixedincome", "TreasuryRates", "YieldCurve", "treasuryRates", "yieldCurve"),
        ("etf", "EtfInfo", "EtfHoldings", "etfInfo", "holdings"),
        ("etf", "EtfInfo", "EtfSectors", "etfInfo", "sectorWeights"),
    ]

    for domain, class_a, class_b, prop_a, prop_b in intra_pairs:
        # Verify both classes exist
        domain_class_names = [m['class_name'] for m in domain_models.get(domain, [])]
        if class_a in domain_class_names and class_b in domain_class_names:
            lines.append(f"Association {domain}::{class_a}_{class_b}")
            lines.append("{")
            lines.append(f"    {prop_a}: {domain}::{class_a}[*];")
            lines.append(f"    {prop_b}: {domain}::{class_b}[*];")
            lines.append("}")
            lines.append("")
            assoc_count += 1

    # Write output
    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Classes:      {total_classes}")
    print(f"  Properties:   {total_props}")
    print(f"  Associations: {assoc_count}")
    print(f"  Domains:      {len(domain_models)}")


if __name__ == "__main__":
    generate_pure_model()
