#!/usr/bin/env python3
"""Generate the study's synthetic credit agreements: projects/unstructured/corpus/loans/.

    python tools/synth-corpus/gen_loan_docs.py

Needs reportlab at exactly REPORTLAB_VERSION. Seeded and deterministic: a run replaces agreements/ and ground_truth/ with
the same bytes. Every enum value is read from or checked against projects/unstructured/model.pure.

Everything here is fictional: the borrowers, the agents, the agreements. They are shaped like a real syndicated credit
agreement, and are not legal drafting anyone should copy.

agreements/ holds each agreement as a PDF and as a .txt of the same words. In the .txt every clause is one
blank-line-delimited block, the unit a passage is built from. ground_truth/ holds one CSV per store::Lending table, named
for it; spans.csv says, for each Covenant and EventOfDefault, exactly which clause states it.
"""
from __future__ import annotations

import csv
import hashlib
import random
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Optional
from xml.sax.saxutils import escape

import reportlab
from reportlab import rl_config
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "unstructured-corpus"))
from corpus_schema import LOANS_ROOT, Model, block_spans, load_model  # noqa: E402

SEED = 20261109

# A PDF's bytes depend on reportlab's version, and the committed PDFs are the proof a re-run reproduces: this is the
# version they were made with, the newest that still runs on Python 3.9.
REPORTLAB_VERSION = "4.2.5"

MM = Decimal(1_000_000)
AGENTS = ["Merrowfield Capital Funding LLC", "Pellbrook Bank, N.A.", "Stannard Harbour Bank, N.A."]
GOVERNING_LAW = "New York"
ROMAN = ["", "I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]


@dataclass(frozen=True)
class Borrower:
    id: str
    name: str
    sector: str
    jurisdiction: str
    rating: str
    assets: str


BORROWERS = [
    Borrower("B-01", "Hartsmere Packaging Holdings, Inc.", "Packaging", "Delaware", "BB-", "converting plants and tooling"),
    Borrower("B-02", "Vennick Regional Health Partners, LLC", "Healthcare Services", "Delaware", "B+", "outpatient facilities and medical equipment"),
    Borrower("B-03", "Ardlow Software Group, Inc.", "Software", "Delaware", "B", "source code, licences and data centre equipment"),
    Borrower("B-04", "Cobbleford Building Products Corp.", "Building Products", "Ohio", "BB", "manufacturing plants and finished goods"),
    Borrower("B-05", "Trevanion Specialty Chemicals, Inc.", "Chemicals", "Texas", "BB-", "reactors, storage terminals and feedstock"),
    Borrower("B-06", "Lissadell Foodservice Distribution, LLC", "Food Distribution", "Georgia", "B+", "distribution centres, fleet vehicles and inventory"),
]


def usd(amount: Decimal) -> str:
    return f"${amount:,.0f}"


def long_date(value: date) -> str:
    return f"{value.strftime('%B')} {value.day}, {value.year}"


def quarter_ends(start: date, count: int) -> list[date]:
    ends, year, month = [], start.year, start.month
    for _ in range(count):
        day = {3: 31, 6: 30, 9: 30, 12: 31}[month]
        ends.append(date(year, month, day))
        month += 3
        if month > 12:
            month, year = 3, year + 1
    return ends


def ratio(value: Decimal) -> str:
    return f"{value:.2f}:1.00"


@dataclass
class Block:
    """One blank-line-delimited block of the .txt: one heading, definition or clause."""
    kind: str  # title | article | section | clause | schedule
    text: str
    table: Optional[list[tuple[str, str]]] = None  # the schedule's rows, which the .txt writes as lines of this block


@dataclass
class Agreement:
    id: str
    borrower: Borrower
    title: str
    signed: date
    agent: str
    blocks: list[Block] = field(default_factory=list)
    facilities: list[dict] = field(default_factory=list)
    collateral: list[dict] = field(default_factory=list)
    covenants: list[dict] = field(default_factory=list)
    steps: list[dict] = field(default_factory=list)
    defaults: list[dict] = field(default_factory=list)

    def add(self, kind: str, text: str, table: Optional[list[tuple[str, str]]] = None) -> int:
        if "\n\n" in text:
            raise SystemExit(f"{self.id}: a block holds a blank line: {text[:60]!r}")
        self.blocks.append(Block(kind, text, table))
        return len(self.blocks) - 1

    def section(self, number: str, name: str, body: str = "") -> None:
        self.add("section", f"Section {number} {name}." + (f" {body}" if body else ""))

    def article(self, number: int, name: str) -> None:
        self.add("article", f"ARTICLE {ROMAN[number]}\n{name}")

    def reserved(self, number: str) -> None:
        self.add("section", f"Section {number} [Reserved].")

    def text(self) -> str:
        return "\n\n".join(block.text for block in self.blocks) + "\n"


# ---- the agreement's parts

def facilities_of(a: Agreement, rng: random.Random, model: Model) -> None:
    kinds = model.enum_values("LN_FACILITY", "KIND")
    term_kinds = [k for k in kinds if k in ("TermLoanA", "TermLoanB", "DelayedDrawTerm")]
    count = rng.randint(1, 3)
    chosen = ["Revolver"] if count == 1 else ["Revolver", rng.choice(term_kinds)]
    if count == 3:
        chosen.append(rng.choice([k for k in term_kinds if k not in chosen]))
    if rng.random() < 0.25 and "Revolver" in chosen:
        chosen[0] = "ABL"
    tenor = {"Revolver": 5, "ABL": 5, "TermLoanA": 5, "TermLoanB": 7, "DelayedDrawTerm": 6}
    for ordinal, kind in enumerate(chosen, 1):
        model.enum("LN_FACILITY", "KIND", kind)
        commitment = Decimal(rng.randrange(75, 650, 25)) * MM
        maturity = date(a.signed.year + tenor[kind], a.signed.month, 28)
        revolving = kind in ("Revolver", "ABL")
        a.facilities.append({
            "FACILITY_ID": f"{a.id}-F{ordinal}", "AGREEMENT_ID": a.id, "KIND": kind, "COMMITMENT_USD": f"{commitment:.2f}",
            "CURRENCY": "USD", "MATURITY_DATE": maturity.isoformat(),
            "BENCHMARK": model.enum("LN_FACILITY", "BENCHMARK", "SOFR"),
            "MARGIN_BPS": rng.randrange(175, 450, 25),
            "FLOOR_BPS": None if revolving else rng.choice([0, 50, 75]),
            "COMMITMENT_FEE_BPS": rng.choice([25, 375, 50]) if revolving or kind == "DelayedDrawTerm" else None,
            "AMORTIZATION_PCT_PER_YEAR": None if revolving else {"TermLoanA": "5.0000", "TermLoanB": "1.0000", "DelayedDrawTerm": "2.5000"}[kind],
            "LIEN_PRIORITY": model.enum("LN_FACILITY", "LIEN_PRIORITY", "FirstLien"),
        })


FACILITY_NAME = {"Revolver": "Revolving Credit Facility", "ABL": "Asset-Based Revolving Facility", "TermLoanA": "Term A Facility",
                 "TermLoanB": "Term B Facility", "DelayedDrawTerm": "Delayed Draw Term Facility"}


def letter_of(index: int) -> str:
    return "abcdefghijklmnopqrstuvwxyz"[index]


def cover(a: Agreement) -> None:
    a.add("title", f"{a.title.upper()}\ndated as of {long_date(a.signed)}\namong\n{a.borrower.name},\nas the Borrower,\n"
                   f"THE LENDERS PARTY HERETO\nand\n{a.agent},\nas Administrative Agent")
    a.add("clause", f"This {a.title} is entered into as of {long_date(a.signed)}, among {a.borrower.name}, a {a.borrower.jurisdiction} "
                    f"entity (the “Borrower”), each lender from time to time party hereto (collectively, the “Lenders”), and "
                    f"{a.agent}, as administrative agent and collateral agent (in such capacities, the “Administrative Agent”).")


def definitions(a: Agreement, threshold: Decimal) -> None:
    a.article(1, "DEFINITIONS")
    a.section("1.01", "Defined Terms", "As used in this Agreement, the following terms have the meanings set forth below:")
    for term, meaning in [
        ("Business Day", "means any day other than a Saturday, Sunday or other day on which commercial banks in New York City are authorized or required by law to close."),
        ("Consolidated EBITDA", "means, for any period, Consolidated Net Income for such period plus, without duplication and to the extent deducted in determining such Consolidated Net Income, interest expense, income taxes, depreciation and amortization, and non-recurring charges not exceeding fifteen percent of Consolidated EBITDA for such period."),
        ("Consolidated Interest Charges", "means, for any period, all cash interest, premium payments and the interest component of Capitalized Leases of the Borrower and its Subsidiaries for such period."),
        ("Consolidated Total Net Debt", "means, as of any date, the aggregate principal amount of Indebtedness of the Borrower and its Subsidiaries on a consolidated basis, minus unrestricted cash and Cash Equivalents not exceeding $75,000,000."),
        ("Consolidated Total Net Leverage Ratio", "means, as of any date, the ratio of (a) Consolidated Total Net Debt as of such date to (b) Consolidated EBITDA for the four consecutive Fiscal Quarters most recently ended."),
        ("Consolidated Senior Secured Net Leverage Ratio", "means, as of any date, the ratio of (a) Consolidated Total Net Debt secured by a Lien on any assets of the Borrower or its Subsidiaries to (b) Consolidated EBITDA for the four consecutive Fiscal Quarters most recently ended."),
        ("Fiscal Quarter", "means each fiscal quarter of the Borrower, ending on the last day of March, June, September and December."),
        ("Liquidity", "means, as of any date, the sum of unrestricted cash and Cash Equivalents of the Borrower and its Subsidiaries and the unused Revolving Commitments then available to be drawn."),
        ("SOFR", "means the secured overnight financing rate as administered by the relevant administrator, and “Term SOFR” means the forward-looking term rate based on SOFR for the applicable interest period."),
        ("Threshold Amount", f"means {usd(threshold)}."),
    ]:
        a.add("clause", f"“{term}” {meaning}")


def credits(a: Agreement) -> None:
    a.article(2, "THE CREDITS")
    a.section("2.01", "Commitments", "Subject to the terms and conditions set forth herein:")
    for index, f in enumerate(a.facilities):
        name = FACILITY_NAME[f["KIND"]]
        matures = long_date(date.fromisoformat(f["MATURITY_DATE"]))
        a.add("clause", f"({letter_of(index)}) {name}. Each Lender severally agrees to make loans to the Borrower under the {name} in an "
                        f"aggregate principal amount not exceeding its commitment, the aggregate of all such commitments being {usd(Decimal(f['COMMITMENT_USD']))} "
                        f"on the date hereof. All amounts outstanding under the {name} are due on {matures}.")
    a.section("2.08", "Interest and Fees", "Interest on the Loans is payable in arrears as follows:")
    for index, f in enumerate(a.facilities):
        name = FACILITY_NAME[f["KIND"]]
        margin = Decimal(f["MARGIN_BPS"]) / 100
        floor = "" if f["FLOOR_BPS"] is None else f", subject to a Term SOFR floor of {Decimal(f['FLOOR_BPS']) / 100:.2f}%"
        fee = "" if f["COMMITMENT_FEE_BPS"] is None else f" The Borrower shall pay a commitment fee of {Decimal(f['COMMITMENT_FEE_BPS']) / 100:.2f}% per annum on the unused portion of the {name}."
        amortization = "" if f["AMORTIZATION_PCT_PER_YEAR"] is None else f" The {name} amortizes in equal quarterly instalments of {Decimal(f['AMORTIZATION_PCT_PER_YEAR']) / 4:.3f}% of its original principal amount."
        a.add("clause", f"({letter_of(index)}) {name}. Loans under the {name} bear interest at Term SOFR plus {margin:.2f}% per annum{floor}.{fee}{amortization}")


def boilerplate_front(a: Agreement) -> None:
    a.article(3, "YIELD PROTECTION AND TAXES")
    a.section("3.01", "Taxes", "All payments by or on account of any obligation of the Borrower hereunder shall be made free and clear of Taxes, except as required by applicable law.")
    a.article(4, "CONDITIONS PRECEDENT")
    a.section("4.01", "Conditions to Effectiveness", "The obligation of each Lender to make its initial Loan is subject to receipt by the Administrative Agent of executed counterparts of this Agreement, customary opinions of counsel and evidence of the Collateral having been perfected.")
    a.article(5, "REPRESENTATIONS AND WARRANTIES")
    a.section("5.01", "Organization", f"The Borrower is duly organized and validly existing under the laws of {a.borrower.jurisdiction} and has all requisite power to carry on its business.")
    a.section("5.02", "No Default", "No Default has occurred and is continuing, and none would result from the consummation of the transactions contemplated hereby.")


def collateral_of(a: Agreement, rng: random.Random, model: Model) -> None:
    kinds = [k for k in model.enum_values("LN_COLLATERAL", "KIND") if k != "AllAssets"]
    wording = {
        "AllAssets": f"substantially all assets of the Borrower and each Guarantor, including {a.borrower.assets}",
        "Receivables": "all accounts receivable of the Borrower and each Guarantor and the proceeds thereof",
        "Inventory": "all inventory of the Borrower and each Guarantor, wherever located",
        "RealEstate": "each owned real property with a fair market value above $10,000,000, by first-priority mortgage",
        "Equipment": f"all machinery and equipment of the Borrower and each Guarantor, including {a.borrower.assets}",
        "EquityPledge": "all equity interests in each direct Subsidiary of the Borrower, limited to 65% of the voting equity of a foreign Subsidiary",
    }
    ordinal = 0
    for facility in a.facilities:
        kind_list = ["AllAssets"] if rng.random() < 0.5 else rng.sample(kinds, rng.randint(1, 2))
        for kind in kind_list:
            ordinal += 1
            a.collateral.append({"COLLATERAL_ID": f"{a.id}-K{ordinal}", "FACILITY_ID": facility["FACILITY_ID"],
                                 "KIND": model.enum("LN_COLLATERAL", "KIND", kind), "DESCRIPTION": wording[kind]})


LEVERAGE = {"MaxTotalLeverage": ("Maximum Total Net Leverage Ratio", "Consolidated Total Net Leverage Ratio"),
            "MaxSeniorSecuredLeverage": ("Maximum Senior Secured Net Leverage Ratio", "Consolidated Senior Secured Net Leverage Ratio")}
COVERAGE = {"MinInterestCoverage": ("Minimum Interest Coverage Ratio", "the ratio of Consolidated EBITDA to Consolidated Interest Charges"),
            "MinFixedChargeCoverage": ("Minimum Fixed Charge Coverage Ratio", "the ratio of Consolidated EBITDA less Capital Expenditures to Consolidated Fixed Charges")}


def covenant_row(a: Agreement, model: Model, ordinal: int, family: str, kind: str, operator: Optional[str], threshold: Optional[Decimal],
                 unit: Optional[str], frequency: str, first: Optional[date], cure: bool, ref: str, block: int) -> dict:
    enum = model.enum
    row = {
        "block": block, "COVENANT_ID": f"{a.id}-C{ordinal}", "AGREEMENT_ID": a.id, "FAMILY": enum("LN_COVENANT", "FAMILY", family),
        "KIND": enum("LN_COVENANT", "KIND", kind), "COVENANT_OPERATOR": None if operator is None else enum("LN_COVENANT", "COVENANT_OPERATOR", operator),
        "THRESHOLD": None if threshold is None else f"{threshold:.4f}", "THRESHOLD_UNIT": None if unit is None else enum("LN_COVENANT", "THRESHOLD_UNIT", unit),
        "TEST_FREQUENCY": enum("LN_COVENANT", "TEST_FREQUENCY", frequency), "FIRST_TEST_DATE": None if first is None else first.isoformat(),
        "EQUITY_CURE_ALLOWED": "true" if cure else "false", "CLAUSE_REF": ref}
    a.covenants.append(row)
    return row


def schedule_lines(rows: list[tuple[str, str]]) -> str:
    return "\n".join(f"{period}: {level}" for period, level in rows)


def financial_covenants(a: Agreement, rng: random.Random, model: Model, ordinal: int, optional: Optional[str]) -> int:
    a.section("7.11", "Financial Covenants")
    first = quarter_ends(date(a.signed.year, ((a.signed.month - 1) // 3 + 1) * 3, 28), 2)[1]
    # (a) the leverage covenant: the step-down, subject to the equity cure
    leverage_kind = rng.choice(sorted(LEVERAGE))
    title, measure = LEVERAGE[leverage_kind]
    start = Decimal(rng.randrange(400, 625, 25)) / 100
    levels = rng.randint(3, 5)
    drop = Decimal(rng.choice([25, 50])) / 100
    ends = quarter_ends(first, levels * 4)
    effective = [ends[i * 2] for i in range(levels)]  # a level holds for two Fiscal Quarters
    thresholds = [start - drop * i for i in range(levels)]
    table = []
    for i, level in enumerate(thresholds):
        period = (f"Fiscal Quarter ending {long_date(effective[i])} through the Fiscal Quarter ending "
                  f"{long_date(ends[(i + 1) * 2 - 1])}") if i + 1 < levels else f"Fiscal Quarter ending {long_date(effective[i])} and each Fiscal Quarter thereafter"
        table.append((period, ratio(level)))
    text = (f"(a) {title}. Subject to Section 7.11(d), the Borrower shall not permit the {measure} as of the last day of any Fiscal "
            f"Quarter, commencing with the Fiscal Quarter ending {long_date(first)}, to exceed the ratio set forth below opposite the "
            f"period in which such Fiscal Quarter ends:")
    block = a.add("schedule", text + "\n" + schedule_lines(table), table)
    row = covenant_row(a, model, ordinal, "Financial", leverage_kind, "AtMost", start, "Multiple", "Quarterly", first, True, "Section 7.11(a)", block)
    for i, level in enumerate(thresholds):
        a.steps.append({"COVENANT_ID": row["COVENANT_ID"], "EFFECTIVE_FROM": effective[i].isoformat(), "THRESHOLD": f"{level:.4f}"})
    ordinal += 1
    # (b) the coverage covenant
    coverage_kind = rng.choice(sorted(COVERAGE))
    title, measure = COVERAGE[coverage_kind]
    floor = Decimal(rng.randrange(175, 325, 25)) / 100
    cure = rng.random() < 0.5
    subject = "Subject to Section 7.11(d), the" if cure else "The"
    block = a.add("clause", f"(b) {title}. {subject} Borrower shall not permit {measure} for any period of four consecutive Fiscal Quarters, "
                            f"commencing with the period ending {long_date(first)}, to be less than {ratio(floor)}.")
    covenant_row(a, model, ordinal, "Financial", coverage_kind, "AtLeast", floor, "Multiple", "Quarterly", first, cure, "Section 7.11(b)", block)
    ordinal += 1
    # (c) a third test, in some agreements
    if optional == "MinLiquidity":
        minimum = Decimal(rng.randrange(25, 105, 5)) * MM
        block = a.add("clause", f"(c) Minimum Liquidity. The Borrower shall not permit Liquidity at any time to be less than {usd(minimum)}.")
        covenant_row(a, model, ordinal, "Financial", "MinLiquidity", "AtLeast", minimum, "Usd", "Continuous", None, False, "Section 7.11(c)", block)
        ordinal += 1
    elif optional == "MaxCapex":
        maximum = Decimal(rng.randrange(40, 160, 5)) * MM
        block = a.add("clause", f"(c) Maximum Capital Expenditures. The Borrower shall not make, or permit its Subsidiaries to make, Capital "
                                f"Expenditures in any fiscal year exceeding {usd(maximum)} in the aggregate, with any unused amount carried forward to the next fiscal year only.")
        covenant_row(a, model, ordinal, "Financial", "MaxCapex", "AtMost", maximum, "Usd", "Annual", first, False, "Section 7.11(c)", block)
        ordinal += 1
    else:
        a.add("clause", "(c) [Reserved].")
    # (d) the equity cure, a right and not a test: no row of its own, the covenants above carry the flag
    days, in_four, lifetime = rng.choice([10, 15]), rng.choice(["two", "three"]), rng.choice(["four", "five"])
    a.add("clause", f"(d) Equity Cure. In the event the Borrower fails to comply with Section 7.11(a){' or Section 7.11(b)' if cure else ''} as of the last day of any "
                    f"Fiscal Quarter, the Borrower shall have the right, until the {'tenth' if days == 10 else 'fifteenth'} Business Day after the date on which financial statements for such Fiscal "
                    f"Quarter are required to be delivered, to receive cash common equity contributions (the “Cure Right”), and the amount so received shall be "
                    f"deemed to increase Consolidated EBITDA for such Fiscal Quarter solely for the purpose of determining compliance with such covenant. The "
                    f"Cure Right may not be exercised in more than {in_four} of any four consecutive Fiscal Quarters or more than {lifetime} times during the term of this Agreement.")
    return ordinal


BASKETS = [
    ("7.01", "Indebtedness", "(a)", "Indebtedness",
     "The Borrower shall not, and shall not permit any Subsidiary to, create, incur or assume any Indebtedness other than Indebtedness under this Agreement and other Indebtedness in an aggregate principal amount at any time outstanding not exceeding {amount} (the “General Debt Basket”)."),
    ("7.02", "Liens", "(a)", "Liens",
     "The Borrower shall not, and shall not permit any Subsidiary to, create or permit to exist any Lien on any of its property, other than Liens securing the Obligations and other Liens securing obligations not exceeding {amount} at any time outstanding (the “General Lien Basket”)."),
    ("7.05", "Asset Sales", "(a)", "AssetSales",
     "The Borrower shall not, and shall not permit any Subsidiary to, make any Disposition other than Dispositions for fair market value, at least 75% in cash, in an aggregate amount in any fiscal year not exceeding {amount}."),
    ("7.06", "Restricted Payments", "(a)", "RestrictedPayments",
     "The Borrower shall not declare or make any Restricted Payment other than Restricted Payments in an aggregate amount during the term of this Agreement not exceeding {amount}, so long as no Default has occurred and is continuing."),
]


def negative_covenants(a: Agreement, rng: random.Random, model: Model, ordinal: int, chosen: list[str], optional_financial: Optional[str]) -> int:
    a.article(7, "NEGATIVE COVENANTS")
    numbers = {b[0]: b for b in BASKETS}
    for number in ("7.01", "7.02", "7.03", "7.04", "7.05", "7.06", "7.07", "7.08", "7.09", "7.10"):
        basket = numbers.get(number)
        if basket is None or basket[3] not in chosen:
            a.reserved(number)
            continue
        _, name, sub, kind, wording = basket
        a.section(number, name)
        amount = Decimal(rng.randrange(25, 205, 5)) * MM
        ref = f"Section {number}{sub}"
        block = a.add("clause", f"{sub} {name} Basket. " + wording.format(amount=usd(amount)))
        covenant_row(a, model, ordinal, "Negative", kind, "AtMost", amount, "Usd", "Continuous", None, False, ref, block)
        ordinal += 1
    return financial_covenants(a, rng, model, ordinal, optional_financial)


def affirmative(a: Agreement, rng: random.Random, model: Model, ordinal: int, reporting: bool) -> int:
    a.article(6, "AFFIRMATIVE COVENANTS")
    a.section("6.01", "Financial Statements", "The Borrower shall deliver to the Administrative Agent:")
    if reporting:
        days = rng.choice([45, 60])
        block = a.add("clause", f"(a) within {days} days after the end of each of the first three Fiscal Quarters of each fiscal year, consolidated balance sheets and related statements of "
                                f"income and cash flows, certified by a Financial Officer as fairly presenting the financial condition of the Borrower and its Subsidiaries, together with a compliance certificate setting forth the calculations under Section 7.11.")
        covenant_row(a, model, ordinal, "Reporting", "FinancialReporting", None, None, None, "Quarterly", None, False, "Section 6.01(a)", block)
        ordinal += 1
    else:
        a.add("clause", "(a) [Reserved].")
    a.section("6.09", "Collateral and Guarantees", "The Obligations shall be secured by first-priority Liens on the following, in favour of the Administrative Agent for the benefit of the Lenders:")
    for index, row in enumerate(a.collateral):
        facility = next(f for f in a.facilities if f["FACILITY_ID"] == row["FACILITY_ID"])
        a.add("clause", f"({letter_of(index)}) with respect to the {FACILITY_NAME[facility['KIND']]}, {row['DESCRIPTION']}.")
    return ordinal


def events_of_default(a: Agreement, rng: random.Random, model: Model, threshold: Decimal) -> None:
    a.article(8, "EVENTS OF DEFAULT")
    a.section("8.01", "Events of Default", "Any of the following constitutes an Event of Default:")
    enum = model.enum
    grace = rng.choice([3, 5])
    word = {3: "three", 5: "five"}[grace]
    extra = rng.choice(["Insolvency", "ChangeOfControl", "FinancialCovenantBreach", None])
    rows = [("PaymentDefault", grace, None, f"(a) Non-Payment. The Borrower fails to pay (i) any principal of any Loan when due, or (ii) any interest on any Loan or any fee within {word} Business Days after the same becomes due."),
            ("CrossDefault", None, threshold, f"(b) Cross-Default. The Borrower or any Subsidiary fails to make any payment when due, beyond any applicable grace period, in respect of Indebtedness having an aggregate principal amount of more than {usd(threshold)} (the Threshold Amount), or any such Indebtedness becomes due before its stated maturity as a result of a default thereunder.")]
    if extra == "Insolvency":
        rows.append(("Insolvency", 60, None, "(c) Insolvency Proceedings. The Borrower or any Subsidiary institutes any proceeding under any Debtor Relief Law, or any such proceeding is instituted against it and remains undismissed for 60 calendar days."))
    elif extra == "ChangeOfControl":
        rows.append(("ChangeOfControl", None, None, "(c) Change of Control. There occurs any Change of Control, being any person or group other than the Permitted Holders acquiring more than 35% of the voting equity of the Borrower."))
    elif extra == "FinancialCovenantBreach":
        rows.append(("FinancialCovenantBreach", None, None, "(c) Financial Covenants. The Borrower fails to perform or observe any covenant in Section 7.11, subject to the Cure Right in Section 7.11(d)."))
    for kind, days, amount, text in rows:
        ref = f"Section 8.01{text[:3]}"
        block = a.add("clause", text)
        a.defaults.append({"block": block, "AGREEMENT_ID": a.id, "CLAUSE_REF": ref, "KIND": enum("LN_EVENT_OF_DEFAULT", "KIND", kind),
                           "GRACE_PERIOD_DAYS": days, "CROSS_DEFAULT_THRESHOLD_USD": None if amount is None else f"{amount:.2f}"})
    a.section("8.02", "Remedies", "If any Event of Default occurs and is continuing, the Administrative Agent may, and at the request of the Required Lenders shall, declare the commitments terminated and all Obligations immediately due and payable.")


def boilerplate_back(a: Agreement) -> None:
    a.article(9, "THE ADMINISTRATIVE AGENT")
    a.section("9.01", "Appointment", f"Each Lender irrevocably appoints {a.agent} as Administrative Agent and authorizes it to act on such Lender's behalf.")
    a.article(10, "MISCELLANEOUS")
    a.section("10.01", "Amendments", "No amendment of any provision of this Agreement shall be effective unless in writing and signed by the Required Lenders and the Borrower.")
    a.section("10.14", "Governing Law", f"THIS AGREEMENT SHALL BE GOVERNED BY, AND CONSTRUED IN ACCORDANCE WITH, THE LAW OF THE STATE OF {GOVERNING_LAW.upper()}.")


def build_agreement(a: Agreement, rng: random.Random, model: Model) -> None:
    threshold = Decimal(rng.randrange(25, 105, 5)) * MM
    facilities_of(a, rng, model)
    collateral_of(a, rng, model)
    cover(a)
    definitions(a, threshold)
    credits(a)
    boilerplate_front(a)
    # 4 to 7 covenants: two financial tests always, then a third test, the baskets and the reporting covenant as drawn
    total = rng.randint(4, 7)
    optional = rng.choice(["MinLiquidity", "MaxCapex"]) if total >= 5 and rng.random() < 0.6 else None
    remaining = total - 2 - (1 if optional else 0)
    reporting = remaining > len(BASKETS) or (remaining > 0 and rng.random() < 0.7)
    remaining -= 1 if reporting else 0
    chosen = rng.sample([b[3] for b in BASKETS], remaining)
    ordinal = affirmative(a, rng, model, 1, reporting)
    negative_covenants(a, rng, model, ordinal, chosen, optional)
    events_of_default(a, rng, model, threshold)
    boilerplate_back(a)
    if not 4 <= len(a.covenants) <= 7 or len(a.defaults) not in (2, 3):
        raise SystemExit(f"{a.id}: {len(a.covenants)} covenants, {len(a.defaults)} events of default")


# ---- the PDF: the same words as the .txt

STYLES = {
    "title": ParagraphStyle("title", fontName="Times-Bold", fontSize=13, leading=20, alignment=TA_CENTER, spaceAfter=10),
    "article": ParagraphStyle("article", fontName="Times-Bold", fontSize=11, leading=14, alignment=TA_CENTER, spaceBefore=12, spaceAfter=8),
    "section": ParagraphStyle("section", fontName="Times-Roman", fontSize=10.5, leading=13.5, alignment=TA_JUSTIFY, spaceAfter=6),
    "clause": ParagraphStyle("clause", fontName="Times-Roman", fontSize=10.5, leading=13.5, alignment=TA_JUSTIFY, leftIndent=18, spaceAfter=6),
}


def write_pdf(path: Path, agreement: Agreement) -> None:
    story = []
    for block in agreement.blocks:
        if block.kind in ("title", "article"):
            story.extend(Paragraph(escape(line), STYLES[block.kind]) for line in block.text.split("\n"))
            if block.kind == "title":
                story.append(Spacer(1, 0.4 * inch))
        elif block.kind == "schedule":
            story.append(Paragraph(escape(block.text.split("\n")[0]), STYLES["clause"]))
            cell = ParagraphStyle("cell", fontName="Times-Roman", fontSize=10, leading=12.5)
            table = Table([[Paragraph(escape(period + ":"), cell), Paragraph(escape(level), cell)] for period, level in block.table],
                          colWidths=[4.6 * inch, 1.0 * inch], hAlign="CENTER")
            table.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("BOTTOMPADDING", (0, 0), (-1, -1), 2)]))
            story.extend([table, Spacer(1, 6)])
        else:
            story.append(Paragraph(escape(block.text), STYLES[block.kind]))
    SimpleDocTemplate(str(path), pagesize=letter, invariant=1, title=agreement.title, author=agreement.agent,
                      leftMargin=inch, rightMargin=inch, topMargin=inch, bottomMargin=inch).build(story)


def write_table(directory: Path, table: str, columns: list[str], rows: list[dict]) -> None:
    with open(directory / f"{table}.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow(["" if row[c] is None else row[c] for c in columns])


def main() -> int:
    if reportlab.Version != REPORTLAB_VERSION:
        raise SystemExit(f"reportlab {reportlab.Version}: the committed PDFs were made with {REPORTLAB_VERSION}; "
                         f"install it (pip install reportlab=={REPORTLAB_VERSION})")
    rl_config.invariant = 1
    model, rng = load_model(), random.Random(SEED)
    directory, truth = LOANS_ROOT / "agreements", LOANS_ROOT / "ground_truth"
    for path in (directory, truth):
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True)

    agreements = []
    for number in range(12):
        borrower = BORROWERS[number % len(BORROWERS)]
        restated = number >= len(BORROWERS)
        title = "Amended and Restated Credit Agreement" if restated else "Credit Agreement"
        signed = date(2025 if restated else 2023, rng.randint(1, 11), rng.randint(1, 26))
        agreement = Agreement(f"CA-{number + 1:03d}", borrower, title, signed, rng.choice(AGENTS))
        build_agreement(agreement, rng, model)
        agreements.append(agreement)

    agreement_rows, covenants, steps, defaults, spans = [], [], [], [], []
    for a in agreements:
        text = a.text()
        (directory / f"{a.id}.txt").write_bytes(text.encode("utf-8"))
        write_pdf(directory / f"{a.id}.pdf", a)
        agreement_rows.append({"AGREEMENT_ID": a.id, "BORROWER_ID": a.borrower.id, "TITLE": a.title, "AGREEMENT_DATE": a.signed.isoformat(),
                               "ADMINISTRATIVE_AGENT": a.agent, "GOVERNING_LAW": GOVERNING_LAW, "PDF_PATH": f"agreements/{a.id}.pdf",
                               "TEXT_SHA256": hashlib.sha256(text.encode("utf-8")).hexdigest()})
        offsets = block_spans(text)
        if len(offsets) != len(a.blocks):
            raise SystemExit(f"{a.id}: {len(a.blocks)} blocks written, {len(offsets)} read back")

        def pointer(table: str, key: str, block: int, row: dict) -> None:
            start, end = offsets[block]
            if text[start:end] != a.blocks[block].text:
                raise SystemExit(f"{a.id}: block {block} does not read back")
            row["PASSAGE_SHA256"] = hashlib.sha256(text[start:end].encode("utf-8")).hexdigest()
            spans.append({"TABLE_NAME": table, "ROW_KEY": key, "FILE_PATH": f"agreements/{a.id}.txt", "CHAR_START": start, "CHAR_END": end,
                          "PASSAGE_SHA256": row["PASSAGE_SHA256"]})

        for row in a.covenants:
            pointer("LN_COVENANT", row["COVENANT_ID"], row["block"], row)
        for row in a.defaults:
            pointer("LN_EVENT_OF_DEFAULT", f"{row['AGREEMENT_ID']}|{row['CLAUSE_REF']}", row["block"], row)
        covenants.extend(a.covenants)
        steps.extend(a.steps)
        defaults.extend(a.defaults)
        if not a.steps or not any(row["EQUITY_CURE_ALLOWED"] == "true" for row in a.covenants) or not any(row["CROSS_DEFAULT_THRESHOLD_USD"] for row in a.defaults):
            raise SystemExit(f"{a.id}: every agreement has a step-down, an equity cure and a cross-default threshold")

    write_table(truth, "LN_BORROWER", model.columns("LN_BORROWER"), [{"BORROWER_ID": b.id, "LEGAL_NAME": b.name, "SECTOR": b.sector, "JURISDICTION": b.jurisdiction, "INTERNAL_RATING": b.rating} for b in BORROWERS])
    write_table(truth, "LN_CREDIT_AGREEMENT", model.columns("LN_CREDIT_AGREEMENT"), agreement_rows)
    write_table(truth, "LN_FACILITY", model.columns("LN_FACILITY"), [row for a in agreements for row in a.facilities])
    write_table(truth, "LN_COLLATERAL", model.columns("LN_COLLATERAL"), [row for a in agreements for row in a.collateral])
    write_table(truth, "LN_COVENANT", model.columns("LN_COVENANT"), covenants)
    write_table(truth, "LN_COVENANT_STEP", model.columns("LN_COVENANT_STEP"), steps)
    write_table(truth, "LN_EVENT_OF_DEFAULT", model.columns("LN_EVENT_OF_DEFAULT"), defaults)
    write_table(truth, "spans", ["TABLE_NAME", "ROW_KEY", "FILE_PATH", "CHAR_START", "CHAR_END", "PASSAGE_SHA256"], spans)
    print(f"agreements {len(agreements)}, borrowers {len(BORROWERS)}, facilities {sum(len(a.facilities) for a in agreements)}, "
          f"collateral {sum(len(a.collateral) for a in agreements)}, covenants {len(covenants)}, steps {len(steps)}, events of default {len(defaults)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
