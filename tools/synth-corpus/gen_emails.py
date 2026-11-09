#!/usr/bin/env python3
"""Generate the study's synthetic sales emails: projects/unstructured/corpus/emails/.

    python tools/synth-corpus/gen_emails.py

Standard library only. Seeded and deterministic: a run replaces bodies/ and ground_truth/ with the same bytes. Every
enum value is read from or checked against projects/unstructured/model.pure.

Everything here is fictional: the firms, the people, the issuers, the deals. Addresses are under the reserved .example
domain. A deal's identifier is SYN- and eight characters, a scheme of this corpus alone: the hyphen means it cannot be a
real CUSIP or ISIN.

ground_truth/ holds one CSV per store::Sales table, named for it, with the model's columns; SA_EMAIL has no BODY column
because the body is bodies/<messageId>.txt. spans.csv says, for each AllocationRequest, exactly which paragraph of which
body states its size and limit.
"""
from __future__ import annotations

import csv
import hashlib
import random
import shutil
import string
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "unstructured-corpus"))
from corpus_schema import EMAILS_ROOT, Model, block_spans, load_model  # noqa: E402

SEED = 20261108
BANK = "Merrowfield Securities"
BANK_DOMAIN = "merrowfield.example"
SYNDICATE = f"syndicate@{BANK_DOMAIN}"
COMPLIANCE = f"compliance@{BANK_DOMAIN}"
MM = Decimal(1_000_000)
ALLOCATION_ROUNDING = Decimal("0.05")  # an allocation is rounded to the nearest $50k
DISCLAIMER = ("For institutional investors only. Indicative and subject to allocation; not an offer or a commitment to "
              "deal. Refer to the offering document.")


def mm(amount: Decimal) -> str:
    """$25mm, $7.35mm."""
    text = format((amount / MM).normalize(), "f")
    return f"${text}mm"


def billions(amount: Decimal) -> str:
    return f"${format((amount / (MM * 1000)).quantize(Decimal('0.01')).normalize(), 'f')}bn"


def d(text: str) -> Decimal:
    return Decimal(text)


@dataclass(frozen=True)
class Client:
    id: str
    name: str
    kind: str
    domicile: str
    tier: str
    desk: str
    domain: str


@dataclass(frozen=True)
class Contact:
    id: str
    client: str
    name: str
    role: str

    @property
    def first(self) -> str:
        return self.name.split()[0]


@dataclass(frozen=True)
class Sales:
    id: str
    name: str
    desk: str
    region: str
    team: str

    @property
    def first(self) -> str:
        return self.name.split()[0]

    @property
    def address(self) -> str:
        return ".".join(self.name.lower().split()) + "@" + BANK_DOMAIN


@dataclass(frozen=True)
class Deal:
    id: str
    issuer: str
    short: str
    product: str
    launch: str
    size: Decimal
    pitch: str
    talk: str
    coupon: Optional[str] = None
    maturity: Optional[str] = None
    pricing: Optional[str] = None
    reoffer: Optional[Decimal] = None
    book: Optional[Decimal] = None
    settles: str = "T+3"

    @property
    def is_equity(self) -> bool:
        return self.product in ("IPO", "FollowOn", "BlockTrade")

    def price(self, value: Decimal) -> str:
        return f"${value:.2f}" if self.is_equity else f"{value:.3f}".rstrip("0").ljust(len(f"{value:.2f}"), "0")

    @property
    def pro_rata(self) -> Decimal:
        return self.size / self.book

    @property
    def cover(self) -> str:
        return f"{(self.book / self.size).quantize(Decimal('0.1'))}x"


@dataclass
class Request:
    size: Decimal
    limit: Optional[Decimal]
    sensitivity: str


@dataclass
class Message:
    message_id: str
    parent: Optional[str]
    thread: str
    sent: datetime
    direction: str
    topic: str
    subject: str
    paragraphs: list[str]
    cc: Optional[str]
    request_paragraph: Optional[int] = None


@dataclass
class Scene:
    """One thread as it is written: who, about what, and the running state of its request."""
    rng: random.Random
    client: Client
    contact: Contact
    sales: Sales
    deal: Optional[Deal]
    request: Optional[Request] = None
    request_message: Optional[Message] = None
    allocated: Optional[Decimal] = None
    status: Optional[str] = None
    responded: Optional[datetime] = None
    messages: list[Message] = field(default_factory=list)

    def choose(self, *options: str) -> str:
        return self.rng.choice(options)


# ---- what is said. Each returns (direction, topic, paragraphs, cc)

def ioi_out(s: Scene):
    deal = s.deal
    opener = s.choose(
        f"{deal.short} is live this morning. {deal.pitch}",
        f"New one for you: {deal.pitch}",
        f"Wanted you to see this before the screens fill up. {deal.pitch}",
    )
    talk = s.choose(
        f"Talk is {deal.talk}. Happy to walk you through the credit if useful.",
        f"Initial talk {deal.talk}. Syndicate expects decent demand, so earlier is better.",
        f"We are at {deal.talk} for now. Let me know if you want the roadshow slides.",
    )
    ask = s.choose("Any interest? Even a rough indication helps us with the book.",
                   "Is this one for you? Give me a shout with a rough size if so.",
                   "Let me know where you might be.")
    return "Outbound", "IndicationOfInterest", [opener, talk, ask], SYNDICATE


def ioi_in(s: Scene):
    paragraphs = [
        s.choose("Thanks for this. We have looked at the name before and it fits the mandate on paper.",
                 "Appreciate the early look. The credit committee is comfortable with the sector for now.",
                 "Thanks. Interesting, though we are fairly full on the sector."),
        s.choose("Treat this as an indication only, not an order. We would need to see where talk settles before we commit.",
                 "No order yet. I will come back once the PM has signed off, probably after guidance.",
                 "Not one for us at this level, but keep us posted if the concession widens."),
    ]
    return "Inbound", "IndicationOfInterest", paragraphs, None


def request_text(s: Scene, request: Request) -> str:
    deal, size = s.deal, mm(request.size)
    if request.sensitivity == "Flexible":
        return s.choose(f"Please enter {size} for us at re-offer, no limit. We are price takers on this one.",
                        f"{size} for us, market order, whatever it clears at.",
                        f"Put us in for {size} with no limit on price.")
    limit = deal.price(request.limit)
    if request.sensitivity == "Hard":
        return s.choose(f"Please put us in for {size} of the {deal.short} at a {limit} limit. That limit is hard: we drop above it.",
                        f"We would like {size}, limit {limit}. It is firm, no room through that level.",
                        f"Our order is {size} with a hard limit of {limit}. Please do not ask us to move it.")
    return s.choose(f"Put us down for {size} with a {limit} limit. It is soft, so call me if it clears a touch through.",
                    f"Order is {size}, limit {limit}. There is some give if the book is strong, but speak to me first.",
                    f"Please show {size} at {limit}. Treat the limit as soft for now.")


def order(s: Scene, size: str, limit: Optional[str], sensitivity: str):
    s.request = Request(d(size) * MM, d(limit) if limit else None, sensitivity)
    lead = s.choose("We have done the work on this and the PM is happy.",
                    "Following up on this one.",
                    "Committee signed off this morning.")
    close = s.choose("Let me know what the book looks like.", "Please confirm once it is in.", "Shout if anything changes.")
    return "Inbound", "AllocationRequest", [lead, request_text(s, s.request), close], None, 1


def revise(s: Scene, size: str, limit: Optional[str], sensitivity: str):
    s.request = Request(d(size) * MM, d(limit) if limit else None, sensitivity)
    lead = s.choose("Saw the reprice. That changes our number.", "The new level does not work at our old size.")
    close = "Please replace our earlier order with this one."
    return "Inbound", "AllocationRequest", [lead, request_text(s, s.request), close], None, 1


def ack(s: Scene):
    paragraphs = [s.choose(f"Got it, you are in the book at {mm(s.request.size)}.",
                           f"Confirmed, {mm(s.request.size)} entered with syndicate.",
                           f"Thanks, order is in for {mm(s.request.size)}."),
                  s.choose("I will come back as soon as we have allocations.",
                           "Timing still looks like today; I will keep you posted.",
                           "Nothing further needed from you for now.")]
    return "Outbound", "AllocationRequest", paragraphs, SYNDICATE


def book_update(s: Scene, book: str, accounts: int, guidance: str, stamp: str):
    deal = s.deal
    paragraphs = [f"Book update on {deal.short}: {billions(d(book) * MM)} from {accounts} accounts as of {stamp} ET, "
                  f"against {mm(deal.size)} of supply.",
                  s.choose(f"Guidance is now {guidance}.", f"We have moved to {guidance}."),
                  s.choose("Numbers are subject to change until books close.", "Please get any final orders in early.")]
    return "Outbound", "BookUpdate", paragraphs, SYNDICATE


def reprice(s: Scene, old: str, new: str, deadline: str):
    deal = s.deal
    paragraphs = [f"Reprice on {deal.short}: demand has let syndicate tighten, and re-offer moves from {old} to {new}.",
                  f"Existing orders stand unless you tell us otherwise. Please reconfirm or amend by {deadline} ET.",
                  s.choose("Sorry for the short notice.", "Call me if you want to talk it through.")]
    return "Outbound", "PricingUpdate", paragraphs, SYNDICATE


def allocated_amount(s: Scene) -> Decimal:
    raw = s.request.size * s.deal.pro_rata / MM
    return (raw / ALLOCATION_ROUNDING).quantize(Decimal(1), rounding=ROUND_HALF_UP) * ALLOCATION_ROUNDING * MM


def respond(s: Scene, status: str, allocated: Decimal) -> None:
    s.status, s.allocated = status, allocated


def partial(s: Scene, reason: str):
    deal = s.deal
    respond(s, "Partial", allocated_amount(s))
    percent = (s.allocated / s.request.size * 100).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
    paragraphs = [f"Allocations are out on {deal.short}. Final book {billions(deal.book)}, {deal.cover} covered on "
                  f"{mm(deal.size)}, re-offer {deal.price(deal.reoffer)}.",
                  f"You are done on {mm(s.allocated)} of your {mm(s.request.size)} ({percent}%). {reason}",
                  s.choose("Appreciate the support on this one.", "Let me know if you want to go through it.",
                           "I know it is less than you wanted. We will remember the order on the next one.")]
    return "Outbound", "AllocationConfirmation", paragraphs, SYNDICATE


def fill(s: Scene, reason: str):
    deal = s.deal
    respond(s, "Filled", s.request.size)
    paragraphs = [f"Good news on {deal.short}: you are filled in full, {mm(s.allocated)} at {deal.price(deal.reoffer)}.",
                  reason,
                  s.choose("Settlement details to follow.", "Thanks for the order.")]
    return "Outbound", "AllocationConfirmation", paragraphs, SYNDICATE


def decline(s: Scene, reason: str):
    respond(s, "Declined", Decimal(0))
    paragraphs = [f"Coming back on your {mm(s.request.size)} order in {s.deal.short}. Unfortunately we cannot allocate to it.",
                  reason,
                  s.choose("Sorry not to have better news.", "Happy to talk through it on the phone.")]
    return "Outbound", "AllocationConfirmation", paragraphs, SYNDICATE


def hold(s: Scene, reason: str):
    paragraphs = [f"I have to pause your order in {s.deal.short}. Compliance has placed it on hold: {reason}",
                  "Please do not treat the order as live, and do not trade around it, until you hear from me.",
                  "I will update you as soon as Compliance clears it."]
    return "Outbound", "ComplianceHold", paragraphs, COMPLIANCE


def cleared(s: Scene, stamp: str):
    paragraphs = [f"Compliance cleared the hold on your {s.deal.short} order at {stamp} ET.",
                  f"Your {mm(s.request.size)} is live in the book again, on the same terms."]
    return "Outbound", "ComplianceHold", paragraphs, COMPLIANCE


def settle(s: Scene):
    deal = s.deal
    client = s.client.id.replace("-", "")
    paragraphs = [f"Settlement instructions for {deal.short}, {mm(s.allocated)}:",
                  f"Trade date {deal.pricing}, settling {deal.settles}. Deliver versus payment through Merrowfield "
                  f"Clearing, DTC participant 9X41, reference {deal.id}/{client}. Identifier {IDENTIFIERS[deal.id]}.",
                  s.choose("Please confirm your SSIs match by close of business.", "Ops will chase if the SSIs on file are stale.")]
    return "Outbound", "Settlement", paragraphs, f"settlements@{BANK_DOMAIN}"


def settle_ack(s: Scene):
    paragraphs = [s.choose("Confirmed, SSIs on file are correct. Our ops team has booked it.",
                           "Matches our records. Booked on our side."),
                  s.choose("Thanks for the quick turnaround.", "Cheers.")]
    return "Inbound", "Settlement", paragraphs, None


def thanks(s: Scene):
    return "Inbound", "Other", [s.choose("Thanks, understood. Better than we feared.",
                                         "Noted, thank you. Keep us in mind for the next one.")], None


def block_out(s: Scene):
    deal = s.deal
    paragraphs = [f"Block in {deal.issuer}: {mm(deal.size)} coming via accelerated bookbuild after the close. {deal.pitch}",
                  f"{deal.talk} Books close in about ninety minutes, and this is strictly wall-crossed until launch.",
                  "Are you involved? I need a size and a limit."]
    return "Outbound", "IndicationOfInterest", paragraphs, SYNDICATE


def intro(s: Scene):
    paragraphs = ["Good to meet at the conference last week. I cover corporate treasury accounts in your region.",
                  "If it would be useful I can set up a call with our rates strategists on hedging the term-out you mentioned."]
    return "Outbound", "Other", paragraphs, None


def intro_reply(s: Scene):
    paragraphs = ["Thanks, that would be useful. Next month is better for us, after the quarter closes.",
                  "No live transaction at this point, just thinking ahead."]
    return "Inbound", "Other", paragraphs, None


# ---- the cast

CLIENTS = [
    Client("C-01", "Quillon Asset Management LLC", "AssetManager", "US", "Tier1", "IGCredit", "quillon-am.example"),
    Client("C-02", "Brantoak Capital Partners LP", "HedgeFund", "US", "Tier1", "HYCredit", "brantoak.example"),
    Client("C-03", "Sorrelmead Teachers Pension Fund", "Pension", "CA", "Tier1", "ECM", "sorrelmead-tpf.example"),
    Client("C-04", "Velmara Life Assurance plc", "Insurer", "GB", "Tier2", "IGCredit", "velmara-life.example"),
    Client("C-05", "Ysolde Sovereign Reserve Authority", "SovereignWealth", "SG", "Tier1", "Rates", "ysolde-sra.example"),
    Client("C-06", "Hallerbrunn Privatbank AG", "PrivateBank", "CH", "Tier3", "EquitiesCash", "hallerbrunn.example"),
    Client("C-07", "Corvane Treasury Services Inc.", "Corporate", "US", "Tier3", "FX", "corvane.example"),
    Client("C-08", "Ostermark Global Credit Fund", "HedgeFund", "LU", "Tier2", "HYCredit", "ostermark.example"),
]
CONTACTS = [
    Contact("P-01", "C-01", "Leena Marchetti", "PortfolioManager"),
    Contact("P-02", "C-01", "Owen Trask", "Trader"),
    Contact("P-03", "C-02", "Ilse Vandermeer", "PortfolioManager"),
    Contact("P-04", "C-03", "Callum Oyelaran", "PortfolioManager"),
    Contact("P-05", "C-03", "Bronwyn Hale", "Analyst"),
    Contact("P-06", "C-04", "Fergus Dunmore", "PortfolioManager"),
    Contact("P-07", "C-05", "Wen Tanaputra", "PortfolioManager"),
    Contact("P-08", "C-05", "Hadi Rahmanov", "Operations"),
    Contact("P-09", "C-06", "Greta Zollinger", "Trader"),
    Contact("P-10", "C-07", "Marcus Adeyemi-Holt", "Analyst"),
    Contact("P-11", "C-08", "Soren Lindqvist-Ames", "PortfolioManager"),
    Contact("P-12", "C-08", "Pia Korhonen", "Trader"),
]
SALES = [
    Sales("S-01", "Dara Whitlock", "IGCredit", "Americas", "IG Syndicate Sales"),
    Sales("S-02", "Tomasz Brevik", "HYCredit", "EMEA", "Leveraged Finance Sales"),
    Sales("S-03", "Anneke Solheim", "ECM", "Americas", "ECM Sales"),
    Sales("S-04", "Rafael Ostrander", "EquitiesCash", "Americas", "Equity Sales"),
    Sales("S-05", "Mei-Lin Castellano", "DCM", "APAC", "DCM Sales"),
]
DEALS = [
    Deal("D-01", "Kestrelline Logistics Inc.", "Kestrelline 5.125% 2034s", "NewIssueBond", "2026-03-03", d("750") * MM,
         "Kestrelline Logistics, $750mm senior unsecured 8-year, expected BBB-/Baa3.", "T+135 area",
         coupon="5.1250", maturity="2034-03-15", pricing="2026-03-04", reoffer=d("99.714"), book=d("2850") * MM),
    Deal("D-02", "Tarnwick Energy Partners LP", "Tarnwick 7.875% 2031s", "NewIssueBond", "2026-04-14", d("400") * MM,
         "Tarnwick Energy, $400mm senior notes, 5NC2, proceeds to refinance the 2027s.", "low 8s area",
         coupon="7.8750", maturity="2031-06-01", pricing="2026-04-15", reoffer=d("99.250"), book=d("1240") * MM),
    Deal("D-03", "Ashgrove Utilities Holdings Corp.", "Ashgrove 4.75% 2036s", "NewIssueBond", "2026-05-05", d("1000") * MM,
         "Ashgrove Utilities, $1bn 10-year first mortgage bonds, regulated opco guarantee.", "T+105 area",
         coupon="4.7500", maturity="2036-09-15", pricing="2026-05-05", reoffer=d("99.880"), book=d("3100") * MM),
    Deal("D-04", "Lumencrest Therapeutics Inc.", "Lumencrest IPO", "IPO", "2026-05-11", d("420") * MM,
         "Lumencrest Therapeutics IPO, $420mm base, late-stage oncology platform.", "the $16.00 to $18.00 range",
         pricing="2026-05-20", reoffer=d("18.00"), book=d("1680") * MM, settles="T+1"),
    Deal("D-05", "Portwain Semiconductor Corp.", "Portwain follow-on", "FollowOn", "2026-06-02", d("250") * MM,
         "Portwain Semiconductor follow-on, $250mm primary, overnight marketed.", "a 4% to 6% file-to-offer discount",
         pricing="2026-06-02", reoffer=d("41.35"), book=d("615") * MM, settles="T+1"),
    Deal("D-06", "Holvane Foods Group Inc.", "Holvane block", "BlockTrade", "2026-06-16", d("180") * MM,
         "Selling holder is the founding family trust, 90-day lock-up on the balance.", "Indicative discount 5% to last close.",
         pricing="2026-06-16", reoffer=d("27.60"), book=d("322") * MM, settles="T+1"),
    Deal("D-07", "Caldrith Materials Inc.", "Caldrith block", "BlockTrade", "2026-07-08", d("95") * MM,
         "Selling holder is a sponsor exiting its remaining stake.", "Indicative discount 6% to last close.",
         pricing="2026-07-08", reoffer=d("12.18"), book=d("190") * MM, settles="T+1"),
    # A floating-rate loan has no fixed coupon: coupon stays absent.
    Deal("D-08", "Bexford Health Services Inc.", "Bexford TLB", "SyndicatedLoan", "2026-07-13", d("600") * MM,
         "Bexford Health, $600mm 6-year term loan B, first lien, proceeds fund the Northgate acquisition.", "S+350 at 99 OID",
         maturity="2032-07-31", pricing="2026-07-28", reoffer=d("99.00"), book=d("1110") * MM, settles="T+7"),
    # Marketing, not priced: a coupon is set at pricing, so it stays absent with the re-offer.
    Deal("D-09", "Wexcombe Telecom Inc.", "Wexcombe 2031s", "NewIssueBond", "2026-09-14", d("500") * MM,
         "Wexcombe Telecom, $500mm senior secured 5-year, roadshow through next Tuesday.", "mid 6s area",
         maturity="2031-10-15", book=d("640") * MM),
    Deal("D-10", "Nimbrel Software Inc.", "Nimbrel follow-on", "FollowOn", "2026-10-05", d("150") * MM,
         "Nimbrel Software follow-on, $150mm, testing the waters ahead of a filing.", "no price guidance yet"),
]
IDENTIFIERS: dict[str, str] = {}

PRO_RATA = "Syndicate allocated strictly pro rata to order size once the book closed, and no account was scaled differently."
IPO_PRO_RATA = ("Long-only accounts were scaled back pro rata to demand across the institutional tranche, so this reflects "
                "the 4x book rather than anything about your order.")
LOAN_PRO_RATA = "Commitments were cut back pro rata across the CLO and institutional accounts at the final book."
MINIMUM_TICKET = "Tickets at or below $2mm were filled in full under the syndicate's minimum-ticket policy."
BLOCK_FILL = "The block cleared with room at this level, so no scale-back was needed."
FOLLOW_ON_FILL = "Priority was given to anchor orders received before launch, and yours was one of them."


def step(kind: Callable, *args) -> tuple:
    return (kind, args)


# 18 threads, 60 messages: (client, contact, salesperson, deal, the first message's time, subject, steps)
THREADS = [
    ("C-01", "P-01", "S-01", "D-01", "2026-03-03 08:41", "Kestrelline 8yr", [
        step(ioi_out), step(order, "25", "99.55", "Soft"), step(ack),
        step(reprice, "99.612", "99.714", "14:30"), step(revise, "20", "99.75", "Hard"),
        step(partial, PRO_RATA)]),
    ("C-04", "P-06", "S-01", "D-01", "2026-03-03 09:17", "Kestrelline order", [
        step(order, "15", "99.65", "Hard"), step(book_update, "1950", 118, "T+120 area", "11:00"),
        step(reprice, "99.612", "99.714", "14:30"),
        step(decline, "The deal re-offered at 99.714, which is above your 99.65 hard limit, so the order dropped at the reprice.")]),
    ("C-03", "P-05", "S-01", "D-01", "2026-03-03 11:06", "Kestrelline book", [
        step(book_update, "1950", 118, "T+120 area", "11:00"), step(order, "2", None, "Flexible"), step(ack),
        step(fill, MINIMUM_TICKET)]),
    ("C-02", "P-03", "S-02", "D-02", "2026-04-14 08:15", "Tarnwick 5NC2", [
        step(ioi_out), step(order, "30", "99.50", "Soft"), step(partial, PRO_RATA), step(settle), step(settle_ack)]),
    ("C-08", "P-11", "S-02", "D-02", "2026-04-14 10:02", "Tarnwick order", [
        step(order, "4", None, "Flexible"),
        step(hold, "the account is pending its annual sanctions re-screen, which lapsed on Friday."),
        step(cleared, "13:20"), step(fill, MINIMUM_TICKET.replace("$2mm", "$5mm"))]),
    ("C-04", "P-06", "S-01", "D-03", "2026-05-05 07:58", "Ashgrove 10yr", [step(ioi_out), step(ioi_in)]),
    ("C-05", "P-07", "S-05", "D-03", "2026-05-05 08:30", "Ashgrove FMB", [
        step(order, "60", "99.90", "Soft"), step(partial, PRO_RATA), step(thanks)]),
    ("C-03", "P-04", "S-03", "D-04", "2026-05-11 09:30", "Lumencrest IPO", [
        step(ioi_out), step(order, "40", "18.25", "Soft"), step(ack), step(partial, IPO_PRO_RATA), step(settle)]),
    ("C-06", "P-09", "S-03", "D-04", "2026-05-12 10:44", "Lumencrest allocation", [
        step(order, "3", "17.50", "Hard"),
        step(decline, "The institutional tranche is closed to private-bank accounts under the syndicate's allocation policy, and the priced level of $18.00 is through your limit in any case.")]),
    ("C-02", "P-03", "S-04", "D-05", "2026-06-02 16:12", "Portwain overnight", [
        step(order, "12", "41.50", "Hard"), step(fill, FOLLOW_ON_FILL), step(settle)]),
    ("C-01", "P-02", "S-04", "D-06", "2026-06-16 16:20", "Holvane block", [
        step(block_out), step(order, "18", "27.75", "Hard"), step(fill, BLOCK_FILL)]),
    ("C-08", "P-12", "S-04", "D-07", "2026-07-08 16:25", "Caldrith block", [
        step(block_out), step(order, "6", "12.25", "Soft"),
        step(hold, "the issuer went on our restricted list this afternoon after a separate mandate, and this needs sign-off from the control room.")]),
    ("C-04", "P-06", "S-02", "D-08", "2026-07-13 09:05", "Bexford TLB", [
        step(ioi_out), step(order, "20", "99.25", "Soft"), step(partial, LOAN_PRO_RATA)]),
    ("C-05", "P-08", "S-05", "D-09", "2026-09-15 02:40", "Wexcombe secured", [
        step(ioi_out), step(order, "35", "99.50", "Soft"), step(ack)]),
    ("C-06", "P-09", "S-04", "D-10", "2026-10-05 10:15", "Nimbrel TTW", [step(ioi_out), step(ioi_in)]),
    ("C-07", "P-10", "S-05", None, "2026-08-04 15:10", "Introduction", [step(intro), step(intro_reply)]),
    ("C-01", "P-01", "S-01", "D-03", "2026-05-05 09:12", "Ashgrove order", [
        step(order, "2", None, "Flexible"), step(fill, MINIMUM_TICKET), step(settle)]),
    ("C-03", "P-04", "S-04", "D-07", "2026-07-08 16:31", "Caldrith", [
        step(block_out), step(order, "10", "11.90", "Hard"),
        step(decline, "The block cleared at $12.18, above your $11.90 hard limit, so the order did not make the book.")]),
]


# ---- writing it down

def identifier(rng: random.Random) -> str:
    return "SYN-" + "".join(rng.choice(string.ascii_uppercase + string.digits) for _ in range(8))


def not_before(kind: Callable, args: tuple, deal: Optional[Deal]) -> Optional[datetime]:
    """The earliest a message of this kind can be sent: it speaks of something that has to have happened."""
    if kind is reprice:
        return datetime.fromisoformat(f"{deal.pricing} 10:05:00")
    if kind in (partial, fill, decline):
        return datetime.fromisoformat(f"{deal.pricing} 16:30:00")
    if kind is book_update:
        return datetime.fromisoformat(f"{deal.launch} {args[3]}:00") + timedelta(minutes=5)
    if kind is cleared:
        return datetime.fromisoformat(f"{deal.launch} {args[0]}:00") + timedelta(minutes=5)
    return None


def build(model: Model, rng: random.Random) -> list[Scene]:
    for deal in DEALS:
        IDENTIFIERS[deal.id] = identifier(rng)
    clients = {c.id: c for c in CLIENTS}
    contacts = {c.id: c for c in CONTACTS}
    sales = {s.id: s for s in SALES}
    deals = {x.id: x for x in DEALS}
    scenes, sequence = [], 0
    for number, (client, contact, salesperson, deal, start, subject, steps) in enumerate(THREADS, 1):
        scene = Scene(rng, clients[client], contacts[contact], sales[salesperson], deals[deal] if deal else None)
        thread_id = f"TH-{number:02d}"
        when = datetime.fromisoformat(start + ":00")
        parent = None
        for position, (kind, args) in enumerate(steps):
            if position:
                when += timedelta(minutes=rng.randint(7, 95), seconds=rng.randint(0, 59))
            floor = not_before(kind, args, scene.deal)
            if floor is not None and when < floor:
                when = floor + timedelta(minutes=rng.randint(0, 12), seconds=rng.randint(0, 59))
            said = kind(scene, *args)
            direction, topic, paragraphs, cc = said[:4]
            sequence += 1
            message = Message(f"MSG-{sequence:04d}", parent, thread_id, when, model.enum("SA_EMAIL", "DIRECTION", direction),
                              model.enum("SA_EMAIL", "TOPIC", topic),
                              ("RE: " if position else "") + subject, compose(scene, direction, paragraphs), cc)
            if len(said) == 5:
                message.request_paragraph = said[4] + 1  # after the greeting
                scene.request_message = message
            if kind in (partial, fill, decline):
                scene.responded = when
            scene.messages.append(message)
            parent = message.message_id
        scenes.append(scene)
    return scenes


def compose(scene: Scene, direction: str, paragraphs: list[str]) -> list[str]:
    if direction == "Outbound":
        name = scene.contact.first
        signature = f"{scene.sales.first}\n{scene.sales.name} | {scene.sales.team} | {BANK}"
        return [scene.choose(f"Hi {name},", f"{name},"), *paragraphs, signature, DISCLAIMER]
    name = scene.sales.first
    signature = f"{scene.contact.name}\n{scene.client.name}"
    return [scene.choose(f"Hi {name},", f"{name},"), *paragraphs, signature]


def write_table(directory: Path, table: str, columns: list[str], rows: list[dict]) -> None:
    with open(directory / f"{table}.csv", "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow(["" if row[c] is None else row[c] for c in columns])


def email_address(contact: Contact, client: Client) -> str:
    return ".".join(contact.name.lower().replace("-", " ").split()) + "@" + client.domain


def main() -> int:
    model, rng = load_model(), random.Random(SEED)
    scenes = build(model, rng)
    bodies, truth = EMAILS_ROOT / "bodies", EMAILS_ROOT / "ground_truth"
    for directory in (bodies, truth):
        if directory.exists():
            shutil.rmtree(directory)
        directory.mkdir(parents=True)

    enum = model.enum
    write_table(truth, "SA_CLIENT", model.columns("SA_CLIENT"), [{
        "CLIENT_ID": c.id, "LEGAL_NAME": c.name, "CLIENT_TYPE": enum("SA_CLIENT", "CLIENT_TYPE", c.kind), "DOMICILE": c.domicile,
        "TIER": enum("SA_CLIENT", "TIER", c.tier), "COVERAGE_DESK": enum("SA_CLIENT", "COVERAGE_DESK", c.desk)} for c in CLIENTS])
    clients = {c.id: c for c in CLIENTS}
    write_table(truth, "SA_CONTACT", model.columns("SA_CONTACT"), [{
        "CONTACT_ID": c.id, "CLIENT_ID": c.client, "CONTACT_NAME": c.name, "CONTACT_ROLE": enum("SA_CONTACT", "CONTACT_ROLE", c.role),
        "EMAIL_ADDRESS": email_address(c, clients[c.client])} for c in CONTACTS])
    write_table(truth, "SA_SALESPERSON", model.columns("SA_SALESPERSON"), [{
        "SALES_ID": s.id, "SALES_NAME": s.name, "DESK": enum("SA_SALESPERSON", "DESK", s.desk),
        "REGION": enum("SA_SALESPERSON", "REGION", s.region)} for s in SALES])
    write_table(truth, "SA_DEAL", model.columns("SA_DEAL"), [{
        "DEAL_ID": x.id, "ISSUER": x.issuer, "PRODUCT": enum("SA_DEAL", "PRODUCT", x.product), "IDENTIFIER": IDENTIFIERS[x.id],
        "COUPON": x.coupon, "MATURITY_DATE": x.maturity, "LAUNCH_DATE": x.launch, "PRICING_DATE": x.pricing, "SIZE_USD": f"{x.size:.2f}",
        "REOFFER_PRICE": None if x.reoffer is None else f"{x.reoffer:.4f}",
        "BOOK_SIZE_USD": None if x.book is None else f"{x.book:.2f}"} for x in DEALS])

    emails, requests, spans = [], [], []
    for scene in scenes:
        client_address = email_address(scene.contact, scene.client)
        for message in scene.messages:
            body = "\n\n".join(message.paragraphs) + "\n"
            (bodies / f"{message.message_id}.txt").write_bytes(body.encode("utf-8"))
            outbound = message.direction == "Outbound"
            emails.append({
                "MESSAGE_ID": message.message_id, "PARENT_MESSAGE_ID": message.parent,
                "DEAL_ID": scene.deal.id if scene.deal else None, "CLIENT_ID": scene.client.id, "SALES_ID": scene.sales.id,
                "THREAD_ID": message.thread, "SENT_AT": message.sent.isoformat(sep=" "),
                "FROM_ADDRESS": scene.sales.address if outbound else client_address,
                "TO_ADDRESSES": client_address if outbound else scene.sales.address, "CC_ADDRESSES": message.cc,
                "SUBJECT": message.subject, "DIRECTION": message.direction, "TOPIC": message.topic,
                "BODY_SHA256": hashlib.sha256(body.encode("utf-8")).hexdigest()})
        if scene.request is None:
            continue
        message = scene.request_message
        body = "\n\n".join(message.paragraphs) + "\n"
        start, end = block_spans(body)[message.request_paragraph]
        passage = body[start:end]
        size_text = mm(scene.request.size)
        if size_text not in passage or (scene.request.limit is not None and scene.deal.price(scene.request.limit) not in passage):
            raise SystemExit(f"{message.message_id}: the paragraph does not state the size and limit: {passage!r}")
        request_id = f"AR-{len(requests) + 1:03d}"
        sha = hashlib.sha256(passage.encode("utf-8")).hexdigest()
        status = scene.status if scene.status is not None else "Requested"
        requests.append({
            "REQUEST_ID": request_id, "EMAIL_MESSAGE_ID": message.message_id, "DEAL_ID": scene.deal.id, "CLIENT_ID": scene.client.id,
            "REQUESTED_QTY_USD": f"{scene.request.size:.2f}",
            "LIMIT_PRICE": None if scene.request.limit is None else f"{scene.request.limit:.4f}",
            "PRICE_SENSITIVITY": enum("SA_ALLOCATION_REQUEST", "PRICE_SENSITIVITY", scene.request.sensitivity),
            "ALLOCATED_QTY_USD": None if scene.allocated is None else f"{scene.allocated:.2f}",
            "STATUS": enum("SA_ALLOCATION_REQUEST", "STATUS", status),
            "RESPONDED_AT": None if scene.responded is None else scene.responded.isoformat(sep=" "), "PASSAGE_SHA256": sha})
        spans.append({"TABLE_NAME": "SA_ALLOCATION_REQUEST", "ROW_KEY": request_id, "FILE_PATH": f"bodies/{message.message_id}.txt",
                      "CHAR_START": start, "CHAR_END": end, "PASSAGE_SHA256": sha})
    if len({row["PASSAGE_SHA256"] for row in requests}) != len(requests):
        raise SystemExit("two allocation requests are stated by byte-identical paragraphs: their pointers would be ambiguous")

    write_table(truth, "SA_EMAIL", [c for c in model.columns("SA_EMAIL") if c != "BODY"], emails)
    write_table(truth, "SA_ALLOCATION_REQUEST", model.columns("SA_ALLOCATION_REQUEST"), requests)
    write_table(truth, "spans", ["TABLE_NAME", "ROW_KEY", "FILE_PATH", "CHAR_START", "CHAR_END", "PASSAGE_SHA256"], spans)
    counts = (len(CLIENTS), len(CONTACTS), len(SALES), len(DEALS), len(emails), len(THREADS), len(requests))
    print("clients %d, contacts %d, salespeople %d, deals %d, emails %d, threads %d, allocation requests %d" % counts)
    if counts[:6] != (8, 12, 5, 10, 60, 18):
        raise SystemExit("the corpus is specified at 8 clients, 12 contacts, 5 salespeople, 10 deals, 60 emails, 18 threads")
    return 0


if __name__ == "__main__":
    sys.exit(main())
