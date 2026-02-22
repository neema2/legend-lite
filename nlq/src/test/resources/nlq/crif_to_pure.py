#!/usr/bin/env python3
"""
Generate ISDA CRIF / SIMM Pure model from spec knowledge and simm-lib Java interfaces.

Outputs to: crif-model.pure
"""

import os

OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "crif-model.pure")


def generate():
    lines = []
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("// ISDA CRIF & SIMM — Risk Data Standards")
    lines.append("// Source: ISDA Common Risk Interchange Format v1.36 + AcadiaSoft simm-lib")
    lines.append("// ═══════════════════════════════════════════════════════════")
    lines.append("")
    lines.append("Profile nlq::NlqProfile")
    lines.append("{")
    lines.append("    stereotypes: [dimension, metric];")
    lines.append("    tags: [description, synonyms, sampleValues, unit, importance, businessDomain, exampleQuestions, whenToUse];")
    lines.append("}")
    lines.append("")

    # ─── CRIF Record ───
    lines.append("// ─── CRIF (Common Risk Interchange Format) ───")
    lines.append("")
    lines.append("Class <<nlq::NlqProfile.metric>>")
    lines.append("  {nlq::NlqProfile.description = 'ISDA Common Risk Interchange Format record for exchanging risk sensitivities between counterparties',")
    lines.append("   nlq::NlqProfile.businessDomain = 'Risk Data Standards',")
    lines.append("   nlq::NlqProfile.whenToUse = 'Use for ISDA SIMM initial margin calculations, risk sensitivity exchange, and regulatory capital',")
    lines.append("   nlq::NlqProfile.exampleQuestions = 'What are the total SIMM sensitivities by product class? Show me CRIF records for IR delta risk'}")
    lines.append("crif::CRIFRecord")
    lines.append("{")
    crif_fields = [
        ("tradeId", "String", "Unique trade identifier"),
        ("portfolioId", "String", "Portfolio or netting set identifier"),
        ("productClass", "String", "SIMM product class: RatesFX, Credit, Equity, Commodity", "sampleValues = 'RatesFX, Credit, Equity, Commodity'"),
        ("riskType", "String", "SIMM risk type: Risk_IRCurve, Risk_IRVol, Risk_CreditQ, Risk_Equity, Risk_FX, Risk_Commodity, etc.", "sampleValues = 'Risk_IRCurve, Risk_IRVol, Risk_CreditQ, Risk_Equity, Risk_FX'"),
        ("qualifier", "String", "Risk qualifier — currency for IR, issuer/index for Credit, ticker for Equity, commodity type for Commodity"),
        ("bucket", "String", "SIMM bucket number within the risk type"),
        ("label1", "String", "Sub-bucket label — tenor for IR (e.g. 2w, 1m, 3m, 1y, 5y, 10y, 30y), maturity for Credit/Equity vol"),
        ("label2", "String", "Secondary label — sub-curve for IR (e.g. OIS, Libor3m), empty for other risk types"),
        ("amount", "Float", "Risk sensitivity amount in local currency", "unit = 'currency'"),
        ("amountCurrency", "String", "Currency of the sensitivity amount (ISO 4217)"),
        ("amountUSD", "Float", "Risk sensitivity amount converted to USD", "unit = 'currency'"),
        ("imModel", "String", "Initial margin model: SIMM, Schedule", "sampleValues = 'SIMM, Schedule'"),
        ("postRegulation", "String", "Regulation under which the posting entity calculates margin"),
        ("collectRegulation", "String", "Regulation under which the collecting entity calculates margin"),
        ("endDate", "StrictDate", "Maturity or end date of the trade"),
        ("creditQuality", "String", "Credit quality category for Schedule IM"),
        ("longShortInd", "String", "Long or short indicator for Schedule IM"),
        ("coveredBondInd", "String", "Covered bond indicator"),
        ("trancheThickness", "Float", "Tranche thickness for securitized products", "unit = 'percent'"),
    ]
    for f in crif_fields:
        name, ptype, desc = f[0], f[1], f[2]
        extra = f", nlq::NlqProfile.{f[3]}" if len(f) > 3 else ""
        lines.append(f"    {{nlq::NlqProfile.description = '{desc}'{extra}}}")
        lines.append(f"    {name}: {ptype}[0..1];")
    lines.append("}")
    lines.append("")

    # ─── SIMM Calculation Hierarchy ───
    lines.append("// ─── SIMM Calculation Model ───")
    lines.append("")

    simm_classes = [
        ("simm::SIMMResult", "metric", "ISDA SIMM calculation result — initial margin amount for a portfolio or netting set",
         "Use for ISDA SIMM initial margin results, margin amounts by product class and risk class", [
            ("portfolioId", "String", "Portfolio or netting set identifier"),
            ("calculationDate", "StrictDate", "Date of the SIMM calculation"),
            ("simmVersion", "String", "SIMM version used (e.g. 2.5, 2.6)"),
            ("totalMargin", "Float", "Total SIMM initial margin amount", "unit = 'currency'"),
            ("currency", "String", "Margin currency"),
            ("productMarginRatesFX", "Float", "SIMM margin for RatesFX product class", "unit = 'currency'"),
            ("productMarginCredit", "Float", "SIMM margin for Credit product class", "unit = 'currency'"),
            ("productMarginEquity", "Float", "SIMM margin for Equity product class", "unit = 'currency'"),
            ("productMarginCommodity", "Float", "SIMM margin for Commodity product class", "unit = 'currency'"),
            ("addOnMargin", "Float", "SIMM add-on margin for notional and fixed amounts", "unit = 'currency'"),
        ]),
        ("simm::ProductClassMargin", "metric", "SIMM margin breakdown by product class (RatesFX, Credit, Equity, Commodity)",
         "Use for SIMM product class level margin components", [
            ("productClass", "String", "Product class: RatesFX, Credit, Equity, Commodity"),
            ("deltaMargin", "Float", "Delta margin component", "unit = 'currency'"),
            ("vegaMargin", "Float", "Vega margin component", "unit = 'currency'"),
            ("curvatureMargin", "Float", "Curvature margin component", "unit = 'currency'"),
            ("baseCorrMargin", "Float", "Base correlation margin (Credit only)", "unit = 'currency'"),
            ("totalMargin", "Float", "Total margin for this product class", "unit = 'currency'"),
        ]),
        ("simm::RiskClassMargin", "metric", "SIMM margin for a specific risk class within a product class",
         "Use for SIMM risk class level margin details", [
            ("riskClass", "String", "Risk class: InterestRate, CreditQualifying, CreditNonQualifying, Equity, Commodity, FX"),
            ("sensitivityClass", "String", "Sensitivity class: Delta, Vega, Curvature"),
            ("marginAmount", "Float", "Margin amount for this risk class", "unit = 'currency'"),
            ("concentrationRiskFactor", "Float", "Concentration risk factor applied"),
        ]),
        ("simm::BucketMargin", "metric", "SIMM margin at the bucket level within a risk class",
         "Use for SIMM bucket level margin details and risk concentration", [
            ("bucket", "String", "Bucket identifier"),
            ("riskClass", "String", "Parent risk class"),
            ("weightedSensitivity", "Float", "Sum of risk-weighted sensitivities in this bucket", "unit = 'currency'"),
            ("concentrationFactor", "Float", "Threshold-based concentration risk factor"),
            ("bucketMargin", "Float", "Net margin for this bucket", "unit = 'currency'"),
        ]),
        ("simm::RiskWeight", "metric", "SIMM risk weight parameter for a specific risk type and bucket",
         "Use for SIMM risk weight calibration parameters", [
            ("riskType", "String", "Risk type this weight applies to"),
            ("bucket", "String", "Bucket this weight applies to"),
            ("riskWeight", "Float", "Risk weight value", "unit = 'percent'"),
            ("concentrationThreshold", "Float", "Concentration threshold", "unit = 'currency'"),
            ("correlationWithinBucket", "Float", "Intra-bucket correlation parameter", "unit = 'percent'"),
            ("correlationAcrossBucket", "Float", "Inter-bucket correlation parameter", "unit = 'percent'"),
        ]),
        ("simm::Sensitivity", "metric", "Individual risk sensitivity input to the SIMM calculation",
         "Use for individual trade-level or portfolio-level risk sensitivities", [
            ("productClass", "String", "Product class"),
            ("riskType", "String", "Risk type"),
            ("qualifier", "String", "Risk qualifier (currency, issuer, ticker, etc.)"),
            ("bucket", "String", "Bucket"),
            ("label1", "String", "Primary label (tenor, maturity)"),
            ("label2", "String", "Secondary label (sub-curve)"),
            ("amount", "Float", "Sensitivity amount", "unit = 'currency'"),
            ("amountUSD", "Float", "Sensitivity amount in USD", "unit = 'currency'"),
            ("amountCurrency", "String", "Currency of sensitivity"),
        ]),
    ]

    # Schedule IM classes
    schedule_classes = [
        ("schedule::ScheduleIMResult", "metric", "Schedule IM calculation result — gross and net initial margin using the simplified schedule approach",
         "Use for Schedule IM initial margin results as an alternative to SIMM", [
            ("portfolioId", "String", "Portfolio identifier"),
            ("grossIM", "Float", "Gross initial margin before netting", "unit = 'currency'"),
            ("netIM", "Float", "Net initial margin after NGR adjustment", "unit = 'currency'"),
            ("netToGrossRatio", "Float", "Net-to-gross ratio (NGR)", "unit = 'percent'"),
            ("currency", "String", "Margin currency"),
        ]),
        ("schedule::ScheduleNotional", "metric", "Schedule IM notional input for a trade",
         "Use for Schedule IM trade-level notional inputs", [
            ("tradeId", "String", "Trade identifier"),
            ("productClass", "String", "Product class"),
            ("notionalAmount", "Float", "Trade notional amount", "unit = 'currency'"),
            ("notionalCurrency", "String", "Notional currency"),
            ("presentValue", "Float", "Present value of the trade", "unit = 'currency'"),
            ("pvCurrency", "String", "PV currency"),
            ("endDate", "StrictDate", "Trade maturity date"),
        ]),
    ]

    # FRTB-SA classes
    frtb_classes = [
        ("frtb::FRTBSAResult", "metric", "FRTB Standardized Approach capital calculation result",
         "Use for FRTB-SA market risk capital requirements", [
            ("portfolioId", "String", "Portfolio identifier"),
            ("calculationDate", "StrictDate", "Calculation date"),
            ("sensitivitiesBasedCharge", "Float", "Sensitivities-based capital charge (SbM)", "unit = 'currency'"),
            ("defaultRiskCharge", "Float", "Default risk charge (DRC)", "unit = 'currency'"),
            ("residualRiskAddOn", "Float", "Residual risk add-on (RRAO)", "unit = 'currency'"),
            ("totalCapital", "Float", "Total FRTB-SA capital requirement", "unit = 'currency'"),
            ("currency", "String", "Capital currency"),
        ]),
        ("frtb::FRTBSensitivity", "metric", "FRTB-SA risk sensitivity input",
         "Use for FRTB standardized approach sensitivity inputs", [
            ("riskClass", "String", "FRTB risk class: GIRR, CSR, Equity, Commodity, FX"),
            ("riskMeasure", "String", "Risk measure: Delta, Vega, Curvature"),
            ("bucket", "String", "FRTB bucket"),
            ("riskFactor", "String", "Specific risk factor"),
            ("sensitivity", "Float", "Sensitivity value", "unit = 'currency'"),
            ("currency", "String", "Sensitivity currency"),
        ]),
    ]

    all_class_groups = [
        ("SIMM Calculation", simm_classes),
        ("Schedule IM", schedule_classes),
        ("FRTB Standardized Approach", frtb_classes),
    ]

    total_props = 0
    class_count = 1  # Already counted CRIFRecord
    total_props += len(crif_fields)

    for group_name, class_list in all_class_groups:
        lines.append(f"// ─── {group_name} ───")
        lines.append("")

        for qualified, stereotype, desc, wtu, props in class_list:
            class_count += 1
            lines.append(f"Class <<nlq::NlqProfile.{stereotype}>>")
            lines.append(f"  {{nlq::NlqProfile.description = '{desc}',")
            lines.append(f"   nlq::NlqProfile.businessDomain = 'Risk Data Standards',")
            lines.append(f"   nlq::NlqProfile.whenToUse = '{wtu}'}}")
            lines.append(qualified)
            lines.append("{")

            for p in props:
                name, ptype, pdesc = p[0], p[1], p[2]
                extra = f", nlq::NlqProfile.{p[3]}" if len(p) > 3 else ""
                lines.append(f"    {{nlq::NlqProfile.description = '{pdesc}'{extra}}}")
                lines.append(f"    {name}: {ptype}[0..1];")
                total_props += 1

            lines.append("}")
            lines.append("")

    with open(OUTPUT_FILE, 'w') as f:
        f.write('\n'.join(lines))

    print(f"Generated {OUTPUT_FILE}")
    print(f"  Classes:    {class_count}")
    print(f"  Properties: {total_props}")


if __name__ == "__main__":
    generate()
