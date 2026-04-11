package com.gs.legend.test;

import org.junit.jupiter.api.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Domain-based stress test: 20 domains × 10 classes × ~12 properties = ~200 classes, ~2400 properties.
 * 20 databases, 20 mappings, ~200 tables, ~200 joins, cross-domain associations.
 *
 * Tests parsing, typechecking, normalization, and plan generation across
 * a realistic investment banking domain model loaded from .pure resource files.
 */
@DisplayName("Domain Stress Tests")
class StressDomainTest {

    private Connection conn;

    @BeforeEach
    void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:duckdb:");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) conn.close();
    }

    /** Load all .pure files from the stress/ resource directory, sorted by name. */
    private String loadStressModel() throws IOException {
        // Find the stress directory on the classpath
        var stressUrl = getClass().getClassLoader().getResource("stress");
        assertNotNull(stressUrl, "stress/ resource directory not found on classpath");

        Path stressDir = Path.of(stressUrl.getPath());
        List<Path> pureFiles;
        try (var stream = Files.list(stressDir)) {
            pureFiles = stream
                    .filter(p -> p.toString().endsWith(".pure"))
                    .filter(p -> !p.getFileName().toString().startsWith("91-")) // skip file-based runtime; we inject our own
                    .sorted()
                    .collect(Collectors.toList());
        }
        assertFalse(pureFiles.isEmpty(), "No .pure files found in stress/");

        var sb = new StringBuilder();
        for (Path f : pureFiles) {
            sb.append(Files.readString(f, StandardCharsets.UTF_8));
            sb.append("\n");
        }
        return sb.toString();
    }

    /** Append a connection + runtime referencing all 20 domain mappings with a single shared DB. */
    private String withRuntime(String model) {
        return model + """

                RelationalDatabaseConnection store::Conn { type: DuckDB; specification: InMemory { }; auth: NoAuth { }; }
                Runtime stress::RT {
                    mappings: [
                        products::ProductsMapping,
                        refdata::RefdataMapping,
                        counterparty::CounterpartyMapping,
                        org::OrgMapping,
                        positions::PositionsMapping,
                        trading::TradingMapping,
                        pnl::PnlMapping,
                        risk::RiskMapping,
                        settlement::SettlementMapping,
                        ops::OpsMapping,
                        collateral::CollateralMapping,
                        sales::SalesMapping,
                        regulatory::RegulatoryMapping,
                        marketdata::MarketdataMapping,
                        funding::FundingMapping,
                        accounting::AccountingMapping,
                        clearing::ClearingMapping,
                        tax::TaxMapping,
                        research::ResearchMapping,
                        prime::PrimeMapping
                    ];
                    connections: [
                        store::DB: [ environment: store::Conn ]
                    ];
                }
                """;
    }

    @Test
    @DisplayName("20-domain model: parse + build + normalize + plan generation")
    void testDomainModel() throws Exception {
        // ---- Phase 0: Load Pure source from resource files ----
        long t0 = System.nanoTime();
        String rawModel = loadStressModel();
        long loadMs = (System.nanoTime() - t0) / 1_000_000;

        String model = withRuntime(rawModel);

        System.out.println("=== DOMAIN STRESS TEST: 20 Domains ===");
        System.out.println("Pure source size: " + (model.length() / 1024) + " KB");
        System.out.println("Phase 0 (load files): " + loadMs + " ms");

        // ---- Phase 1: Parse + build model ----
        long t1 = System.nanoTime();
        var builder = new com.gs.legend.model.PureModelBuilder().addSource(model);
        long buildMs = (System.nanoTime() - t1) / 1_000_000;
        System.out.println("Phase 1 (parse + build model): " + buildMs + " ms");

        // ---- Phase 2: Normalize all 20 mappings ----
        long t2 = System.nanoTime();
        var mappingNames = builder.resolveMappingNames("stress::RT");
        System.out.println("Resolved " + mappingNames.size() + " mappings from runtime");
        var normalizer = new com.gs.legend.compiler.MappingNormalizer(builder, mappingNames);
        long normMs = (System.nanoTime() - t2) / 1_000_000;
        System.out.println("Phase 2 (normalize " + mappingNames.size() + " mappings): " + normMs + " ms");

        var dialect = builder.resolveDialect("stress::RT");
        var normalizedMapping = normalizer.normalizedMapping();
        var modelCtx = normalizer.modelContext();

        // ---- Phase 3: Wide projections with deep cross-domain navigation ----
        // Each query projects 40-60 properties, navigating 2-4 hops across domains
        String[] queries = {

            // Q0: Trade → Instrument → Sector + Counterparty + Trader + Book (48 props, 3-hop)
            """
            trading::Trade.all()->project(~[
                tradeId, tradeDate, settlementDate, quantity, price, notional, side, status,
                tradeType, currency, commission, fees, executionVenue, isBlock, createdTime, lastModifiedTime,
                instrName:t|$t.instrument.name, instrTicker:t|$t.instrument.ticker,
                instrAssetClass:t|$t.instrument.assetClass, instrCurrency:t|$t.instrument.currency,
                instrIsin:t|$t.instrument.isin, instrCusip:t|$t.instrument.cusip,
                instrSedol:t|$t.instrument.sedol, instrProductType:t|$t.instrument.productType,
                instrIsActive:t|$t.instrument.isActive, instrNotional:t|$t.instrument.notionalAmount,
                instrListingDate:t|$t.instrument.listingDate, instrMaturityDate:t|$t.instrument.maturityDate,
                instrDescription:t|$t.instrument.description, instrIssueDate:t|$t.instrument.issueDate,
                sectorName:t|$t.instrument.sector.name, sectorGics:t|$t.instrument.sector.gicsCode,
                sectorLevel:t|$t.instrument.sector.level, sectorIsActive:t|$t.instrument.sector.isActive,
                cptyId:t|$t.counterparty.counterpartyId, cptyName:t|$t.counterparty.legalName,
                cptyLei:t|$t.counterparty.lei, cptyType:t|$t.counterparty.counterpartyType,
                cptyDomicile:t|$t.counterparty.domicile, cptyTier:t|$t.counterparty.tier,
                cptyIsActive:t|$t.counterparty.isActive, cptyCreditRating:t|$t.counterparty.creditRating,
                traderFirst:t|$t.trader.firstName, traderLast:t|$t.trader.lastName,
                traderId:t|$t.trader.traderId, traderSeniority:t|$t.trader.seniority,
                bookId:t|$t.book.bookId, bookName:t|$t.book.name,
                bookCurrency:t|$t.book.currency, bookType:t|$t.book.bookType
            ])""",

            // Q1: Trade → Book → Desk + Instrument + Trader (50 props, 3-hop)
            """
            trading::Trade.all()->project(~[
                tradeId, tradeDate, quantity, price, notional, side, status, tradeType,
                currency, commission, fees, executionVenue, isBlock, createdTime,
                bkId:t|$t.book.bookId, bkName:t|$t.book.name, bkCurrency:t|$t.book.currency,
                bkType:t|$t.book.bookType, bkStrategy:t|$t.book.strategy,
                bkIsActive:t|$t.book.isActive, bkOpenDate:t|$t.book.openDate,
                bkLegalEntity:t|$t.book.legalEntity, bkPnlYTD:t|$t.book.pnlYTD,
                bkDescription:t|$t.book.description,
                deskId:t|$t.book.desk.deskId, deskName:t|$t.book.desk.name,
                deskType:t|$t.book.desk.deskType, deskAssetClass:t|$t.book.desk.assetClass,
                deskRegion:t|$t.book.desk.region, deskIsActive:t|$t.book.desk.isActive,
                deskHeadTrader:t|$t.book.desk.headTrader, deskCostCenter:t|$t.book.desk.costCenter,
                deskFloor:t|$t.book.desk.floor, deskBuilding:t|$t.book.desk.building,
                deskPhone:t|$t.book.desk.phone, deskRiskLimit:t|$t.book.desk.riskLimit,
                deskPnlTarget:t|$t.book.desk.pnlTarget,
                instrId:t|$t.instrument.instrumentId, instrName:t|$t.instrument.name,
                instrTicker:t|$t.instrument.ticker, instrAsset:t|$t.instrument.assetClass,
                instrCcy:t|$t.instrument.currency, instrIsin:t|$t.instrument.isin,
                instrActive:t|$t.instrument.isActive, instrNotional:t|$t.instrument.notionalAmount,
                instrMaturity:t|$t.instrument.maturityDate, instrType:t|$t.instrument.productType,
                traderId:t|$t.trader.traderId, traderFirst:t|$t.trader.firstName,
                traderLast:t|$t.trader.lastName, traderSeniority:t|$t.trader.seniority,
                traderSpecialization:t|$t.trader.specialization, traderIsActive:t|$t.trader.isActive,
                traderEmail:t|$t.trader.email, traderBadge:t|$t.trader.badge
            ])""",

            // Q2: DailyPnL → Book → Desk + Trader (45 props, 3-hop: pnl→positions→org)
            """
            pnl::DailyPnL.all()->project(~[
                pnlId, pnlDate, totalPnL, realizedPnL, unrealizedPnL, newTradePnL, carryPnL,
                fxPnL, commissions, fees, currency, isOfficial, publishedTime, version,
                bkId:p|$p.book.bookId, bkName:p|$p.book.name, bkCurrency:p|$p.book.currency,
                bkType:p|$p.book.bookType, bkStrategy:p|$p.book.strategy,
                bkIsActive:p|$p.book.isActive, bkOpenDate:p|$p.book.openDate,
                bkLegalEntity:p|$p.book.legalEntity, bkPnlYTD:p|$p.book.pnlYTD,
                deskId:p|$p.book.desk.deskId, deskName:p|$p.book.desk.name,
                deskType:p|$p.book.desk.deskType, deskAssetClass:p|$p.book.desk.assetClass,
                deskRegion:p|$p.book.desk.region, deskIsActive:p|$p.book.desk.isActive,
                deskHeadTrader:p|$p.book.desk.headTrader, deskCostCenter:p|$p.book.desk.costCenter,
                deskFloor:p|$p.book.desk.floor,
                traderId:p|$p.trader.traderId, traderFirst:p|$p.trader.firstName,
                traderLast:p|$p.trader.lastName, traderSeniority:p|$p.trader.seniority,
                traderEmail:p|$p.trader.email, traderIsActive:p|$p.trader.isActive,
                traderHireDate:p|$p.trader.hireDate, traderLicense:p|$p.trader.license,
                deskId2:p|$p.desk.deskId, deskName2:p|$p.desk.name,
                deskRegion2:p|$p.desk.region, deskAsset2:p|$p.desk.assetClass,
                deskActive2:p|$p.desk.isActive
            ])""",

            // Q3: Settlement → Trade → Instrument + Counterparty → Country (52 props, 4-hop)
            """
            settlement::Settlement.all()->project(~[
                settlementId, settlementDate, amount, currency, status, settledDate,
                settlementMethod, custodian, depository, settledAmount, failReason,
                isPartial, priority, lastUpdated,
                trdId:s|$s.trade.tradeId, trdDate:s|$s.trade.tradeDate,
                trdQty:s|$s.trade.quantity, trdPrice:s|$s.trade.price,
                trdNotional:s|$s.trade.notional, trdSide:s|$s.trade.side,
                trdStatus:s|$s.trade.status, trdType:s|$s.trade.tradeType,
                trdCcy:s|$s.trade.currency, trdComm:s|$s.trade.commission,
                trdFees:s|$s.trade.fees, trdVenue:s|$s.trade.executionVenue,
                trdIsBlock:s|$s.trade.isBlock, trdCreated:s|$s.trade.createdTime,
                instrId:s|$s.trade.instrument.instrumentId,
                instrName:s|$s.trade.instrument.name,
                instrTicker:s|$s.trade.instrument.ticker,
                instrAsset:s|$s.trade.instrument.assetClass,
                instrCcy:s|$s.trade.instrument.currency,
                instrIsin:s|$s.trade.instrument.isin,
                instrActive:s|$s.trade.instrument.isActive,
                instrType:s|$s.trade.instrument.productType,
                cptyId:s|$s.counterparty.counterpartyId,
                cptyName:s|$s.counterparty.legalName,
                cptyLei:s|$s.counterparty.lei,
                cptyType:s|$s.counterparty.counterpartyType,
                cptyDomicile:s|$s.counterparty.domicile,
                cptyCreditRating:s|$s.counterparty.creditRating,
                cptyTier:s|$s.counterparty.tier,
                cptyCountry:s|$s.counterparty.country.countryCode,
                cptyCountryName:s|$s.counterparty.country.name,
                cptyCountryRegion:s|$s.counterparty.country.region
            ])""",

            // Q4: Confirmation → Trade → Instrument → Sector (40 props, 4-hop: ops→trading→products→refdata)
            """
            ops::Confirmation.all()->project(~[
                confirmId, status, confirmMethod, isElectronic, version,
                sentDate, affirmedDate, daysOutstanding, isOverdue, counterpartyRef,
                trdId:c|$c.trade.tradeId, trdDate:c|$c.trade.tradeDate,
                trdQty:c|$c.trade.quantity, trdPrice:c|$c.trade.price,
                trdSide:c|$c.trade.side, trdStatus:c|$c.trade.status,
                trdCcy:c|$c.trade.currency, trdComm:c|$c.trade.commission,
                trdVenue:c|$c.trade.executionVenue, trdIsBlock:c|$c.trade.isBlock,
                instrId:c|$c.trade.instrument.instrumentId,
                instrName:c|$c.trade.instrument.name,
                instrTicker:c|$c.trade.instrument.ticker,
                instrAsset:c|$c.trade.instrument.assetClass,
                instrCcy:c|$c.trade.instrument.currency,
                instrIsin:c|$c.trade.instrument.isin,
                instrType:c|$c.trade.instrument.productType,
                instrActive:c|$c.trade.instrument.isActive,
                instrNotional:c|$c.trade.instrument.notionalAmount,
                instrMaturity:c|$c.trade.instrument.maturityDate,
                secName:c|$c.trade.instrument.sector.name,
                secGics:c|$c.trade.instrument.sector.gicsCode,
                secLevel:c|$c.trade.instrument.sector.level,
                secActive:c|$c.trade.instrument.sector.isActive,
                cptyId:c|$c.trade.counterparty.counterpartyId,
                cptyName:c|$c.trade.counterparty.legalName,
                cptyLei:c|$c.trade.counterparty.lei,
                cptyType:c|$c.trade.counterparty.counterpartyType,
                cptyDomicile:c|$c.trade.counterparty.domicile,
                cptyCreditRating:c|$c.trade.counterparty.creditRating
            ])""",

            // Q5: SalesCredit → Trade → Book → Desk (42 props, 4-hop: sales→trading→positions→org)
            """
            sales::SalesCredit.all()->project(~[
                creditId, creditAmount, creditDate, creditType, currency,
                splitPct, status, isOverride, overrideBy, originalAmount,
                region, productArea,
                trdId:sc|$sc.trade.tradeId, trdDate:sc|$sc.trade.tradeDate,
                trdQty:sc|$sc.trade.quantity, trdPrice:sc|$sc.trade.price,
                trdNotional:sc|$sc.trade.notional, trdCcy:sc|$sc.trade.currency,
                trdSide:sc|$sc.trade.side, trdStatus:sc|$sc.trade.status,
                bkId:sc|$sc.trade.book.bookId, bkName:sc|$sc.trade.book.name,
                bkCcy:sc|$sc.trade.book.currency, bkType:sc|$sc.trade.book.bookType,
                bkStrategy:sc|$sc.trade.book.strategy, bkLegalEntity:sc|$sc.trade.book.legalEntity,
                bkActive:sc|$sc.trade.book.isActive,
                deskId:sc|$sc.trade.book.desk.deskId, deskName:sc|$sc.trade.book.desk.name,
                deskType:sc|$sc.trade.book.desk.deskType,
                deskAsset:sc|$sc.trade.book.desk.assetClass,
                deskRegion:sc|$sc.trade.book.desk.region,
                deskActive:sc|$sc.trade.book.desk.isActive,
                instrId:sc|$sc.trade.instrument.instrumentId,
                instrName:sc|$sc.trade.instrument.name,
                instrTicker:sc|$sc.trade.instrument.ticker,
                instrAsset:sc|$sc.trade.instrument.assetClass,
                instrCcy:sc|$sc.trade.instrument.currency,
                instrType:sc|$sc.trade.instrument.productType,
                cptyId:sc|$sc.trade.counterparty.counterpartyId,
                cptyName:sc|$sc.trade.counterparty.legalName,
                cptyLei:sc|$sc.trade.counterparty.lei
            ])""",

            // Q6: Greeks → Position → Instrument + Instrument (44 props, 3-hop: risk→positions→products)
            """
            risk::Greeks.all()->project(~[
                greeksId, calcDate, delta, gamma, vega, theta, rho, charm, vanna, volga,
                currency, model, isEOD, underlyingPrice,
                posId:g|$g.position.positionId, posQty:g|$g.position.quantity,
                posMarketVal:g|$g.position.marketValue, posCcy:g|$g.position.currency,
                posCostBasis:g|$g.position.costBasis, posAvgCost:g|$g.position.averageCost,
                posDirection:g|$g.position.direction, posIsOpen:g|$g.position.isOpen,
                posDate:g|$g.position.positionDate, posLastUpdated:g|$g.position.lastUpdated,
                instrId:g|$g.position.instrument.instrumentId,
                instrName:g|$g.position.instrument.name,
                instrTicker:g|$g.position.instrument.ticker,
                instrAsset:g|$g.position.instrument.assetClass,
                instrCcy:g|$g.position.instrument.currency,
                instrIsin:g|$g.position.instrument.isin,
                instrType:g|$g.position.instrument.productType,
                instrActive:g|$g.position.instrument.isActive,
                instrNotional:g|$g.position.instrument.notionalAmount,
                secName:g|$g.position.instrument.sector.name,
                secGics:g|$g.position.instrument.sector.gicsCode,
                secLevel:g|$g.position.instrument.sector.level,
                gInstrId:g|$g.instrument.instrumentId,
                gInstrName:g|$g.instrument.name,
                gInstrTicker:g|$g.instrument.ticker,
                gInstrAsset:g|$g.instrument.assetClass
            ])""",

            // Q7: TradeReport → Trade → Instrument + Book + Counterparty (46 props, 4-hop)
            """
            regulatory::TradeReport.all()->project(~[
                reportId, reportType, status, submissionDate, reportingEntity,
                regime, uti, usi, reportingCounterparty,
                rejectReason, retryCount, isLateReport, dueDate,
                trdId:r|$r.trade.tradeId, trdDate:r|$r.trade.tradeDate,
                trdQty:r|$r.trade.quantity, trdPrice:r|$r.trade.price,
                trdNotional:r|$r.trade.notional, trdSide:r|$r.trade.side,
                trdStatus:r|$r.trade.status, trdCcy:r|$r.trade.currency,
                trdComm:r|$r.trade.commission, trdVenue:r|$r.trade.executionVenue,
                trdIsBlock:r|$r.trade.isBlock,
                instrId:r|$r.trade.instrument.instrumentId,
                instrName:r|$r.trade.instrument.name,
                instrTicker:r|$r.trade.instrument.ticker,
                instrAsset:r|$r.trade.instrument.assetClass,
                instrCcy:r|$r.trade.instrument.currency,
                instrIsin:r|$r.trade.instrument.isin,
                instrType:r|$r.trade.instrument.productType,
                instrActive:r|$r.trade.instrument.isActive,
                bkId:r|$r.trade.book.bookId, bkName:r|$r.trade.book.name,
                bkCcy:r|$r.trade.book.currency, bkType:r|$r.trade.book.bookType,
                bkStrategy:r|$r.trade.book.strategy,
                cptyId:r|$r.trade.counterparty.counterpartyId,
                cptyName:r|$r.trade.counterparty.legalName,
                cptyLei:r|$r.trade.counterparty.lei,
                cptyType:r|$r.trade.counterparty.counterpartyType,
                cptyDomicile:r|$r.trade.counterparty.domicile,
                deskId:r|$r.trade.book.desk.deskId,
                deskName:r|$r.trade.book.desk.name,
                deskRegion:r|$r.trade.book.desk.region,
                deskAsset:r|$r.trade.book.desk.assetClass,
                deskActive:r|$r.trade.book.desk.isActive
            ])""",

            // Q8: CollateralAgreement → Counterparty → Country (40 props, 3-hop)
            """
            collateral::CollateralAgreement.all()->project(~[
                agreementId, agreementType, threshold, minimumTransferAmount, currency,
                effectiveDate, terminationDate, independentAmount,
                roundingAmount, valuationFrequency, disputeResolutionDays, isActive,
                eligibleCollateral, governingLaw,
                cptyId:ca|$ca.counterparty.counterpartyId,
                cptyName:ca|$ca.counterparty.legalName,
                cptyLei:ca|$ca.counterparty.lei,
                cptyType:ca|$ca.counterparty.counterpartyType,
                cptyDomicile:ca|$ca.counterparty.domicile,
                cptyCreditRating:ca|$ca.counterparty.creditRating,
                cptyRatingAgency:ca|$ca.counterparty.ratingAgency,
                cptyTier:ca|$ca.counterparty.tier,
                cptyIndustry:ca|$ca.counterparty.industry,
                cptyIsActive:ca|$ca.counterparty.isActive,
                cptyOnboard:ca|$ca.counterparty.onboardDate,
                countryCode:ca|$ca.counterparty.country.countryCode,
                countryName:ca|$ca.counterparty.country.name,
                countryRegion:ca|$ca.counterparty.country.region,
                countryIso3:ca|$ca.counterparty.country.iso3Code,
                countrySubRegion:ca|$ca.counterparty.country.subRegion,
                countryContinent:ca|$ca.counterparty.country.continent,
                countryCapital:ca|$ca.counterparty.country.capitalCity,
                countryTimezone:ca|$ca.counterparty.country.timeZone,
                countryGdp:ca|$ca.counterparty.country.gdp,
                countryPopulation:ca|$ca.counterparty.country.population,
                countryIsEU:ca|$ca.counterparty.country.isEU,
                countryIsOECD:ca|$ca.counterparty.country.isOECD
            ])""",

            // Q9: Position → Instrument → Sector + Currency (40 props, 3-hop)
            """
            positions::Position.all()->project(~[
                positionId, quantity, averageCost, marketValue, unrealizedPnL, realizedPnL,
                currency, positionDate, direction, costBasis, accruedInterest, lastUpdated,
                isOpen, settlementDate,
                instrId:p|$p.instrument.instrumentId, instrName:p|$p.instrument.name,
                instrTicker:p|$p.instrument.ticker, instrAsset:p|$p.instrument.assetClass,
                instrCcy:p|$p.instrument.currency, instrIsin:p|$p.instrument.isin,
                instrType:p|$p.instrument.productType, instrActive:p|$p.instrument.isActive,
                instrNotional:p|$p.instrument.notionalAmount,
                instrMaturity:p|$p.instrument.maturityDate,
                instrListing:p|$p.instrument.listingDate,
                instrDescription:p|$p.instrument.description,
                secName:p|$p.instrument.sector.name,
                secGics:p|$p.instrument.sector.gicsCode,
                secLevel:p|$p.instrument.sector.level,
                secActive:p|$p.instrument.sector.isActive,
                secDescription:p|$p.instrument.sector.description,
                ccyCode:p|$p.instrument.instrumentCurrency.code,
                ccyName:p|$p.instrument.instrumentCurrency.name,
                ccyMinor:p|$p.instrument.instrumentCurrency.minorUnit,
                ccySymbol:p|$p.instrument.instrumentCurrency.symbol,
                ccyActive:p|$p.instrument.instrumentCurrency.isActive,
                ccyRegion:p|$p.instrument.instrumentCurrency.region,
                ccyMajor:p|$p.instrument.instrumentCurrency.isMajor,
                ccyCentralBank:p|$p.instrument.instrumentCurrency.centralBank
            ])""",

            // Q10: Wide filter + sort + limit with cross-domain nav (21 props)
            """
            trading::Trade.all()
                ->filter({t|$t.status == 'EXECUTED'})
                ->project(~[
                    tradeId, tradeDate, quantity, price, notional, side, currency,
                    commission, fees, executionVenue, isBlock,
                    instrName:t|$t.instrument.name, instrAsset:t|$t.instrument.assetClass,
                    instrCcy:t|$t.instrument.currency, instrIsin:t|$t.instrument.isin,
                    bkName:t|$t.book.name, bkCcy:t|$t.book.currency,
                    deskName:t|$t.book.desk.name, deskRegion:t|$t.book.desk.region,
                    cptyName:t|$t.counterparty.legalName, cptyLei:t|$t.counterparty.lei
                ])
                ->sort(~notional->descending())
                ->limit(100)""",

            // Q11: THE MONSTER — 9 disjoint JOINs from Trade across 6 domains, 60 props
            // Trade → Instrument → {Sector, Currency, Exchange} + Book → Desk + Trader + Counterparty → Country
            """
            trading::Trade.all()->project(~[
                tradeId, tradeDate, settlementDate, quantity, price, notional, side, status,
                tradeType, currency, commission, fees, executionVenue, isBlock, createdTime, lastModifiedTime,
                instrId:t|$t.instrument.instrumentId, instrName:t|$t.instrument.name,
                instrTicker:t|$t.instrument.ticker, instrAsset:t|$t.instrument.assetClass,
                instrCcy:t|$t.instrument.currency, instrIsin:t|$t.instrument.isin,
                instrCusip:t|$t.instrument.cusip, instrType:t|$t.instrument.productType,
                secName:t|$t.instrument.sector.name, secGics:t|$t.instrument.sector.gicsCode,
                secLevel:t|$t.instrument.sector.level, secActive:t|$t.instrument.sector.isActive,
                secDesc:t|$t.instrument.sector.description,
                ccyCode:t|$t.instrument.instrumentCurrency.code,
                ccyName:t|$t.instrument.instrumentCurrency.name,
                ccySymbol:t|$t.instrument.instrumentCurrency.symbol,
                ccyRegion:t|$t.instrument.instrumentCurrency.region,
                ccyMajor:t|$t.instrument.instrumentCurrency.isMajor,
                ccyCentralBank:t|$t.instrument.instrumentCurrency.centralBank,
                exchMic:t|$t.instrument.exchange.mic,
                exchName:t|$t.instrument.exchange.name,
                exchCity:t|$t.instrument.exchange.city,
                exchTimezone:t|$t.instrument.exchange.timezone,
                exchIsElectronic:t|$t.instrument.exchange.isElectronic,
                exchRegulator:t|$t.instrument.exchange.regulatoryBody,
                bkId:t|$t.book.bookId, bkName:t|$t.book.name,
                bkCcy:t|$t.book.currency, bkType:t|$t.book.bookType,
                bkStrategy:t|$t.book.strategy, bkPnlYTD:t|$t.book.pnlYTD,
                deskId:t|$t.book.desk.deskId, deskName:t|$t.book.desk.name,
                deskType:t|$t.book.desk.deskType, deskAsset:t|$t.book.desk.assetClass,
                deskRegion:t|$t.book.desk.region, deskCostCenter:t|$t.book.desk.costCenter,
                traderId:t|$t.trader.traderId, traderFirst:t|$t.trader.firstName,
                traderLast:t|$t.trader.lastName, traderBadge:t|$t.trader.badge,
                traderSeniority:t|$t.trader.seniority, traderSpecialization:t|$t.trader.specialization,
                cptyId:t|$t.counterparty.counterpartyId, cptyName:t|$t.counterparty.legalName,
                cptyLei:t|$t.counterparty.lei, cptyType:t|$t.counterparty.counterpartyType,
                cptyCountryCode:t|$t.counterparty.country.countryCode,
                cptyCountryName:t|$t.counterparty.country.name,
                cptyCountryRegion:t|$t.counterparty.country.region,
                cptyCountryContinent:t|$t.counterparty.country.continent,
                cptyCountryIsEU:t|$t.counterparty.country.isEU
            ])""",
        };

        System.out.println("\n=== QUERY PLAN GENERATION ===");
        int passed = 0, failed = 0;
        long queryStartAll = System.nanoTime();
        for (int q = 0; q < queries.length; q++) {
            String query = queries[q];
            String phase = "parse";
            try {
                long qStart = System.nanoTime();
                var vs = com.gs.legend.parser.PureParser.parseQuery(query);
                long parseUs = (System.nanoTime() - qStart) / 1_000;
                phase = "typeCheck";
                long t = System.nanoTime();
                var unit = new com.gs.legend.compiler.TypeChecker(modelCtx).check(vs);
                long typeUs = (System.nanoTime() - t) / 1_000;
                phase = "resolve";
                t = System.nanoTime();
                var storeRes = new com.gs.legend.compiler.MappingResolver(
                        unit, normalizedMapping, builder).resolve();
                long resolveUs = (System.nanoTime() - t) / 1_000;
                phase = "planGen";
                t = System.nanoTime();
                var plan = new com.gs.legend.plan.PlanGenerator(
                        unit, dialect, storeRes).generate();
                long planUs = (System.nanoTime() - t) / 1_000;
                long totalUs = (System.nanoTime() - qStart) / 1_000;
                assertNotNull(plan.sql(), "Query " + q + " produced null SQL");
                assertFalse(plan.sql().isBlank(), "Query " + q + " produced blank SQL");
                System.out.printf("  PASS q%d: SQL(%d chars) %,dμs [parse=%,d type=%,d resolve=%,d plan=%,d]%n",
                        q, plan.sql().length(), totalUs, parseUs, typeUs, resolveUs, planUs);
                passed++;
            } catch (Exception e) {
                System.out.println("  FAIL q" + q + " [" + phase + "]: "
                        + e.getClass().getSimpleName() + ": " + e.getMessage());
                System.out.println("    Query: " + query);
                var st = e.getStackTrace();
                for (int si = 0; si < Math.min(5, st.length); si++) {
                    System.out.println("      " + st[si]);
                }
                failed++;
            }
        }
        long queryMs = (System.nanoTime() - queryStartAll) / 1_000_000;
        long totalMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("\n" + queries.length + " queries: " + passed + " passed, " + failed + " failed in " + queryMs + " ms");
        System.out.println("TOTAL: " + totalMs + " ms");
        assertEquals(0, failed, failed + " queries failed out of " + queries.length);
    }
}
