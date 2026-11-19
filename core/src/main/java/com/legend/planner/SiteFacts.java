// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.planner;

/**
 * AN UPSTREAM MODE'S WORDS OF A STATEMENT'S RAISE SITES, A TWO-FORM HOOK OF THE PLAN STYLE (K1 of the presentation
 * program, S1, 2026-11-19; {@code testplan.PlanStyleFacets.TWO_FORMS}): the edge's data a raise site carries
 * ({@code ExecutionPlan.Site#extensions}), planned with each site a render wrote, a statement's or a CSV reading's. The
 * product's form plans none ({@link PlanMaker#NO_SITE_FACTS}): a site says
 * lite's sentence ({@code ExecutionPlan.Site#message}) and nothing else. The compatibility edge's form words each site
 * it can word at planning, from the literal operands its render wrote ({@code edge.SiteWords#planned}). Stateless: the
 * words are the plan's data, planned where the statement is (until then a recorder minted per planning call kept them
 * beside the plan, offered each statement through {@code PlanStyle.siteWords}: both deleted).
 */
public interface SiteFacts {

    /** The edge's records of {@code site}, one raise site a render wrote; none where it words none. */
    java.util.List<com.legend.executionplan.Extension> of(com.legend.sql.dialect.RaiseSite site);
}
