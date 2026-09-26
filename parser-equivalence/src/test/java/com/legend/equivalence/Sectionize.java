package com.legend.equivalence;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * The MECHANICAL sectionize transform (sections normalization): given a
 * sectionless Pure snippet, compute where the engine's {@code ###Section}
 * headers belong — one insertion at each element whose kind leaves the
 * current section, positions taken from OUR lenient parse. Shared by the
 * probe (ZSectionizeProbe (deleted 2026-08-12, in git history)), the source rewriter
 * ({@link ZSectionNormalizeRewrite}) and the own-corpus ratchet
 * ({@link OwnCorpusConformanceTest}).
 */
final class Sectionize {

    private Sectionize() {
    }

    /** The engine section that owns an element kind ({@code Pure} = the
     *  default section — no header needed). */
    static String sectionOf(com.legend.model.PackageableElement e) {
        return switch (e) {
            case com.legend.model.DatabaseDefinition d -> "Relational";
            case com.legend.model.MappingDefinition m -> "Mapping";
            case com.legend.model.CleanSheetMappingDefinition m -> "Mapping";
            case com.legend.model.LegacyMappingDefinition m -> "Mapping";
            case com.legend.model.RuntimeDefinition r -> "Runtime";
            case com.legend.model.ConnectionDefinition c -> "Connection";
            case com.legend.model.ModelConnectionDefinition c -> "Connection";
            case com.legend.model.ModelChainConnectionDefinition c -> "Connection";
            case com.legend.model.ServiceDefinition s -> "Service";
            case com.legend.model.ExecutionEnvironmentDefinition s -> "Service";
            case com.legend.model.DataSpaceDefinition d -> "DataSpace";
            // section carriers already know their section — never a move
            case com.legend.model.GenericSectionElementDefinition g ->
                    g.section();
            case com.legend.model.OpaqueElementDefinition o -> o.sectionName();
            case com.legend.model.PersistenceDefinition p -> "Persistence";
            case com.legend.model.PersistenceContextDefinition p ->
                    "Persistence";
            case com.legend.model.SnowflakeActivatorDefinition s ->
                    "Snowflake";
            default -> "Pure";
        };
    }

    /** One header insertion: {@code section}'s header belongs at char
     *  {@code offset} of the decoded snippet. */
    record Insertion(int offset, String section) {
    }

    /**
     * The insertion plan for a snippet, in source order — SECTION-AWARE:
     * existing {@code ###} headers count (an element already inside its
     * right section needs nothing; an existing header resets the running
     * scope, exactly as it will at parse time). Returns null when the
     * snippet is not sectionizable (does not lenient-parse, or an element
     * has no recorded offset); an EMPTY list means every element already
     * sits in its section.
     */
    static @com.legend.base.Nullable List<Insertion> plan(String text) {
        com.legend.model.ParsedModel pm;
        java.util.List<com.legend.lexer.TokenStream.SectionHeader> headers;
        try {
            var tokens = com.legend.lexer.Lexer.tokenize(text);
            headers = tokens.sectionHeaders();
            pm = Surfaces.platform(tokens);
        } catch (Throwable t) {
            return null;
        }
        List<com.legend.model.PackageableElement> els =
                new ArrayList<>(pm.elements());
        Map<String, Integer> offs = pm.elementOffsets();
        for (com.legend.model.PackageableElement e : els) {
            if (offs.get(e.qualifiedName()) == null) {
                return null;
            }
        }
        els.sort(Comparator.comparingInt(e -> offs.get(e.qualifiedName())));
        List<Insertion> inserts = new ArrayList<>();
        String current = "Pure";
        int nextHeader = 0;
        for (com.legend.model.PackageableElement e : els) {
            int off = offs.get(e.qualifiedName());
            while (nextHeader < headers.size()
                    && headers.get(nextHeader).contentStartOffset() <= off) {
                current = headers.get(nextHeader).name();   // real header wins
                nextHeader++;
            }
            String want = sectionOf(e);
            if (!want.equals(current)) {
                inserts.add(new Insertion(off, want));
                current = want;
            }
        }
        return inserts;
    }

    /** {@link #plan(String)} applied in decoded space: each header on its
     *  own line ({@code \n###X\n}). Null/identity mirror plan()'s verdicts. */
    static @com.legend.base.Nullable String apply(String text) {
        List<Insertion> inserts = plan(text);
        if (inserts == null) {
            return null;
        }
        if (inserts.isEmpty()) {
            return text;
        }
        StringBuilder sb = new StringBuilder(text);
        for (int i = inserts.size() - 1; i >= 0; i--) {
            sb.insert(inserts.get(i).offset(),
                    "\n###" + inserts.get(i).section() + "\n");
        }
        return sb.toString();
    }
}
