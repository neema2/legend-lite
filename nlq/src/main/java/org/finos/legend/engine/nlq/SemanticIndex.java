package org.finos.legend.engine.nlq;

import org.finos.legend.pure.dsl.definition.PureModelBuilder;
import org.finos.legend.pure.m3.Association;
import org.finos.legend.pure.m3.Property;
import org.finos.legend.pure.m3.PureClass;

import java.util.*;
import java.util.stream.Collectors;

/**
 * TF-IDF based semantic index for routing natural language queries to relevant
 * Pure classes. Builds an index at model-load time and scores classes against
 * NLQ queries at query time.
 *
 * Scoring formula:
 *   score = 0.4 * tfidf(query, description + synonyms)
 *         + 0.3 * tfidf(query, propertyTerms)
 *         + 0.2 * exactMatch(query, exampleQuestions)
 *         + 0.1 * importanceBoost
 */
public class SemanticIndex {

    private final List<ClassIndexEntry> entries = new ArrayList<>();
    private final Map<String, Double> idf = new HashMap<>();
    private int totalDocuments = 0;

    // ==================== Index Entry ====================

    /**
     * An indexed entry for a single class, containing all metadata
     * extracted from NLQ annotations.
     */
    public record ClassIndexEntry(
            String qualifiedName,
            String description,
            List<String> synonyms,
            String businessDomain,
            String importance,
            List<String> propertyTerms,
            List<String> exampleQuestions,
            // Pre-tokenized for scoring
            List<String> descriptionTokens,
            List<String> propertyTokens
    ) {}

    // ==================== Retrieval Result ====================

    public record RetrievalResult(
            String qualifiedName,
            double score,
            double descriptionScore,
            double propertyScore,
            double exampleScore,
            double importanceBoost
    ) implements Comparable<RetrievalResult> {
        @Override
        public int compareTo(RetrievalResult other) {
            return Double.compare(other.score, this.score); // descending
        }
    }

    // ==================== Build Index ====================

    /**
     * Builds the index from a PureModelBuilder. Extracts NLQ metadata
     * from class and property tagged values.
     */
    public void buildIndex(PureModelBuilder modelBuilder) {
        entries.clear();
        idf.clear();

        Map<String, PureClass> allClasses = modelBuilder.getAllClasses();
        totalDocuments = allClasses.size();

        // Phase 1: Build entries and collect document frequencies
        Map<String, Integer> docFreq = new HashMap<>();

        for (PureClass pc : allClasses.values()) {
            ClassIndexEntry entry = buildEntry(pc);
            entries.add(entry);

            // Count document frequency for IDF
            Set<String> uniqueTokens = new HashSet<>();
            uniqueTokens.addAll(entry.descriptionTokens());
            uniqueTokens.addAll(entry.propertyTokens());
            for (String token : uniqueTokens) {
                docFreq.merge(token, 1, Integer::sum);
            }
        }

        // Phase 2: Compute IDF
        for (var entry : docFreq.entrySet()) {
            double idfVal = Math.log((double) totalDocuments / (1 + entry.getValue()));
            idf.put(entry.getKey(), idfVal);
        }
    }

    private ClassIndexEntry buildEntry(PureClass pc) {
        String description = getTagValue(pc, "description");
        if (description == null) {
            // Fallback to doc.doc
            description = pc.getTagValue("doc", "doc");
        }
        if (description == null) {
            description = "";
        }

        String synonymsStr = getTagValue(pc, "synonyms");
        List<String> synonyms = synonymsStr != null
                ? Arrays.asList(synonymsStr.split(",\\s*"))
                : List.of();

        String businessDomain = getTagValue(pc, "businessDomain");
        String importance = getTagValue(pc, "importance");

        String exampleQuestionsStr = getTagValue(pc, "exampleQuestions");
        List<String> exampleQuestions = exampleQuestionsStr != null
                ? Arrays.asList(exampleQuestionsStr.split("\\|"))
                : List.of();

        // Build property terms: property name + description + synonyms
        List<String> propertyTerms = new ArrayList<>();
        for (Property prop : pc.allProperties()) {
            propertyTerms.add(prop.name());

            String propDesc = prop.getTagValue(NLQ_PROFILE, "description");
            if (propDesc != null) {
                propertyTerms.add(propDesc);
            }

            String propSyn = prop.getTagValue(NLQ_PROFILE, "synonyms");
            if (propSyn != null) {
                propertyTerms.addAll(Arrays.asList(propSyn.split(",\\s*")));
            }

            String propUnit = prop.getTagValue(NLQ_PROFILE, "unit");
            if (propUnit != null) {
                propertyTerms.add(propUnit);
            }

            String propSample = prop.getTagValue(NLQ_PROFILE, "sampleValues");
            if (propSample != null) {
                propertyTerms.addAll(Arrays.asList(propSample.split(",\\s*")));
            }
        }

        // Build combined description text: name + description + synonyms + displayName
        String displayName = getTagValue(pc, "displayName");
        StringBuilder descText = new StringBuilder();
        descText.append(pc.name()).append(" ");
        if (displayName != null) {
            descText.append(displayName).append(" ");
        }
        descText.append(description).append(" ");
        for (String syn : synonyms) {
            descText.append(syn).append(" ");
        }

        List<String> descriptionTokens = tokenize(descText.toString());
        List<String> propertyTokensList = tokenize(String.join(" ", propertyTerms));

        return new ClassIndexEntry(
                pc.qualifiedName(),
                description,
                synonyms,
                businessDomain != null ? businessDomain : "",
                importance != null ? importance : "medium",
                propertyTerms,
                exampleQuestions,
                descriptionTokens,
                propertyTokensList
        );
    }

    // ==================== Query ====================

    /**
     * Retrieves the top-K most relevant classes for a natural language query.
     *
     * @param query  The natural language query
     * @param topK   Maximum number of classes to return
     * @param domain Optional domain filter (null or empty to skip)
     * @return Scored and sorted retrieval results
     */
    public List<RetrievalResult> retrieve(String query, int topK, String domain) {
        List<String> queryTokens = tokenize(query);
        String queryLower = query.toLowerCase();

        List<RetrievalResult> results = new ArrayList<>();

        for (ClassIndexEntry entry : entries) {
            // Domain pre-filter
            if (domain != null && !domain.isEmpty()
                    && !entry.businessDomain().isEmpty()
                    && !entry.businessDomain().equalsIgnoreCase(domain)) {
                continue;
            }

            double descScore = tfidfScore(queryTokens, entry.descriptionTokens());
            double propScore = tfidfScore(queryTokens, entry.propertyTokens());
            double exampleScore = exampleMatchScore(queryLower, entry.exampleQuestions());
            double impBoost = importanceBoost(entry.importance());

            double totalScore = 0.4 * descScore
                    + 0.3 * propScore
                    + 0.2 * exampleScore
                    + 0.1 * impBoost;

            if (totalScore > 0) {
                results.add(new RetrievalResult(
                        entry.qualifiedName(),
                        totalScore,
                        descScore,
                        propScore,
                        exampleScore,
                        impBoost
                ));
            }
        }

        Collections.sort(results);

        // Apply score threshold: only return results with score >= 10% of top score
        // This prevents low-relevance classes from polluting results in small models
        if (!results.isEmpty()) {
            double topScore = results.get(0).score();
            double threshold = topScore * 0.10;
            results = results.stream()
                    .filter(r -> r.score() >= threshold)
                    .collect(Collectors.toList());
        }

        return results.stream().limit(topK).collect(Collectors.toList());
    }

    /**
     * Retrieves top-K classes without domain filtering.
     */
    public List<RetrievalResult> retrieve(String query, int topK) {
        return retrieve(query, topK, null);
    }

    // ==================== Association Expansion ====================

    /**
     * Expands a set of retrieved class names with associated classes
     * reachable within the given hop count.
     *
     * @param classNames    The initially retrieved class names
     * @param modelBuilder  The model builder for association lookup
     * @param maxHops       Maximum association hops (typically 1-2)
     * @return Expanded set including associated classes
     */
    public Set<String> expandWithAssociations(
            Set<String> classNames, PureModelBuilder modelBuilder, int maxHops) {

        Set<String> expanded = new HashSet<>(classNames);
        Set<String> frontier = new HashSet<>(classNames);

        Map<String, Association> allAssociations = modelBuilder.getAllAssociations();

        for (int hop = 0; hop < maxHops; hop++) {
            Set<String> newFrontier = new HashSet<>();
            for (Association assoc : allAssociations.values()) {
                String class1 = assoc.property1().targetClass();
                String class2 = assoc.property2().targetClass();

                if (frontier.contains(class1) && !expanded.contains(class2)) {
                    newFrontier.add(class2);
                }
                if (frontier.contains(class2) && !expanded.contains(class1)) {
                    newFrontier.add(class1);
                }
            }
            expanded.addAll(newFrontier);
            frontier = newFrontier;

            if (frontier.isEmpty()) break;
        }

        return expanded;
    }

    // ==================== Scoring Internals ====================

    private double tfidfScore(List<String> queryTokens, List<String> docTokens) {
        if (queryTokens.isEmpty() || docTokens.isEmpty()) {
            return 0.0;
        }

        // Term frequency in document
        Map<String, Integer> tf = new HashMap<>();
        for (String token : docTokens) {
            tf.merge(token, 1, Integer::sum);
        }

        double score = 0.0;
        for (String queryToken : queryTokens) {
            int termFreq = tf.getOrDefault(queryToken, 0);
            if (termFreq > 0) {
                double idfVal = idf.getOrDefault(queryToken, 0.0);
                // Log-normalized TF * IDF
                score += (1 + Math.log(termFreq)) * idfVal;
            }
        }

        // Normalize by document length to avoid bias toward large documents
        return score / Math.sqrt(docTokens.size());
    }

    private double exampleMatchScore(String queryLower, List<String> exampleQuestions) {
        if (exampleQuestions.isEmpty()) {
            return 0.0;
        }

        double bestScore = 0.0;
        for (String example : exampleQuestions) {
            String exLower = example.toLowerCase().trim();
            if (exLower.isEmpty()) continue;

            // Compute token overlap (Jaccard-like similarity)
            Set<String> querySet = new HashSet<>(tokenize(queryLower));
            Set<String> exSet = new HashSet<>(tokenize(exLower));

            Set<String> intersection = new HashSet<>(querySet);
            intersection.retainAll(exSet);

            Set<String> union = new HashSet<>(querySet);
            union.addAll(exSet);

            if (!union.isEmpty()) {
                double jaccard = (double) intersection.size() / union.size();
                bestScore = Math.max(bestScore, jaccard);
            }
        }

        return bestScore;
    }

    private double importanceBoost(String importance) {
        return switch (importance != null ? importance.toLowerCase() : "medium") {
            case "high" -> 1.0;
            case "medium" -> 0.5;
            case "low" -> 0.1;
            default -> 0.5;
        };
    }

    // ==================== Tokenization ====================

    /**
     * Tokenizes text into lowercase terms, splitting on whitespace,
     * punctuation, and camelCase boundaries. Removes stop words.
     */
    public static List<String> tokenize(String text) {
        if (text == null || text.isBlank()) {
            return List.of();
        }

        // Normalize common financial abbreviations before splitting
        String normalized = text;
        for (var entry : ABBREVIATIONS.entrySet()) {
            normalized = normalized.replaceAll("(?i)" + entry.getKey(), entry.getValue());
        }

        // Split camelCase: "tradeDate" -> "trade Date" -> "trade date"
        String expanded = normalized.replaceAll("([a-z])([A-Z])", "$1 $2");

        // Split on non-alphanumeric
        String[] raw = expanded.toLowerCase().split("[^a-z0-9]+");

        List<String> tokens = new ArrayList<>();
        for (String token : raw) {
            if (!token.isEmpty() && token.length() > 1 && !STOP_WORDS.contains(token)) {
                tokens.add(token);
            }
        }
        return tokens;
    }

    private static final Map<String, String> ABBREVIATIONS = Map.of(
            "P&L", "pnl",
            "p&l", "pnl",
            "PnL", "pnl",
            "cpty", "counterparty",
            "CPTY", "counterparty",
            "FX", "fx foreign exchange",
            "OTC", "otc over the counter",
            "YTD", "ytd year to date",
            "MTD", "mtd month to date"
    );

    private static final Set<String> STOP_WORDS = Set.of(
            "the", "is", "at", "of", "on", "in", "to", "for", "and", "or",
            "an", "as", "by", "be", "it", "do", "if", "no", "up", "so",
            "me", "my", "we", "he", "am", "are", "was", "all", "any",
            "has", "had", "how", "its", "may", "can", "did", "not",
            "but", "our", "out", "own", "say", "she", "too", "use",
            "her", "him", "his", "let", "put", "run", "set", "try",
            "who", "why", "big", "few", "get", "got", "new", "now",
            "old", "see", "way", "day", "man", "each", "find",
            "from", "have", "here", "just", "like", "long", "make",
            "many", "more", "most", "much", "must", "name", "only",
            "over", "such", "take", "than", "that", "them", "then",
            "they", "this", "time", "very", "what", "when", "with",
            "will", "your", "also", "been", "come", "done", "some",
            "show", "list", "give", "tell", "please", "want"
    );

    // ==================== Helpers ====================

    private static final String NLQ_PROFILE = "nlq::NlqProfile";

    /**
     * Gets an NLQ tagged value from a PureClass.
     */
    private String getTagValue(PureClass pc, String tagName) {
        return pc.getTagValue(NLQ_PROFILE, tagName);
    }

    /**
     * @return The number of indexed classes
     */
    public int size() {
        return entries.size();
    }

    /**
     * @return All index entries (for testing/debugging)
     */
    public List<ClassIndexEntry> getEntries() {
        return Collections.unmodifiableList(entries);
    }
}
