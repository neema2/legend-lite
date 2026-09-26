package com.legend.warehouse.server;

import com.legend.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * The statements DuckDB does not know, which manage who may read what, in Postgres's spelling:
 *
 * <pre>
 *   CREATE ROLE r                                  DROP ROLE r
 *   GRANT SELECT ON [TABLE|VIEW|FUNCTION] name TO grantee
 *   GRANT SELECT ON SCHEMA [catalog.]schema TO grantee
 *   REVOKE SELECT ON ... FROM grantee
 *   GRANT r TO member                              REVOKE r FROM member
 *   SHOW GRANTS
 * </pre>
 *
 * A name is {@code name}, {@code schema.name} or {@code catalog.schema.name} (the statement's catalog
 * and schema {@code main} by default); identifiers may be double-quoted. A grantee or member is a user
 * or a role. Anything else is not one of these, and goes to DuckDB.
 */
final class AdminStatements {

    private AdminStatements() {
    }

    sealed interface Admin permits CreateRole, DropRole, Select, Membership, ShowGrants {
    }

    record CreateRole(String role) implements Admin {
    }

    record DropRole(String role) implements Admin {
    }

    /** GRANT (or REVOKE, {@code grant} false) SELECT on an object, or a schema ({@code name} empty). */
    record Select(boolean grant, Grants.Grant target) implements Admin {
    }

    /** GRANT (or REVOKE) a role to (from) a user or a role. */
    record Membership(boolean grant, String role, String member) implements Admin {
    }

    record ShowGrants() implements Admin {
    }

    /** The admin statement {@code sql} is, or null when it is not one (DuckDB's to run). */
    static @Nullable Admin parse(String sql, String catalog) {
        List<String> t = tokens(sql);
        if (t.isEmpty()) return null;
        String first = up(t.get(0));
        try {
            switch (first) {
                case "CREATE", "DROP" -> {
                    if (t.size() == 3 && up(t.get(1)).equals("ROLE")) {
                        return first.equals("CREATE") ? new CreateRole(t.get(2)) : new DropRole(t.get(2));
                    }
                    return null;
                }
                case "SHOW" -> {
                    return t.size() == 2 && up(t.get(1)).equals("GRANTS") ? new ShowGrants() : null;
                }
                case "GRANT", "REVOKE" -> {
                    boolean grant = first.equals("GRANT");
                    String to = grant ? "TO" : "FROM";
                    if (t.size() >= 6 && up(t.get(1)).equals("SELECT") && up(t.get(2)).equals("ON")) {
                        int i = 3;
                        boolean schema = false;
                        String kind = up(t.get(i));
                        if (kind.equals("SCHEMA")) {
                            schema = true;
                            i++;
                        } else if (kind.equals("TABLE") || kind.equals("VIEW") || kind.equals("FUNCTION")) {
                            i++;
                        }
                        List<String> name = new ArrayList<>();
                        name.add(t.get(i++));
                        while (i < t.size() && t.get(i).equals(".")) {
                            name.add(t.get(i + 1));
                            i += 2;
                        }
                        if (i + 2 != t.size() || !up(t.get(i)).equals(to)) return null;
                        String grantee = t.get(i + 1);
                        Grants.Grant g;
                        if (schema) {
                            if (name.size() > 2) return null;
                            g = name.size() == 2 ? new Grants.Grant(name.get(0), name.get(1), "", grantee)
                                    : new Grants.Grant(catalog, name.get(0), "", grantee);
                        } else {
                            g = switch (name.size()) {
                                case 1 -> new Grants.Grant(catalog, "main", name.get(0), grantee);
                                case 2 -> new Grants.Grant(catalog, name.get(0), name.get(1), grantee);
                                case 3 -> new Grants.Grant(name.get(0), name.get(1), name.get(2), grantee);
                                default -> null;
                            };
                            if (g == null) return null;
                        }
                        return new Select(grant, g);
                    }
                    if (t.size() == 4 && up(t.get(2)).equals(to)) return new Membership(grant, t.get(1), t.get(3));
                    return null;
                }
                default -> {
                    return null;
                }
            }
        } catch (IndexOutOfBoundsException malformed) {
            return null;
        }
    }

    private static String up(String s) {
        return s.toUpperCase(Locale.ROOT);
    }

    /** Words, double-quoted identifiers (quotes removed, "" unescaped) and dots; a final ';' dropped. */
    static List<String> tokens(String sql) {
        List<String> out = new ArrayList<>();
        String s = sql.strip();
        if (s.endsWith(";")) s = s.substring(0, s.length() - 1);
        int i = 0;
        while (i < s.length()) {
            char c = s.charAt(i);
            if (Character.isWhitespace(c)) {
                i++;
            } else if (c == '.') {
                out.add(".");
                i++;
            } else if (c == '"') {
                StringBuilder b = new StringBuilder();
                i++;
                while (i < s.length()) {
                    if (s.charAt(i) == '"') {
                        if (i + 1 < s.length() && s.charAt(i + 1) == '"') {
                            b.append('"');
                            i += 2;
                            continue;
                        }
                        break;
                    }
                    b.append(s.charAt(i++));
                }
                i++;
                out.add(b.toString());
            } else if (Character.isLetterOrDigit(c) || c == '_') {
                int start = i;
                while (i < s.length() && (Character.isLetterOrDigit(s.charAt(i)) || s.charAt(i) == '_')) i++;
                out.add(s.substring(start, i));
            } else {
                return List.of("?");   // anything else: not one of ours
            }
        }
        return out;
    }
}
