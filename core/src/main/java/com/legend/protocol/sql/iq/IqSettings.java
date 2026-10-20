// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.protocol.sql.iq;

import com.legend.protocol.sql.SqlAst;
import com.legend.protocol.sql.SqlTranslationException;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * THE SETTINGS VECTOR an {@code #IQ{...}#} island is read under (docs/IQ_SQL_STUDY_2026_10_13.md §9.17; IQ island,
 * 2026-10-14, the options framework): a value for every setting of the registry ({@link IqOption}), built the way IQ
 * builds a connection's -- the engine base's defaults (IQ 16's or SQL Anywhere 17's), then the database's level (its
 * creation properties, its PUBLIC options), then the client's login (a TDS login's {@code sp_tsql_environment}, IQ's
 * ODBC driver's date and time formats), then the query's level ({@link #resolve}, the one entry point for the settings
 * given), then the SET statements before the island's SELECT ({@link IqSetPrefix}, {@link #set}). Every rule of the
 * translator that depends on a setting reads it here, never a profile's name.
 *
 * <p>A setting's value is IQ's spelling of it, as {@code CONNECTION_PROPERTY} and {@code DB_PROPERTY} read it back:
 * {@code On} and {@code Off}, an integer's digits, {@code YMD}, a format as written. An option has IQ's levels, measured
 * on SQL Anywhere 17: the database's PUBLIC value, a temporary PUBLIC value ({@code SET TEMPORARY OPTION PUBLIC.x}),
 * the user's ({@code SET OPTION x}, which also replaces the connection's temporary value), the connection's temporary
 * one (a login's overrides, the query's level, {@code SET TEMPORARY OPTION x}); the first of temporary, user, temporary
 * PUBLIC and PUBLIC that is set is the value (a temporary value hides the PUBLIC one).
 */
public final class IqSettings {

    /** The engine base whose defaults the vector starts from: an IQ 16 database's (whose queries over tables IQ's engine
     *  runs) or a SQL Anywhere 17 database's (whose every query SQL Anywhere runs). */
    public enum Base { IQ16, SA17 }

    /** The client a connection is made by: one at the database's defaults (embedded SQL, dbcapi), IQ's ODBC driver (its
     *  date and time formats), or a TDS login (Open Client, jConnect: {@code CommProtocol} {@code TDS}). */
    public enum Client { DEFAULT, ODBC, TDS }

    /** Where a SET statement sets an option (IQ island, 2026-10-14, the SET statements before a SELECT). */
    public enum Scope {
        /** {@code SET TEMPORARY OPTION x}: the connection's. */
        TEMPORARY,
        /** {@code SET OPTION x}: the user's, which IQ applies to the connection at once over its temporary value. */
        USER,
        /** {@code SET OPTION PUBLIC.x}: the database's. */
        PUBLIC,
        /** {@code SET TEMPORARY OPTION PUBLIC.x}: the database's until the server stops. */
        PUBLIC_TEMPORARY
    }

    /** A SET statement's value as written: a word (an identifier, {@code ON}, {@code OFF}), a number, a string,
     *  {@code NULL}, or none ({@code SET OPTION x =}). */
    public record Value(Form form, String text) {

        /** What a value is written as. */
        public enum Form { WORD, NUMBER, STRING, NULL, NONE }

        /** As IQ's syntax writes it. */
        public String written() {
            return switch (form) {
                case STRING -> quoted(text);
                case NULL -> "NULL";
                case NONE -> "";
                case WORD, NUMBER -> text;
            };
        }
    }

    private final String preset;
    private final Base base;
    private final Client client;
    /** The database's level: its creation properties and PUBLIC values, by setting. */
    private final Map<IqOption, String> database;
    /** The PUBLIC values set temporarily ({@code SET TEMPORARY OPTION PUBLIC.x}). */
    private final Map<IqOption, String> publicTemporary;
    /** The user's values ({@code SET OPTION x}). */
    private final Map<IqOption, String> user;
    /** The connection's temporary values (a login's overrides, the query's level, {@code SET TEMPORARY OPTION x}). */
    private final Map<IqOption, String> temporary;
    /** The settings made beyond the preset, as {@code name=value}, in order: how the vector is named in messages. */
    private final List<String> changes;

    private IqSettings(String preset, Base base, Client client, Map<IqOption, String> database,
            Map<IqOption, String> publicTemporary, Map<IqOption, String> user, Map<IqOption, String> temporary,
            List<String> changes) {
        this.preset = preset;
        this.base = base;
        this.client = client;
        this.database = Collections.unmodifiableMap(database);
        this.publicTemporary = Collections.unmodifiableMap(publicTemporary);
        this.user = Collections.unmodifiableMap(user);
        this.temporary = Collections.unmodifiableMap(temporary);
        this.changes = List.copyOf(changes);
    }

    /** The settings of a connection by {@code client} to a database at {@code base}'s defaults, named {@code spelling}
     *  (a profile's name): the login's overrides are the connection's temporary values. */
    static IqSettings preset(String spelling, Base base, Client client) {
        return preset(spelling, base, client, Map.of());
    }

    /** IQ island, landing 4 (WP1), 2026-10-14: the database properties of the real SAP IQ 17.1 the {@code iq17-*}
     *  presets are (collation UTF8BIN and its charset UTF-8; case-sensitive and blank-padded as the IQ base); IQ
     *  island, 2026-10-15 (checklist 0.6, measured): and its PUBLIC options where they differ from IQ 16's defaults
     *  (tsql_outer_joins On). */
    static final Map<IqOption, String> UTF8BIN = Map.of(IqOption.COLLATION, "UTF8BIN", IqOption.CHAR_SET, "UTF-8",
            IqOption.TSQL_OUTER_JOINS, "On");

    /** {@link #preset(String, Base, Client)} for a database whose level is {@code database} (its creation properties,
     *  as {@link #resolve} reads them at the database's level) over the base's defaults. */
    static IqSettings preset(String spelling, Base base, Client client, Map<IqOption, String> database) {
        return new IqSettings(spelling, base, client, new LinkedHashMap<>(database), new LinkedHashMap<>(),
                new LinkedHashMap<>(), login(base, client), List.of());
    }

    private static Map<IqOption, String> login(Base base, Client client) {
        Map<IqOption, String> temporary = new LinkedHashMap<>();
        for (IqOption o : IqOption.values()) {
            String login = client == Client.TDS ? o.tdsLogin() : client == Client.ODBC ? o.odbcLogin() : null;
            if (login != null && o.defaultFor(base) != null) {
                temporary.put(o, login);
            }
        }
        return temporary;
    }

    private IqSettings with(Client c, Map<IqOption, String> db, Map<IqOption, String> pt, Map<IqOption, String> u,
            Map<IqOption, String> t, String change) {
        List<String> all = new ArrayList<>(changes);
        all.add(change);
        return new IqSettings(preset, base, c, db, pt, u, t, all);
    }

    /** The value of {@code option}: the connection's temporary one, else the user's, else the temporary PUBLIC one, else
     *  the database's, else the base's default; null where the base has no such setting. */
    public @com.legend.base.Nullable String value(IqOption option) {
        String v = temporary.get(option);
        if (v == null) {
            v = user.get(option);
        }
        if (v == null) {
            v = publicTemporary.get(option);
        }
        if (v == null) {
            v = database.get(option);
        }
        return v != null ? v : option.defaultFor(base);
    }

    private boolean on(IqOption option) {
        return "On".equalsIgnoreCase(value(option));
    }

    private String text(IqOption option) {
        String v = value(option);
        if (v == null) {
            throw new IllegalStateException(option.spelling() + " has no value under " + spelling());
        }
        return v;
    }

    private int integer(IqOption option) {
        return Integer.parseInt(text(option));
    }

    // ------------------------------------------------------------------ the settings given

    /** Where a setting is given. IQ island, 2026-10-14 (the options framework): the settings will come from Pure's
     *  tagged values of a platform Profile whose tags are IQ's names, on a Database element and on the enclosing
     *  function (announced separately); the parity harness gives a case's here. */
    public enum Level {
        /** The database's: its creation properties, its PUBLIC options, the protocol its clients connect by. */
        DATABASE,
        /** The query's: the connection's temporary values, after the login's. */
        QUERY
    }

    /** One setting given: where, IQ's name of it (matched without regard to case) and its value as text (a tagged
     *  value's). */
    public record Setting(Level level, String name, String value) {
    }

    /** The place of an error of the settings given (line and column 0: none in the SQL). */
    static final SqlAst.Pos SETTINGS = new SqlAst.Pos(0, 0);

    /**
     * THE ENTRY POINT: the settings of {@code preset} with {@code settings} given, by IQ's precedence -- the base's
     * defaults, the database's level, the login's overrides, the query's level -- and IQ's errors (measured on SQL
     * Anywhere 17). At the database's level: a database property and any option (a PUBLIC-only one too) become the
     * database's (an option's PUBLIC value, which the login's temporary values hide, as in IQ), {@code CommProtocol}
     * the login ({@code 'TDS'} a TDS login's overrides, {@code 'CmdSeq'} none; {@code 'TDS'} with IQ's ODBC driver
     * refused); a name of no setting is an error. At the query's level an option is the connection's temporary value;
     * a database property, {@code CommProtocol}, a name of no option, an option the base lacks, or a PUBLIC-only option
     * is IQ's error -203 (IQ sets no such temporary option). At either level an option with no effect on a SELECT (the
     * research's Tier 4) is accepted and ignored, a value the setting does not take is IQ's -201 ({@link #read}), a
     * setting given twice at one level is refused. What is not built is refused when a SELECT is read
     * ({@link #check}). Errors carry line and column 0.
     */
    public static IqSettings resolve(IqProfile preset, List<Setting> settings) {
        IqSettings s = preset.settings();
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (Setting st : settings) {
            if (!seen.add(st.level() + " " + IqLexer.lower(st.name()))) {
                throw SqlTranslationException.unsupported("the setting " + st.name() + " is given twice at the "
                        + IqLexer.lower(st.level().name()) + "'s level", SETTINGS);
            }
        }
        for (Setting st : settings) {
            if (st.level() == Level.DATABASE && IqOption.named(st.name()) == IqOption.COMM_PROTOCOL) {
                String v = s.read(IqOption.COMM_PROTOCOL, st.value(), SETTINGS);
                Client c = v.equals("TDS") ? Client.TDS : s.client == Client.TDS ? Client.DEFAULT : s.client;
                if (s.client == Client.ODBC && c == Client.TDS) {
                    throw SqlTranslationException.unsupported("CommProtocol 'TDS' with the profile " + s.preset
                            + " is not supported: IQ's ODBC driver connects by CmdSeq", SETTINGS);
                }
                s = s.with(c, new LinkedHashMap<>(s.database), new LinkedHashMap<>(s.publicTemporary),
                        new LinkedHashMap<>(s.user), login(s.base, c), change(st));
            }
        }
        for (Setting st : settings) {
            IqOption o = IqOption.named(st.name());
            if (st.level() == Level.DATABASE && o == IqOption.COMM_PROTOCOL) {
                continue;
            }
            boolean database = st.level() == Level.DATABASE;
            if (o == null) {
                if (IqOption.ignored(st.name(), s.base)) {
                    SqlTranslationException e = database ? null : s.noTemporaryLevel(st.name(), SETTINGS);
                    if (e != null) {
                        throw e;
                    }
                    ignoredValue(st.name(), new Value(Value.Form.STRING, st.value()), SETTINGS);
                    s = s.note(change(st));
                    continue;
                }
                throw database ? noSetting(st.name()) : noOption(st.name(), true, SETTINGS);
            }
            if (!(database ? o.level() == IqOption.Level.DATABASE || o.settable(s.base) : o.settable(s.base))) {
                throw database ? noSetting(st.name())
                        : o.level() == IqOption.Level.DATABASE || o.level() == IqOption.Level.CONNECTION
                        ? SqlTranslationException.invalid("'" + st.name() + "' is set at the database's level, not a"
                                + " query's: IQ sets no temporary option of that name" + IqDialect.code(-203, "42W45"),
                                SETTINGS)
                        : noOption(st.name(), true, SETTINGS);
            }
            if (!database && o.level() == IqOption.Level.PUBLIC_ONLY) {
                SqlTranslationException e = s.noTemporaryLevel(st.name(), SETTINGS);
                throw e != null ? e : SqlTranslationException.invalid("the option '" + st.name() + "' is set for"
                        + " PUBLIC only, at the database's level, never temporarily" + IqDialect.code(-203, "42W45"),
                        SETTINGS);
            }
            String v = s.read(o, st.value(), SETTINGS);
            Map<IqOption, String> db = new LinkedHashMap<>(s.database);
            Map<IqOption, String> t = new LinkedHashMap<>(s.temporary);
            if (database) {
                db.put(o, v);
            } else {
                t.put(o, v);
            }
            s = s.with(s.client, db, new LinkedHashMap<>(s.publicTemporary), new LinkedHashMap<>(s.user), t,
                    change(st));
        }
        return s.collation();
    }

    /** IQ's -200: a setting of no such name for the database. */
    private static SqlTranslationException noSetting(String name) {
        return SqlTranslationException.invalid("IQ has no setting '" + name + "'" + IqDialect.code(-200, "42W16"),
                SETTINGS);
    }

    /** {@code value} as IQ's syntax writes a string. */
    private static String quoted(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** A setting given, as the vector's name shows it: {@code database.name='value'} or {@code query.name='value'}. */
    private static String change(Setting st) {
        return IqLexer.lower(st.level().name()) + "." + st.name() + "=" + quoted(st.value());
    }

    private static boolean digit(char c) {
        return c >= '0' && c <= '9';
    }

    /** The collation and the charset agree: a database's charset follows its collation (ISO_BINENG's is
     *  ISO_8859-1:1987, UTF8BIN's UTF-8), and a collation follows a charset set alone (its binary one). */
    private IqSettings collation() {
        String coll = database.get(IqOption.COLLATION);
        String cs = database.get(IqOption.CHAR_SET);
        Map<IqOption, String> db = new LinkedHashMap<>(database);
        if (coll != null && cs == null && charsetOf(coll) != null) {
            db.put(IqOption.CHAR_SET, java.util.Objects.requireNonNull(charsetOf(coll)));
        } else if (cs != null && coll == null) {
            if (cs.equalsIgnoreCase("UTF-8")) {
                db.put(IqOption.COLLATION, "UTF8BIN");
            } else if (cs.equalsIgnoreCase("ISO_8859-1:1987")) {
                db.put(IqOption.COLLATION, "ISO_BINENG");
            }
        } else {
            return this;
        }
        return new IqSettings(preset, base, client, db, publicTemporary, user, temporary, changes);
    }

    private static @com.legend.base.Nullable String charsetOf(String collation) {
        return collation.equalsIgnoreCase("ISO_BINENG") ? "ISO_8859-1:1987"
                : collation.equalsIgnoreCase("UTF8BIN") ? "UTF-8" : null;
    }

    // ------------------------------------------------------------------ SET statements

    /**
     * These settings after {@code SET [TEMPORARY] OPTION [PUBLIC.]name = value} ({@code scope} which), IQ's rules as
     * measured on SQL Anywhere 17: a name of no option is IQ's error -203 when set temporarily, -200 for the user, and
     * for PUBLIC IQ's own new option, which no SELECT reads (so are a database property and {@code CommProtocol}: no
     * option of theirs); a PUBLIC-only option set temporarily is -203, for the user -202; an option with no effect on a
     * SELECT is accepted and ignored; a value the option does not take is -201. No value (or NULL) resets the level's
     * value: a temporary one to the user's or PUBLIC's, the user's (and the temporary one) to PUBLIC's; a reset of a
     * PUBLIC value (to the server's own default, which is no base's) is refused by name. {@code pos} places the error.
     */
    public IqSettings set(Scope scope, String name, Value value, SqlAst.Pos pos) {
        return set(scope, name, value, pos, false);
    }

    /** {@link #set(Scope, String, Value, SqlAst.Pos)}, {@code existing} for {@code SET EXISTING OPTION}: a PUBLIC name
     *  of no option is then IQ's -200 rather than a new option of the database's (measured on SQL Anywhere 17). */
    public IqSettings set(Scope scope, String name, Value value, SqlAst.Pos pos, boolean existing) {
        IqOption o = IqOption.named(name);
        boolean temporarily = scope == Scope.TEMPORARY || scope == Scope.PUBLIC_TEMPORARY;
        SqlTranslationException noTemporary = temporarily ? noTemporaryLevel(name, pos) : null;
        if (noTemporary != null) {
            throw noTemporary;
        }
        if (o == null || !o.settable(base)) {
            if (o == null && IqOption.ignored(name, base)) {
                return note(scope, name, value.form() == Value.Form.NONE || value.form() == Value.Form.NULL ? value
                        : new Value(Value.Form.STRING, ignoredValue(name, value, pos)));
            }
            if (scope == Scope.PUBLIC && !existing) {
                // IQ makes it a new option of the database's, which nothing of a SELECT's reads
                return note(scope, name, value);
            }
            throw noOption(name, temporarily, pos);
        }
        if (o.level() == IqOption.Level.PUBLIC_ONLY && scope != Scope.PUBLIC) {
            // for the user: -202, on IQ 17.1 and on the oracle's SQL Anywhere 17 databases alike (measured 2026-10-14)
            throw SqlTranslationException.invalid("the option '" + name + "' is set for PUBLIC only: SET OPTION PUBLIC."
                    + name + IqDialect.code(-202, "42W43"), pos);
        }
        Map<IqOption, String> db = new LinkedHashMap<>(database);
        Map<IqOption, String> pt = new LinkedHashMap<>(publicTemporary);
        Map<IqOption, String> u = new LinkedHashMap<>(user);
        Map<IqOption, String> t = new LinkedHashMap<>(temporary);
        String change = (scope == Scope.PUBLIC || scope == Scope.PUBLIC_TEMPORARY ? "PUBLIC." : "")
                + o.spelling() + "=" + value.written();
        if (value.form() == Value.Form.NULL || value.form() == Value.Form.NONE) {
            switch (scope) {
                case TEMPORARY -> t.remove(o);
                case USER -> {
                    u.remove(o);
                    t.remove(o);
                }
                case PUBLIC_TEMPORARY -> pt.remove(o);
                case PUBLIC -> throw SqlTranslationException.unsupported("resetting the PUBLIC value of the option '"
                        + name + "' is not supported: IQ then reads the server's own default, which is not read here",
                        pos);
            }
            return with(client, db, pt, u, t, change);
        }
        String v = read(o, value, pos);
        switch (scope) {
            case TEMPORARY -> t.put(o, v);
            case USER -> {
                u.put(o, v);
                t.remove(o);
            }
            case PUBLIC_TEMPORARY -> pt.put(o, v);
            case PUBLIC -> db.put(o, v);
        }
        return with(client, db, pt, u, t, change);
    }

    /** These settings after a T-SQL {@code SET} statement that sets {@code option} to {@code value} (IQ's spelling,
     *  checked by the caller) as the connection's temporary value; {@code change} as written. */
    IqSettings setTemporary(IqOption option, String value, String change) {
        Map<IqOption, String> t = new LinkedHashMap<>(temporary);
        t.put(option, value);
        return with(client, new LinkedHashMap<>(database), new LinkedHashMap<>(publicTemporary),
                new LinkedHashMap<>(user), t, change);
    }

    /** These settings, a setting or a statement that changes nothing a SELECT reads noted (an option of no effect, a
     *  T-SQL statement of none). */
    IqSettings note(String change) {
        return with(client, new LinkedHashMap<>(database), new LinkedHashMap<>(publicTemporary),
                new LinkedHashMap<>(user), new LinkedHashMap<>(temporary), change);
    }

    private IqSettings note(Scope scope, String name, Value value) {
        return note((scope == Scope.PUBLIC || scope == Scope.PUBLIC_TEMPORARY ? "PUBLIC." : "") + name + "="
                + value.written());
    }

    /**
     * The options IQ keeps with no temporary level, and IQ's error for setting one temporarily: -202 ("only PUBLIC
     * settings are allowed", 42W43) or -203 (42W45). Measured on SQL Anywhere 17 (every PUBLIC option set temporarily
     * at its own value) and, for the ones an island can name, on IQ 17.1 (2026-10-14), which differs on
     * round_to_even (-202 there; SQL Anywhere has no such option). time_zone is -1924 on IQ 17.1 (left at -202: it is
     * pinned, refused by name before a SELECT reads it).
     */
    private static final Map<String, Integer> NO_TEMPORARY_LEVEL = noTemporaryLevel();

    private static Map<String, Integer> noTemporaryLevel() {
        Map<String, Integer> m = new java.util.HashMap<>();
        for (String n : words("checkpoint_time recovery_time reserved_keywords ansi_permissions login_mode"
                + " extern_login_credentials integrated_server_name java_main_userid java_location java_vm_options"
                + " java_class_path database_authentication min_password_length auditing auditing_options"
                + " global_database_id max_hash_size truncate_timestamp_values preserve_source_format"
                + " pinned_cursor_percent_of_cache temp_space_limit_check optimization_workload log_deadlocks"
                + " webservice_namespace_host webservice_sessionid_name http_connection_pool_basesize"
                + " http_connection_pool_timeout allow_snapshot_isolation oem_string trusted_certificates_file"
                + " disk_sandbox audit_log max_connections time_zone reserved_connections"
                + " allow_optional_semicolon_in_tsql round_to_even")) {
            m.put(n, -202);
        }
        for (String n : words("precision scale min_role_admins db_publisher verify_all_columns delete_old_logs"
                + " qualify_owners quote_all_identifiers replication_error replication_error_piece subscribe_by_remote"
                + " verify_threshold blob_threshold compression external_remote_options save_remote_passwords"
                + " sr_date_format sr_time_format sr_timestamp_format sr_timestamp_with_time_zone_format"
                + " assume_distinct_servers ml_remote_id")) {
            m.put(n, -203);
        }
        return java.util.Collections.unmodifiableMap(m);
    }

    /** The words of {@code s}, split at single spaces (no regular expression: the drop-in surface's rule). */
    private static List<String> words(String s) {
        List<String> out = new java.util.ArrayList<>();
        int start = 0;
        for (int i = 0; i <= s.length(); i++) {
            if (i == s.length() || s.charAt(i) == ' ') {
                if (i > start) {
                    out.add(s.substring(start, i));
                }
                start = i + 1;
            }
        }
        return out;
    }

    /** IQ's error for setting {@code name} temporarily where this base keeps it with no temporary level; null if it
     *  has one. */
    private @com.legend.base.Nullable SqlTranslationException noTemporaryLevel(String name, SqlAst.Pos pos) {
        if (base == Base.SA17 && IqLexer.lower(name).equals("round_to_even")) {
            return null; // IQ's own option: SQL Anywhere 17 has none (-203, as a name of no option)
        }
        Integer code = NO_TEMPORARY_LEVEL.get(IqLexer.lower(name));
        if (code == null) {
            return null;
        }
        return code == -202
                ? SqlTranslationException.invalid("the option '" + name + "' is set for PUBLIC only: SET OPTION PUBLIC."
                        + name + IqDialect.code(-202, "42W43"), pos)
                : SqlTranslationException.invalid("the option '" + name + "' is set for PUBLIC only, never"
                        + " temporarily" + IqDialect.code(-203, "42W45"), pos);
    }

    /** {@code value} of an option a SELECT does not read, as written; its value checked where IQ checks one an island
     *  is pasted with: isolation_level, 0 to 3 or a snapshot level (IQ 17.1: 9 is -201). */
    private static String ignoredValue(String name, Value value, SqlAst.Pos pos) {
        if (IqLexer.lower(name).equals("isolation_level")) {
            String v = IqLexer.lower(value.text().trim());
            if (!(v.equals("0") || v.equals("1") || v.equals("2") || v.equals("3") || v.equals("snapshot")
                    || v.equals("statement-snapshot") || v.equals("readonly-statement-snapshot"))) {
                throw invalidSetting(name, value.written(), pos);
            }
        }
        return value.text();
    }

    /** IQ's error for a name of no option set {@code temporarily} (-203) or otherwise (-200). */
    private static SqlTranslationException noOption(String name, boolean temporarily, SqlAst.Pos pos) {
        return temporarily
                ? SqlTranslationException.invalid("IQ sets no temporary option '" + name + "': it has no option of"
                        + " that name" + IqDialect.code(-203, "42W45"), pos)
                : SqlTranslationException.invalid("there is no option '" + name + "'" + IqDialect.code(-200, "42W16"),
                        pos);
    }

    /** IQ's error -201: {@code value} is no setting of {@code option}. */
    static SqlTranslationException invalidSetting(String option, String value, SqlAst.Pos pos) {
        return SqlTranslationException.invalid("the value " + value + " is no setting of the option '" + option + "'"
                + IqDialect.code(-201, "42W17"), pos);
    }

    /**
     * {@code text} (a tagged value's) as {@code option} takes it (IQ's spelling of it): read as IQ reads a string
     * ({@link #read(IqOption, Value, SqlAst.Pos)}) -- {@code '060'} is 60, {@code '60.8'} is -201.
     */
    String read(IqOption option, String text, SqlAst.Pos pos) {
        return read(option, new Value(Value.Form.STRING, text), pos);
    }

    /**
     * {@code value} as {@code option} takes it (IQ's spelling of it), as IQ reads it, measured on IQ 17.1 and SQL
     * Anywhere 17 alike: {@code On} or {@code Off} in any case, quoted or not; {@code YMD}, {@code MDY} or {@code DMY}
     * in any case; an integer in its range written as a number whose value is whole ({@code 60}, {@code 6e1},
     * {@code 60.0}) or as a string ({@link #integerText}: {@code '060'}, {@code ' +60'}, {@code ''} as 0); any text.
     * Else IQ's error -201: a fraction ({@code 60.8}, {@code '60.0'}) too, under either base (the research's D-Q2,
     * "IQ cuts a fraction", is refuted by IQ 17.1, which raises -201 as SQL Anywhere does).
     */
    String read(IqOption option, Value value, SqlAst.Pos pos) {
        String text = value.text();
        IqOption.Kind kind = option.kind();
        if (kind == IqOption.Kind.ON_OFF) {
            if (value.form() != Value.Form.NUMBER && (text.equalsIgnoreCase("on") || text.equalsIgnoreCase("off"))) {
                return text.equalsIgnoreCase("on") ? "On" : "Off";
            }
            throw invalidSetting(option.spelling(), value.written(), pos);
        }
        if (kind == IqOption.Kind.DATE_ORDER) {
            String upper = IqLexer.lower(text).toUpperCase(java.util.Locale.ROOT);
            if (value.form() != Value.Form.NUMBER && (upper.equals("YMD") || upper.equals("MDY")
                    || upper.equals("DMY"))) {
                return upper;
            }
            throw invalidSetting(option.spelling(), value.written(), pos);
        }
        if (kind.isInteger()) {
            BigDecimal n = null;
            if (value.form() == Value.Form.NUMBER) {
                n = new BigDecimal(text);
            } else if (value.form() == Value.Form.STRING) {
                String digits = integerText(text);
                n = digits == null ? null : new BigDecimal(digits);
            }
            if (n == null || n.signum() != 0 && n.stripTrailingZeros().scale() > 0) {
                throw invalidSetting(option.spelling(), value.written(), pos);
            }
            BigInteger whole = n.toBigInteger();
            if (whole.compareTo(BigInteger.valueOf(kind.min())) < 0
                    || whole.compareTo(BigInteger.valueOf(kind.max())) > 0) {
                throw invalidSetting(option.spelling(), value.written(), pos);
            }
            return whole.toString();
        }
        if (option == IqOption.COMM_PROTOCOL) {
            if (text.equalsIgnoreCase("TDS") || text.equalsIgnoreCase("CmdSeq")) {
                return text.equalsIgnoreCase("TDS") ? "TDS" : "CmdSeq";
            }
            throw SqlTranslationException.unsupported("CommProtocol " + value.written() + " is not supported: a"
                    + " connection here is 'CmdSeq' or 'TDS'", pos);
        }
        if (option == IqOption.COLLATION || option == IqOption.CHAR_SET) {
            return text.toUpperCase(java.util.Locale.ROOT);
        }
        return text;
    }

    private static boolean digits(String s) {
        for (int i = 0; i < s.length(); i++) {
            if (!digit(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /** A string as IQ reads it for an integer option (measured on IQ 17.1 and SQL Anywhere 17, 2026-10-14): leading
     *  blanks skipped, then at most one {@code +}, then digits ({@code ' +60'}, {@code '0060'} are 60); the empty
     *  string is 0; anything else ({@code '60 '}, {@code '-1'}, {@code '+ 60'}, {@code '6e1'}, {@code '60.0'}) is no
     *  integer (null, IQ's -201). */
    private static @com.legend.base.Nullable String integerText(String s) {
        if (s.isEmpty()) {
            return "0";
        }
        int i = 0;
        while (i < s.length() && s.charAt(i) == ' ') {
            i++;
        }
        if (i < s.length() && s.charAt(i) == '+') {
            i++;
        }
        String d = s.substring(i);
        return !d.isEmpty() && digits(d) ? d : null;
    }

    // ------------------------------------------------------------------ what is built

    /**
     * Refuses by name what these settings would need that the island does not build: a value other than the default
     * of an option pinned (the research's Tier 3), not built yet (Tier 2), or that needs a provisional decision; a value
     * of a Tier 1 option or database property outside those read exactly ({@link IqOption.Status#SOME_VALUES}); a
     * combination of database properties not read. Each refusal names the setting. Read once per statement, for the
     * settings its SELECT is translated under.
     */
    public void check() {
        for (IqOption o : IqOption.values()) {
            String v = value(o);
            String d = o.defaultFor(base);
            if (v == null || d == null) {
                continue;
            }
            switch (o.status()) {
                case IMPLEMENTED, IGNORED -> {
                }
                case SOME_VALUES -> someValues(o, v);
                case PROVISIONAL, NOT_YET, PINNED -> {
                    if (!same(o, v, d)) {
                        throw refused(o, v, o.status() == IqOption.Status.PROVISIONAL
                                ? "it needs the research's provisional decision " + o.decision() + ", not yet verified"
                                : o.status() == IqOption.Status.PINNED ? "the island reads it at its default, " + d
                                        + ", only (the research's Tier 3: not reproduced faithfully)"
                                        : "the island reads it at its default, " + d + ", only");
                    }
                }
            }
        }
    }

    private static boolean same(IqOption o, String v, String d) {
        return v.equalsIgnoreCase(d) || o == IqOption.SQL_FLAGGER_ERROR_LEVEL && v.equalsIgnoreCase("W");
    }

    private void someValues(IqOption o, String v) {
        switch (o) {
            case BLANK_PADDING -> {
                if (base == Base.IQ16 && v.equals("Off")) {
                    throw refused(o, v, "an IQ 16 database is always blank-padded");
                }
            }
            case COLLATION -> {
                if (!v.equals("ISO_BINENG") && !v.equals("UTF8BIN")) {
                    throw refused(o, v, "the collations read are ISO_BINENG and UTF8BIN");
                }
            }
            case CHAR_SET -> {
                if (!v.equals("ISO_8859-1:1987") && !v.equals("UTF-8")) {
                    throw refused(o, v, "the charsets read are ISO_8859-1:1987 and UTF-8");
                }
                String coll = text(IqOption.COLLATION);
                if (!v.equals(charsetOf(coll))) {
                    throw refused(o, v, "a database of collation " + coll + " has the charset " + charsetOf(coll));
                }
            }
            case ACCENT_SENSITIVE -> {
                if (v.equals("On")) {
                    throw refused(o, v, "the binary collations read are accent-sensitive as IQ reads them with"
                            + " AccentSensitive Off; an accent-insensitive comparison is not read");
                }
            }
            case PRECISION, SCALE -> {
                if (scale() > precision()) {
                    throw SqlTranslationException.unsupported("the settings precision = " + precision() + " and scale"
                            + " = " + scale() + " are not supported: a SCALE above the PRECISION makes IQ show some"
                            + " results as '?'", SETTINGS);
                }
            }
            default -> {
                // the formats: a value not read is refused where a date or a timestamp is written as text
            }
        }
    }

    private SqlTranslationException refused(IqOption o, String v, String why) {
        return SqlTranslationException.unsupported("the setting " + o.spelling() + " = '" + v + "' is not supported"
                + " yet: " + why, SETTINGS);
    }

    // ------------------------------------------------------------------ the settings the translator reads

    /** How the island names these settings in its messages: the profile's name, then the settings made beyond it. */
    public String spelling() {
        return changes.isEmpty() ? preset : preset + "(" + String.join(", ", changes) + ")";
    }

    /** The profile the settings started from. */
    public String preset() {
        return preset;
    }

    public Base base() {
        return base;
    }

    public Client client() {
        return client;
    }

    /** Whether the database is IQ's ({@link Base#IQ16}): only there does a query over tables follow the IQ engine's
     *  rules; a SQL Anywhere 17 database's every query its front end runs. */
    public boolean iqDatabase() {
        return base == Base.IQ16;
    }

    /** {@code CaseSensitive}: the database compares strings case-sensitively (IQ's CASE RESPECT; SQL Anywhere's
     *  default ignores case). */
    public boolean caseSensitive() {
        return on(IqOption.CASE_SENSITIVE);
    }

    /** {@code BlankPadding}: the database ignores trailing blanks in comparisons. */
    public boolean blankPadding() {
        return on(IqOption.BLANK_PADDING);
    }

    /** {@code CharSet}: the database's CHAR charset is ISO-8859-1 (IQ's), not UTF-8. */
    public boolean latin1() {
        return text(IqOption.CHAR_SET).equalsIgnoreCase("ISO_8859-1:1987");
    }

    /** Whether the database is the real SAP IQ 17.1's the {@code iq17-*} presets describe (IQ's base, a UTF-8 CHAR
     *  charset), whose judge is the live IQ; IQ 16's stand-in, SQL Anywhere 17, judges the others. */
    public boolean iq17() {
        return base() == Base.IQ16 && !latin1();
    }

    /** {@code ansinull}: On, a comparison with NULL is UNKNOWN; Off (a TDS login), {@code = NULL} means IS NULL. */
    public boolean ansinull() {
        return on(IqOption.ANSINULL);
    }

    /** {@code tsql_variables}: On (a TDS login, as its login procedure sets it), {@code @name} is a placeholder whose
     *  value the client binds (IQ island, 2026-10-14, review 6's M1, measured on SQL Anywhere 17: it reads the bound
     *  value even where a column of its name is in scope); Off, a connection's variable, read after such a column. */
    public boolean tsqlVariables() {
        return on(IqOption.TSQL_VARIABLES);
    }

    /** {@code tsql_outer_joins}: On, {@code *=} and {@code =*} are T-SQL outer joins; Off, IQ's error -1099 (IQ island,
     *  2026-10-15, checklist 0.6, measured on IQ 17.1). */
    public boolean tsqlOuterJoins() {
        return on(IqOption.TSQL_OUTER_JOINS);
    }

    /** {@code quoted_identifier}: On, {@code "x"} is an identifier; Off (a TDS login), a string. */
    public boolean quotedIdentifier() {
        return on(IqOption.QUOTED_IDENTIFIER);
    }

    /** {@code escape_character}: On, {@code \n}, {@code \\} and {@code \xHH} are escapes in a string; Off (a TDS
     *  login), a backslash is a character. */
    public boolean escapeCharacter() {
        return on(IqOption.ESCAPE_CHARACTER);
    }

    /** {@code date_order}: how a date string is read. */
    public String dateOrder() {
        return text(IqOption.DATE_ORDER);
    }

    /** {@code nearest_century}: a two-digit year below it is of the 2000s, else of the 1900s. */
    public int nearestCentury() {
        return integer(IqOption.NEAREST_CENTURY);
    }

    /** {@code date_format}: how a date is written as text. */
    public String dateFormat() {
        return text(IqOption.DATE_FORMAT);
    }

    /** {@code time_format}. */
    public String timeFormat() {
        return text(IqOption.TIME_FORMAT);
    }

    /** {@code timestamp_format}. */
    public String timestampFormat() {
        return text(IqOption.TIMESTAMP_FORMAT);
    }

    /** {@code precision}: the widest NUMERIC. */
    public int precision() {
        return integer(IqOption.PRECISION);
    }

    /** {@code scale}: the scale of an unqualified NUMERIC's computations. */
    public int scale() {
        return integer(IqOption.SCALE);
    }

    /** {@code divide_by_zero_error}: On, a zero divisor is IQ's error -628; Off, the quotient (or remainder) is
     *  NULL, of its type (measured on SQL Anywhere 17 for every type: integers, NUMERIC, DOUBLE, {@code %}, MOD). */
    public boolean divideByZeroError() {
        return on(IqOption.DIVIDE_BY_ZERO_ERROR);
    }

    /** {@code ansi_substring}: On, SUBSTRING's positions below 1 are virtual and a negative length is IQ's error;
     *  Off, a start below 1 counts from the end and a negative length the characters before the start. */
    public boolean ansiSubstring() {
        return on(IqOption.ANSI_SUBSTRING);
    }

    /** {@code first_day_of_week}: the day DATEPART's weekday counts from, 1 (Monday) to 7 (Sunday). */
    public int firstDayOfWeek() {
        return integer(IqOption.FIRST_DAY_OF_WEEK);
    }

    @Override
    public String toString() {
        return spelling();
    }
}
