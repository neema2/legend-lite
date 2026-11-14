// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.testbackend;

import com.legend.exec.Sessions;

import java.sql.Connection;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * THE TEST SIDE'S SESSIONS OVER ITS OWN CONNECTIONS (D70, 2026-10-30, the seam's step A). The product makes a session
 * once and holds it; a test opens a JDBC connection of its own, sends its own statements on it (its tables, its
 * asserts) and hands the same connection to the product many times. One session per connection is what the product's
 * memo needs (the seeds established, the session statements run, D66), so this class keeps it: {@link #over} answers
 * the one session over a connection, {@link #owning} registers the session a backend opens for the runner, and
 * {@link #connectionOf} gives a harness that sends its own JDBC on the body's session (the corpus's referee brackets,
 * the fixture asides) the connection beneath it. This is the keying the product's {@code SessionState} map did for
 * everyone until D70, moved to the one side that holds raw connections.
 */
public final class TestSessions {
    private TestSessions() {
    }

    /** The one session over each connection a test handed in. The session references its connection, so a weak key
     *  would never clear: every entry whose connection is closed is purged at each registration instead ({@link
     *  #purge}; the D70 audit: a closed connection, its session and its state -- the wire types of every statement
     *  the body ran -- stayed for the JVM otherwise). */
    private static final Map<Connection, Sessions.Session> OVER =
            java.util.Collections.synchronizedMap(new WeakHashMap<>());

    /** Each session's connection by the session's identity ({@link Sessions#identity}: an opaque token the session
     *  holds, so the entry clears with the session), for {@link #connectionOf}. */
    private static final Map<Object, Connection> OF =
            java.util.Collections.synchronizedMap(new WeakHashMap<>());

    /** The one session over {@code connection}: the owning session a backend registered for it ({@link #owning}), else
     *  a borrowed session made once ({@link Sessions#borrowed}: closing it closes nothing). */
    public static Sessions.Session over(Connection connection) {
        purge();
        return OVER.computeIfAbsent(connection, c -> {
            Sessions.Session s = Sessions.borrowed(c);
            OF.put(Sessions.identity(s), c);
            return s;
        });
    }

    /** The session that OWNS {@code connection} ({@link Sessions#owned}: closing it closes the connection), registered
     *  as the one session over it. */
    public static Sessions.Session owning(Connection connection) {
        purge();
        Sessions.Session s = Sessions.owned(connection);
        OVER.put(connection, s);
        OF.put(Sessions.identity(s), connection);
        return s;
    }

    /** The entries whose connection is closed, dropped: {@code isClosed} on a closed connection is the driver's own
     *  flag, no round trip. */
    private static void purge() {
        synchronized (OVER) {
            OVER.values().removeIf(Sessions::closed);
        }
    }

    /** The connection beneath {@code session} (or any view of it), for a session this class made; a session it did
     *  not make is refused by name. */
    public static Connection connectionOf(Sessions.Session session) {
        Connection c = OF.get(Sessions.identity(session));
        if (c == null) {
            throw new IllegalStateException("no connection is known beneath this session: the test side knows the"
                    + " sessions it made (TestSessions.over, TestSessions.owning), not the product's own");
        }
        return c;
    }
}
