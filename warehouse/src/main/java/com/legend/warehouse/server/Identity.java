package com.legend.warehouse.server;

import com.legend.base.Nullable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import javax.crypto.Mac;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Who is asking: users, their passwords, and the tokens that stand for a
 * signed-in user.
 *
 * <p>THE PRINCIPAL COMES ONLY FROM A VERIFIED TOKEN (program §3, 0b). A
 * token is {@code base64url(principal|expiry|signedInAt) . base64url(HMAC-SHA256)}
 * under a key only this server holds; nothing a client sends can name a
 * user any other way. W1's users and passwords live in the server's
 * configuration; W2 replaces the password store with an identity
 * provider's signed tokens.
 */
public final class Identity {

    /** What a principal may look like: it is written into SQL as a string literal. */
    private static final Pattern PRINCIPAL = Pattern.compile("[A-Za-z0-9_.@-]{1,128}");
    /** One character a principal cannot hold: {@link #accountPrincipal} writes {@code _} for it. */
    private static final Pattern NOT_PRINCIPAL = Pattern.compile("[^A-Za-z0-9_.@-]");
    private static final int ITERATIONS = 120_000;

    /** How long one sign-in lasts, refreshes included: past it, only the password signs in again. */
    public static final Duration DEFAULT_SESSION_LIMIT = Duration.ofHours(12);

    private final byte[] key;
    private final Duration tokenLife;
    private final Duration sessionLimit;
    private final Clock clock;
    private final Map<String, Hashed> users = new HashMap<>();
    private final SecureRandom random = new SecureRandom();

    private record Hashed(byte[] salt, byte[] hash) {
    }

    private record Launch(String principal, byte[] key) {
    }

    private @Nullable Launch launch;

    public Identity(byte[] key, Duration tokenLife, Clock clock) {
        this(key, tokenLife, DEFAULT_SESSION_LIMIT, clock);
    }

    public Identity(byte[] key, Duration tokenLife, Duration sessionLimit, Clock clock) {
        if (key.length < 32) throw new IllegalArgumentException("the token key must be at least 32 bytes");
        this.key = key.clone();
        this.tokenLife = tokenLife;
        this.sessionLimit = sessionLimit;
        this.clock = clock;
    }

    /** Whether a user of that name can sign in (names compare without case, as grants do). */
    public synchronized boolean hasUser(String name) {
        for (String u : users.keySet()) {
            if (u.equalsIgnoreCase(name)) return true;
        }
        return launch != null && launch.principal().equalsIgnoreCase(name);
    }

    /** A user a token may stand for: one with a password, or the launch key's. */
    private synchronized boolean known(String principal) {
        return users.containsKey(principal) || (launch != null && launch.principal().equals(principal));
    }

    public static boolean validPrincipal(String name) {
        return PRINCIPAL.matcher(name).matches();
    }

    /**
     * The operating-system account running the single-user app, as a principal: each character a principal
     * cannot hold becomes {@code _}, so a Windows account name with a space ({@code John Madsen}) is
     * {@code John_Madsen} rather than a server that will not start (review of neema2/legend-lite#14,
     * 2026-10-03). Only a name with nothing to keep (empty, or longer than a principal) is refused.
     */
    public static String accountPrincipal(String account) {
        String principal = NOT_PRINCIPAL.matcher(account).replaceAll("_");
        if (!validPrincipal(principal)) {
            throw new IllegalArgumentException("--single-user: the account name '" + account + "' cannot be a warehouse user");
        }
        return principal;
    }

    /** Add a user; only the password's salted hash is kept. */
    public void addUser(String name, String password) {
        if (!validPrincipal(name)) throw new IllegalArgumentException("bad user name: " + name);
        byte[] salt = new byte[16];
        random.nextBytes(salt);
        users.put(name, new Hashed(salt, pbkdf2(password, salt)));
    }

    /** A token for the user, or null when the name or password is wrong. */
    public @Nullable Issued login(String name, String password) {
        Hashed h = users.get(name);
        // The hash runs even for an unknown user, so a wrong name and a wrong
        // password take the same time.
        byte[] tried = pbkdf2(password, h == null ? new byte[16] : h.salt());
        if (h == null || !MessageDigest.isEqual(tried, h.hash())) return null;
        Instant now = clock.instant();
        return issue(name, now.plus(tokenLife), now.getEpochSecond());
    }

    /**
     * Make the key that signs {@code principal} in without a password: the single-user app's
     * (docs/DATACUBE_APP_PLAN_2026_10_02.md, A1), handed to the browser it opens in the address's
     * fragment, which a browser never sends. It stays good while the server runs, so the page can
     * reload; there is one, made once.
     */
    public synchronized String launchKey(String principal) {
        if (!validPrincipal(principal)) throw new IllegalArgumentException("bad user name: " + principal);
        if (launch != null) throw new IllegalStateException("the launch key is already made");
        byte[] k = new byte[32];
        random.nextBytes(k);
        launch = new Launch(principal, k);
        return enc(k);
    }

    /** A token for the launch key's principal, or null when {@code key} is not the launch key. */
    public synchronized @Nullable Issued loginWithKey(String key) {
        if (launch == null) return null;
        byte[] tried;
        try {
            tried = Base64.getUrlDecoder().decode(key);
        } catch (IllegalArgumentException garbled) {
            return null;
        }
        if (!MessageDigest.isEqual(tried, launch.key())) return null;
        Instant now = clock.instant();
        return issue(launch.principal(), now.plus(tokenLife), now.getEpochSecond());
    }

    /**
     * A fresh token for the user a still-valid token stands for, so an open page need not ask for
     * the password each hour. It carries the ORIGINAL sign-in time, and no refresh reaches past
     * that plus the session limit: a token that leaks is not good forever. Null when the token is
     * not valid, or the session has reached its limit.
     */
    public @Nullable Issued refresh(@Nullable String token) {
        Verified v = verified(token);
        if (v == null) return null;
        Instant now = clock.instant();
        Instant limit = Instant.ofEpochSecond(v.signedInAt()).plus(sessionLimit);
        Instant expires = now.plus(tokenLife);
        if (expires.isAfter(limit)) expires = limit;
        if (!expires.isAfter(now)) return null;
        return issue(v.principal(), expires, v.signedInAt());
    }

    private Issued issue(String principal, Instant expires, long signedInAt) {
        String payload = principal + "|" + expires.getEpochSecond() + "|" + signedInAt;
        return new Issued(enc(payload.getBytes(StandardCharsets.UTF_8)) + "." + enc(sign(payload)), principal, expires);
    }

    /** A token that has been issued. */
    public record Issued(String token, String principal, Instant expires) {
    }

    /** The principal a token stands for, or null when it is forged, garbled or expired. */
    public @Nullable String verify(@Nullable String token) {
        Verified v = verified(token);
        return v == null ? null : v.principal();
    }

    private record Verified(String principal, long signedInAt) {
    }

    private @Nullable Verified verified(@Nullable String token) {
        if (token == null) return null;
        int dot = token.indexOf('.');
        if (dot <= 0 || dot != token.lastIndexOf('.')) return null;
        String payload;
        byte[] sig;
        try {
            payload = new String(Base64.getUrlDecoder().decode(token.substring(0, dot)), StandardCharsets.UTF_8);
            sig = Base64.getUrlDecoder().decode(token.substring(dot + 1));
        } catch (IllegalArgumentException garbled) {
            return null;
        }
        if (!MessageDigest.isEqual(sign(payload), sig)) return null;
        // principal|expiry|signedInAt; a principal holds no '|' (PRINCIPAL)
        String[] parts = payload.split("\\|", -1);
        if (parts.length != 3) return null;
        String principal = parts[0];
        long expiry;
        long signedInAt;
        try {
            expiry = Long.parseLong(parts[1]);
            signedInAt = Long.parseLong(parts[2]);
        } catch (NumberFormatException garbled) {
            return null;
        }
        if (clock.instant().getEpochSecond() >= expiry) return null;
        return validPrincipal(principal) && known(principal) ? new Verified(principal, signedInAt) : null;
    }

    private byte[] sign(String payload) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("HMAC-SHA256 unavailable", e);
        }
    }

    private static byte[] pbkdf2(String password, byte[] salt) {
        try {
            SecretKeyFactory f = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256");
            return f.generateSecret(new PBEKeySpec(password.toCharArray(), salt, ITERATIONS, 256)).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2 unavailable", e);
        }
    }

    private static String enc(byte[] b) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(b);
    }
}
