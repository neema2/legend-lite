package com.legend.warehouse.server;

import com.legend.Nullable;
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
 * token is {@code base64url(principal|expiry) . base64url(HMAC-SHA256)}
 * under a key only this server holds; nothing a client sends can name a
 * user any other way. W1's users and passwords live in the server's
 * configuration; W2 replaces the password store with an identity
 * provider's signed tokens.
 */
public final class Identity {

    /** What a principal may look like: it is written into SQL as a string literal. */
    private static final Pattern PRINCIPAL = Pattern.compile("[A-Za-z0-9_.@-]{1,128}");
    private static final int ITERATIONS = 120_000;

    private final byte[] key;
    private final Duration tokenLife;
    private final Clock clock;
    private final Map<String, Hashed> users = new HashMap<>();
    private final SecureRandom random = new SecureRandom();

    private record Hashed(byte[] salt, byte[] hash) {
    }

    public Identity(byte[] key, Duration tokenLife, Clock clock) {
        if (key.length < 32) throw new IllegalArgumentException("the token key must be at least 32 bytes");
        this.key = key.clone();
        this.tokenLife = tokenLife;
        this.clock = clock;
    }

    public static boolean validPrincipal(String name) {
        return PRINCIPAL.matcher(name).matches();
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
        Instant expires = clock.instant().plus(tokenLife);
        String payload = name + "|" + expires.getEpochSecond();
        return new Issued(enc(payload.getBytes(StandardCharsets.UTF_8)) + "." + enc(sign(payload)), name, expires);
    }

    /** A token that has been issued. */
    public record Issued(String token, String principal, Instant expires) {
    }

    /** The principal a token stands for, or null when it is forged, garbled or expired. */
    public @Nullable String verify(@Nullable String token) {
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
        int bar = payload.lastIndexOf('|');
        if (bar <= 0) return null;
        String principal = payload.substring(0, bar);
        long expiry;
        try {
            expiry = Long.parseLong(payload.substring(bar + 1));
        } catch (NumberFormatException garbled) {
            return null;
        }
        if (clock.instant().getEpochSecond() >= expiry) return null;
        return validPrincipal(principal) && users.containsKey(principal) ? principal : null;
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
