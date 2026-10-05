package com.legend.sdlc;

import com.legend.base.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** {@link Storage} in memory: a test's, and the page's (whose records the page persists itself). */
public final class MemoryStorage implements Storage {
    private final TreeMap<String, String> records = new TreeMap<>();
    /** Keys written or deleted since the last {@link #drainChanges()}: what the page must persist. */
    private final LinkedHashSet<String> changed = new LinkedHashSet<>();

    @Override
    public @Nullable String get(String key) {
        return records.get(key);
    }

    @Override
    public void put(String key, String value) {
        records.put(key, value);
        changed.add(key);
    }

    @Override
    public void delete(String key) {
        records.remove(key);
        changed.add(key);
    }

    @Override
    public List<String> keys(String prefix) {
        List<String> out = new ArrayList<>();
        for (Map.Entry<String, String> e : records.tailMap(prefix, true).entrySet()) {
            if (!e.getKey().startsWith(prefix)) break;
            out.add(e.getKey());
        }
        return out;
    }

    /** The keys changed since the last call, in first-change order, then forgotten. */
    public List<String> drainChanges() {
        List<String> out = new ArrayList<>(changed);
        changed.clear();
        return out;
    }

    /** Loads a record without counting it as a change (the page restoring what it persisted). */
    public void load(String key, String value) {
        records.put(key, value);
    }
}
