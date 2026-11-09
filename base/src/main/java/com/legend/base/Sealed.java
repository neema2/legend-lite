// Copyright 2026 Legend Contributors
// SPDX-License-Identifier: Apache-2.0

package com.legend.base;

import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A REGISTRY BUILT ONCE (the user's ruling of 2026-11-09, "no mutable state, fix all of it"): a static table a class
 * fills while it initialises -- the platform's built-in functions, the lexer's keywords, the lowering's rule tables --
 * and seals when it is done ({@link Seal#seal}), its last static initializer. Before the seal it takes entries; after
 * it, every write is refused by name, and every view it hands out is read-only. So a static table is a constant of the
 * process, never a channel: a write after start-up, a registration from elsewhere, is an error, not a change. The
 * guard {@code StaticStateRegisterTest} reads every static field of the product after its class is initialised and
 * accepts a sealed one as constant.
 */
public final class Sealed {

    private Sealed() {
    }

    /** A sealable table. */
    public interface Seal {
        /** No write after this. */
        void seal();

        /** Whether {@link #seal} has run. */
        boolean sealed();
    }

    /** A map over {@code delegate} (its iteration order is the delegate's), writable until sealed. */
    public static <K, V> SealedMap<K, V> map(Map<K, V> delegate) {
        return new SealedMap<>(delegate);
    }

    /** A list over {@code delegate}, writable until sealed. */
    public static <T> SealedList<T> list(List<T> delegate) {
        return new SealedList<>(delegate);
    }

    /** A set over {@code delegate} (its iteration order is the delegate's), writable until sealed. */
    public static <T> SealedSet<T> set(Set<T> delegate) {
        return new SealedSet<>(delegate);
    }

    private static void refuse(boolean sealed) {
        if (sealed) {
            throw new UnsupportedOperationException("a sealed registry takes no write after its class initialised");
        }
    }

    /** See {@link #map}. */
    public static final class SealedMap<K, V> extends AbstractMap<K, V> implements Seal {
        private final Map<K, V> delegate;
        private volatile boolean sealed;

        private SealedMap(Map<K, V> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seal() {
            sealed = true;
        }

        @Override
        public boolean sealed() {
            return sealed;
        }

        @Override
        public V get(Object key) {
            return delegate.get(key);
        }

        @Override
        public boolean containsKey(Object key) {
            return delegate.containsKey(key);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public V put(K key, V value) {
            refuse(sealed);
            return delegate.put(key, value);
        }

        @Override
        public V remove(Object key) {
            refuse(sealed);
            return delegate.remove(key);
        }

        @Override
        public void clear() {
            refuse(sealed);
            delegate.clear();
        }

        @Override
        public Set<Entry<K, V>> entrySet() {
            return sealed ? Collections.unmodifiableMap(delegate).entrySet() : delegate.entrySet();
        }

        @Override
        public Set<K> keySet() {
            return sealed ? Collections.unmodifiableSet(delegate.keySet()) : delegate.keySet();
        }

        @Override
        public Collection<V> values() {
            return sealed ? Collections.unmodifiableCollection(delegate.values()) : delegate.values();
        }
    }

    /** See {@link #list}. */
    public static final class SealedList<T> extends AbstractList<T> implements Seal, java.util.RandomAccess {
        private final List<T> delegate;
        private volatile boolean sealed;

        private SealedList(List<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seal() {
            sealed = true;
        }

        @Override
        public boolean sealed() {
            return sealed;
        }

        @Override
        public T get(int index) {
            return delegate.get(index);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public void add(int index, T element) {
            refuse(sealed);
            delegate.add(index, element);
        }

        @Override
        public T set(int index, T element) {
            refuse(sealed);
            return delegate.set(index, element);
        }

        @Override
        public T remove(int index) {
            refuse(sealed);
            return delegate.remove(index);
        }
    }

    /** See {@link #set}. */
    public static final class SealedSet<T> extends AbstractSet<T> implements Seal {
        private final Set<T> delegate;
        private volatile boolean sealed;

        private SealedSet(Set<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public void seal() {
            sealed = true;
        }

        @Override
        public boolean sealed() {
            return sealed;
        }

        @Override
        public boolean contains(Object o) {
            return delegate.contains(o);
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public boolean add(T t) {
            refuse(sealed);
            return delegate.add(t);
        }

        @Override
        public boolean remove(Object o) {
            refuse(sealed);
            return delegate.remove(o);
        }

        @Override
        public Iterator<T> iterator() {
            return sealed ? Collections.unmodifiableSet(delegate).iterator() : delegate.iterator();
        }
    }
}
