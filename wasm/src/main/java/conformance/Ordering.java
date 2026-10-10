package conformance;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * The ordering family: the order a HashMap and a HashSet iterate in, which a program can come to depend on unseen
 * (lite's code iterates maps behind the Map interface), the hash codes that order comes from, a TreeMap over Unicode
 * keys, and a stable sort. Set.of and Map.of are left out: the JVM itself varies their order from run to run.
 */
final class Ordering {

    private Ordering() {
    }

    static String hashOrders() {
        Out out = new Out();
        Rng r = new Rng(12);
        for (int size : new int[] {1, 2, 3, 5, 8, 12, 13, 20, 50, 100, 300}) {
            for (int round = 0; round < 15; round++) {
                List<String> words = new ArrayList<>();
                for (int i = 0; i < size; i++) {
                    words.add(r.word());
                }
                Map<String, Integer> map = new HashMap<>();
                for (int i = 0; i < words.size(); i++) {
                    map.put(words.get(i), i);
                }
                Set<String> set = new HashSet<>(words);
                Set<String> collected = words.stream().collect(Collectors.toSet());
                Map<String, Integer> toMap = words.stream().distinct()
                        .collect(Collectors.toMap(w -> w, String::length));
                for (int i = 0; i < words.size(); i += 3) {
                    map.remove(words.get(i));
                }
                Set<Long> longs = new HashSet<>();
                Set<Integer> ints = new HashSet<>();
                for (int i = 0; i < size; i++) {
                    longs.add(r.next() >> r.below(64));
                    ints.add((int) r.next() >> r.below(32));
                }
                out.add("size " + size + " round " + round + " " + String.join(",", words),
                        String.join(",", map.keySet()) + " | " + String.join(",", set) + " | "
                                + String.join(",", collected) + " | " + String.join(",", toMap.keySet()) + " | "
                                + longs + " | " + ints);
            }
        }
        // keys whose hash codes collide ("Aa" and "BB" hash alike)
        Map<String, Integer> colliding = new HashMap<>();
        String[] parts = {"Aa", "BB"};
        for (int i = 0; i < 64; i++) {
            StringBuilder k = new StringBuilder();
            for (int j = 0; j < 6; j++) {
                k.append(parts[(i >> j) & 1]);
            }
            colliding.put(k.toString(), i);
        }
        out.add("colliding", String.join(",", colliding.keySet()));
        return out.toString();
    }

    static String hashCodes() {
        Out out = new Out();
        for (String s : Text.CORPUS) {
            out.add("hash " + s, s.hashCode() + " " + Objects.hash(s, 1, 2L) + " " + List.of(s, "x").hashCode());
        }
        Rng r = new Rng(13);
        for (int i = 0; i < 2_000; i++) {
            long l = r.next();
            double d = Double.longBitsToDouble(r.next());
            out.add(Out.hex(l) + " " + Out.hex(Double.doubleToRawLongBits(d)), Long.valueOf(l).hashCode() + " "
                    + Double.valueOf(d).hashCode() + " " + Boolean.valueOf((l & 1) == 0).hashCode() + " "
                    + r.word().hashCode());
        }
        TreeMap<String, Integer> tree = new TreeMap<>();
        for (int i = 0; i < Text.CORPUS.length; i++) {
            tree.put(Text.CORPUS[i], i);
        }
        out.add("treemap", tree.values().toString());
        List<String> words = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            words.add(r.word());
        }
        words.sort(Comparator.comparingInt(String::length));
        out.add("stable sort by length", String.join(",", words));
        words.sort(Comparator.comparing((String w) -> w.charAt(0)).thenComparing(String::length));
        out.add("thenComparing", String.join(",", words));
        return out.toString();
    }
}
