package com.github.benmanes.caffeine.cache.simulator.policy.sampled;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admission;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toSet;


/**
 * A cache that uses a sampled array of entries to implement simple page replacement algorithms.
 * <p>
 * The sampling approach for an approximate of classical policies is described
 * <a href="http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.110.8469">Efficient Randomized Web
 * Cache Replacement Schemes Using Samples from Past Eviction Times</a>. The Hyperbolic algorithm is
 * a newer addition to this family and is described in
 * <a href="https://www.usenix.org/system/files/conference/atc17/atc17-blankstein.pdf">Hyperbolic
 * Caching: Flexible Caching for Web Applications</a>.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 */
public final class WeightedSampledPolicy implements Policy {
    final Long2ObjectMap<Node> data;
    final PolicyStats policyStats;
    final EvictionPolicy policy;
    final Sample sampleStrategy;
    final Admittor admittor;
    final int maximumSize;
    final int sampleSize;
    final Random random;
    ArrayList<Node> dynamicTable;
    int currentSize;


    long tick;

    public WeightedSampledPolicy(Admission admission, EvictionPolicy policy, Config config) {
        this.policyStats = new PolicyStats(admission.format("sampled." + policy.label()));
        this.admittor = admission.from(config, policyStats);

        SampledSettings settings = new SampledSettings(config);
        this.sampleStrategy = settings.sampleStrategy();
        this.random = new Random(settings.randomSeed());
        this.data = new Long2ObjectOpenHashMap<>();
        this.maximumSize = settings.maximumSize();
        this.sampleSize = settings.sampleSize();
        this.dynamicTable = new ArrayList<>();
        this.policy = policy;
        this.currentSize = 0;
    }

    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config, EvictionPolicy policy) {
        BasicSettings settings = new BasicSettings(config);
        return settings.admission().stream().map(admission ->
                new WeightedSampledPolicy(admission, policy, config)
        ).collect(toSet());
    }

    @Override
    public Set<Characteristic> characteristics() {
        return Sets.immutableEnumSet(WEIGHTED);
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void record(AccessEvent event) {
        long key = event.key();
        final int weight = event.weight();
        Node node = data.get(key);
        admittor.record(key);
        long now = ++tick;
        if (now % 5 == -1) {
            System.out.printf("State for %s: {currentSize=%d, data.size=%d, hitRate=%f}%n", policyStats.name(), currentSize, data.size(), policyStats.hitRate());
        }
        if (node == null) {
            if (weight > maximumSize) {
                policyStats.recordOperation();
                return;
            }
            node = new Node(key, data.size(), now, weight);
            policyStats.recordOperation();
            policyStats.recordWeightedMiss(weight);
            dynamicTable.add(node);
            currentSize += weight;
            /*
            try {
                dynamicTable[node.index] = node;
            }catch (ArrayIndexOutOfBoundsException e) {
                System.out.printf("Exception index with node=%s running policy=%s and total=%d%n", node.toString(), policyStats.name(), currentSize);
                System.exit(1);
            }
             */
            data.put(key, node);
            evict(node);
        } else {
            policyStats.recordOperation();
            policyStats.recordWeightedHit(weight);
            node.accessTime = now;
            node.frequency++;
        }
    }

    /**
     * Evicts if the map exceeds the maximum capacity.
     */
    private void evict(Node candidate) {
        if (currentSize > maximumSize) {
            while (currentSize > maximumSize) {
                List<Node> sample = (policy == EvictionPolicy.RANDOM)
                        ? dynamicTable
                        : sampleStrategy.sample(dynamicTable, candidate, sampleSize, random, policyStats);
                Node victim = policy.select(sample, random, tick);
                policyStats.recordEviction();


                if (admittor.admit(candidate.key, victim.key)) {
                    removeFromTable(victim);
                    data.remove(victim.key);
                } else {
                    removeFromTable(candidate);
                    data.remove(candidate.key);
                }
            }
        }
    }

    /**
     * Removes the node from the table and adds the index to the free list.
     */
    private void removeFromTable(Node node) {
        currentSize -= node.weight;
        dynamicTable.remove(node);
    }

    /**
     * The algorithms to choose a random sample with.
     */
    public enum Sample {
        GUESS {
            @SuppressWarnings("PMD.AvoidReassigningLoopVariables")
            @Override
            public <E> List<E> sample(List<E> elements, E candidate,
                                      int sampleSize, Random random, PolicyStats policyStats) {
                List<E> sample = new ArrayList<>(sampleSize);
                policyStats.addOperations(sampleSize);
                for (int i = 0; i < sampleSize; i++) {
                    int index = random.nextInt(elements.size());
                    if (elements.get(index) == candidate) {
                        i--; // try again
                    }
                    sample.add(elements.get(index));
                }
                return sample;
            }
        },
        RESERVOIR {
            @Override
            public <E> List<E> sample(List<E> elements, E candidate,
                                      int sampleSize, Random random, PolicyStats policyStats) {
                List<E> sample = new ArrayList<>(sampleSize);
                policyStats.addOperations(elements.size());
                int count = 0;
                for (E e : elements) {
                    if (e == candidate) {
                        continue;
                    }
                    count++;
                    if (sample.size() <= sampleSize) {
                        sample.add(e);
                    } else {
                        int index = random.nextInt(count);
                        if (index < sampleSize) {
                            sample.set(index, e);
                        }
                    }
                }
                return sample;
            }
        },
        SHUFFLE {
            @Override
            public <E> List<E> sample(List<E> elements, E candidate,
                                      int sampleSize, Random random, PolicyStats policyStats) {
                List<E> sample = new ArrayList<>(elements);
                policyStats.addOperations(elements.size());
                Collections.shuffle(sample, random);
                sample.remove(candidate);
                return sample.subList(0, sampleSize);
            }
        };

        abstract <E> List<E> sample(List<E> elements, E candidate,
                                    int sampleSize, Random random, PolicyStats policyStats);
    }

    /**
     * The replacement policy.
     */
    public enum EvictionPolicy {

        /**
         * Evicts entries based on insertion order.
         */
        FIFO {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                return sample.stream().min((first, second) ->
                        Long.compare(first.insertionTime, second.insertionTime)).get();
            }
        },

        /**
         * Evicts entries based on how recently they are used, with the least recent evicted first.
         */
        LRU {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                return sample.stream().min((first, second) ->
                        Long.compare(first.accessTime, second.accessTime)).get();
            }
        },

        /**
         * Evicts entries based on how recently they are used, with the least recent evicted first.
         */
        MRU {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                return sample.stream().max((first, second) ->
                        Long.compare(first.accessTime, second.accessTime)).get();
            }
        },

        /**
         * Evicts entries based on how frequently they are used, with the least frequent evicted first.
         */
        LFU {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                return sample.stream().min((first, second) ->
                        Long.compare(first.frequency, second.frequency)).get();
            }
        },

        /**
         * Evicts entries based on how frequently they are used, with the most frequent evicted first.
         */
        MFU {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                return sample.stream().max((first, second) ->
                        Long.compare(first.frequency, second.frequency)).get();
            }
        },

        /**
         * Evicts a random entry.
         */
        RANDOM {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                int victim = random.nextInt(sample.size());
                return sample.get(victim);
            }
        },

        /**
         * Evicts entries based on how frequently they are used divided by their age.
         */
        HYPERBOLIC {
            @Override
            Node select(List<Node> sample, Random random, long tick) {
                return sample.stream().min((first, second) ->
                        Double.compare(hyperbolic(first, tick), hyperbolic(second, tick))).get();
            }

            double hyperbolic(Node node, long tick) {
                return node.frequency / (double) (tick - node.insertionTime);
            }
        };

        public String label() {
            return StringUtils.capitalize(name().toLowerCase(US));
        }

        /**
         * Determines which node to evict.
         */
        abstract Node select(List<Node> sample, Random random, long tick);
    }

    static final class Node {
        final long key;
        final long insertionTime;

        long accessTime;
        int frequency;
        int index;
        int weight;

        public Node(long key, int index, long tick, int weight) {
            this.insertionTime = tick;
            this.accessTime = tick;
            this.index = index;
            this.key = key;
            this.weight = weight;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("index", index)
                    .add("time", accessTime)
                    .add("weight", weight)
                    .toString();
        }
    }

    static final class SampledSettings extends BasicSettings {
        public SampledSettings(Config config) {
            super(config);
        }

        public int sampleSize() {
            return config().getInt("sampled.size");
        }

        public Sample sampleStrategy() {
            return Sample.valueOf(config().getString("sampled.strategy").toUpperCase(US));
        }
    }
}
