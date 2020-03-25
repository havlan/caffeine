package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;
import static com.google.common.base.Preconditions.checkState;

public class LecarPlain implements Policy {
    private final Long2ObjectMap<Node> data;
    private final Node headLru;
    private final PolicyStats policyStats;
    private final int maximumSize;
    private final int partSize;
    private final LinkedHashMap<Long, Node> HLru;
    private final LinkedHashMap<Long, Node> HLfu;
    private final PriorityQueue<Node> pQueue;
    private double[] weights;
    private final double learningRate;
    private final double discountRate;
    private final Random generator;
    private int sizeLru = 0;
    private int sizeLfu = 0;
    private long discreteTime = 0L;
    private LastOperation lastOperation = LastOperation.Record;

    private int weightedSizeLru = 0;
    private int weightedSizeLfu = 0;

    public LecarPlain(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.maximumSize = settings.maximumSize();
        this.partSize = maximumSize / 2;
        this.data = new Long2ObjectOpenHashMap<>();
        double initialWeight = 0.5;
        this.weights = new double[]{initialWeight, 1.0 - initialWeight};
        this.headLru = new Node();
        this.learningRate = 0.1;
        this.discountRate = Math.pow(0.005, 1.0 / maximumSize);
        this.pQueue = new PriorityQueue<Node>(maximumSize, Comparator.comparingInt(a -> a.frequency));
        this.generator = new Random(settings.randomSeed());
        this.policyStats = new PolicyStats("adaptive.LecarPure");
        this.HLru = new LinkedHashMap<Long, Node>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > partSize;
            }

            @Override
            public String toString() {
                StringBuilder output = new StringBuilder();
                for (Long key : this.keySet()) {
                    output.append(String.format("Object={%d => %s}\n", key, this.get(key)));
                }
                return output.toString();
            }
        };
        this.HLfu = new LinkedHashMap<Long, Node>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > partSize;
            }

            @Override
            public String toString() {
                StringBuilder output = new StringBuilder();
                for (Long key : this.keySet()) {
                    output.append(String.format("Object={%d => %s}\n", key, this.get(key)));
                }
                return output.toString();
            }
        };
    }

    public static Set<Policy> policies(Config config) {
        return ImmutableSet.of(new LecarPlain(config));
    }


    @Override
    public Set<Characteristic> characteristics() {
        return Sets.immutableEnumSet(WEIGHTED);
    }

    @Override
    public void record(AccessEvent event) {
        policyStats.recordOperation();

        final long key = event.key();
        final int weight = event.weight();

        Node lruNode = HLru.get(key);
        boolean foundInGhost = false;
        if (lruNode != null) {
            lastOperation = LastOperation.OnGhostLru;
            onGhostHit(lruNode, GhostType.GHOST_LRU);
            foundInGhost = true;
        }
        Node lfuNode = HLfu.get(key);
        if (lfuNode != null) {
            lastOperation = LastOperation.OnGhostLfu;
            onGhostHit(lfuNode, GhostType.GHOST_LFU);
            foundInGhost = true;
        }

        if (!foundInGhost) {
            Node node = data.get(key);
            //checkCurrentState();
            if (node == null) {
                onMiss(key, weight);
            } else {

                lastOperation = LastOperation.OnHit;
                onCacheHit(node);
            }
        }
        discreteTime++;
    }

    private void onGhostHit(Node q, GhostType ghostType) {
        policyStats.recordWeightedMiss(q.weight);
        // if we found in history, adjust weights
        updateState(q, ghostType);
        addNodeFromGhost(q);
    }


    private void onCacheHit(Node node) {
        policyStats.recordWeightedHit(node.weight);
        if (node.type == QueueType.LFU) {
            node.frequency++;
        } else {
            // if it's an LRU node, bump its position
            node.remove();
            node.append(headLru);
        }
    }

    private void onMiss(long key, int weight) {
        policyStats.recordMiss();

        if (HLru.containsKey(key)) {
            Node q = HLru.get(key);
            HLru.remove(key);
            updateState(q, GhostType.GHOST_LRU);
        } else if (HLfu.containsKey(key)) {
            Node q = HLfu.get(key);
            HLfu.remove(key);
            updateState(q, GhostType.GHOST_LFU);
        }
        lastOperation = LastOperation.OnMiss;
        policyStats.recordWeightedMiss(weight);
        QueueType type = pickCacheType();
        addToCache(key, type, weight);
        cleanupCache(type);
    }


    private void updateState(Node q, GhostType type) {
        double regret = Math.pow(discountRate, discreteTime - q.historyStamp);
        double[] localWeights = getWeights();
        if (type == GhostType.GHOST_LRU) {
            localWeights[1] = localWeights[1] * Math.exp(learningRate * regret);
        } else if (type == GhostType.GHOST_LFU) {
            localWeights[0] = localWeights[0] * Math.exp(learningRate * regret);
        }
        localWeights[0] = localWeights[0] / (localWeights[0] + localWeights[1]); // normalization
        localWeights[0] = BigDecimal.valueOf(localWeights[0]).setScale(4, RoundingMode.HALF_UP).doubleValue();
        localWeights[1] = 1.0 - localWeights[0];
        setWeights(localWeights);
    }

    private void setWeights(double[] localWeights) {
        this.weights = localWeights;
    }

    private double[] getWeights() {
        return weights;
    }

    private void addNodeFromGhost(Node node) {
        QueueType type = pickCacheType();
        if (type == QueueType.LRU) {
            node.type = QueueType.LRU;
            node.append(headLru);
            data.put(node.key, node);
            weightedSizeLru += node.weight;
            sizeLru++;
        } else {
            node.type = QueueType.LFU;
            node.frequency = 1;
            pQueue.offer(node);
            data.put(node.key, node);
            weightedSizeLfu += node.weight;
            sizeLfu++;
        }
        cleanupCache(type);
    }

    private QueueType pickCacheType() {
        double randomChoice = generator.nextDouble();
        QueueType type;
        if (randomChoice < weights[0]) {
            type = QueueType.LRU;
        } else {
            type = QueueType.LFU;
        }
        return type;
    }

    private void addToCache(long key, QueueType type, final int weight) {
        if (weight > maximumSize) {
            policyStats.recordOperation();
            return;
        }
        Node node;
        if (type == QueueType.LRU) {
            node = new Node(key);
            node.weight = weight;
            node.type = QueueType.LRU;
            data.put(key, node);
            node.append(headLru);
            sizeLru++;
            weightedSizeLru += weight;
        } else if (type == QueueType.LFU) {
            node = new Node(key, weight, 1);
            node.type = QueueType.LFU;
            data.put(key, node);
            pQueue.offer(node);
            sizeLfu++;
            weightedSizeLfu += weight;
        } else {
            throw new IllegalArgumentException("Fuck!");
        }
    }

    private void cleanupCache(QueueType type) {
        if (type == QueueType.LRU) {
            cleanupLruCache();
        } else if (type == QueueType.LFU) {
            cleanupLfuCache();
        }
    }

    private void cleanupLruCache() {
        if (weightedSizeLru + weightedSizeLfu > maximumSize) {
            while (weightedSizeLru + weightedSizeLfu > maximumSize) {
                Node victim = headLru.next;
                sizeLru--;
                weightedSizeLru -= victim.weight;
                victim.remove();
                victim.historyStamp = (int) discreteTime;
                data.remove(victim.key);
                HLru.put(victim.key, victim);
                policyStats.recordEviction();
            }
        }
    }

    private void cleanupLfuCache() {
        if (weightedSizeLru + weightedSizeLfu > maximumSize) {
            while (weightedSizeLru + weightedSizeLfu > maximumSize) {
                Node victim = pQueue.poll();
                if (victim != null) {
                    data.remove(victim.key);
                    sizeLfu--;
                    weightedSizeLfu -= victim.weight;
                    victim.historyStamp = (int) discreteTime;
                    HLfu.put(victim.key, victim);
                } else {
                    break;
                }
            }
        }
    }

    private void checkCurrentState() {
        int countLru = data.values().stream().filter(node -> node.type == QueueType.LRU).map(i -> i.weight).mapToInt(Integer::intValue).sum();
        int countLfu = data.values().stream().filter(node -> node.type == QueueType.LFU).map(i -> i.weight).mapToInt(Integer::intValue).sum();
        try {

            System.out.printf("Lastoperation=%s, sizeLru=%d, sizeLfu=%d%nWeighted size lru=%d and count=%d%nWeighted size lfu=%d and count=%d%n%n", lastOperation.toString(), sizeLru, sizeLfu, weightedSizeLru, countLru, weightedSizeLfu, countLfu);
            checkState(weightedSizeLru == countLru);
            checkState(weightedSizeLfu == countLfu);
            checkState((sizeLru + sizeLfu) <= maximumSize);
        } catch (IllegalStateException e) {
            System.out.printf("Exception at time %d. E=%s%n", discreteTime, e.toString());
            System.exit(1);
        }
    }

    @Override
    public void finished() {
        int countLru = data.values().stream().filter(node -> node.type == QueueType.LRU).map(i -> i.weight).mapToInt(Integer::intValue).sum();
        int countLfu = data.values().stream().filter(node -> node.type == QueueType.LFU).map(i -> i.weight).mapToInt(Integer::intValue).sum();
        System.out.printf("Weighted size lru=%d and count=%d%n", weightedSizeLru, countLru);
        System.out.printf("Weighted size lfu=%d and count=%d%n", weightedSizeLfu, countLfu);
        System.out.printf("sizeLru=%d, sizeLfu=%d%n", sizeLru, sizeLfu);
        checkState(weightedSizeLru == countLru);
        checkState(weightedSizeLfu == countLfu);
        checkState((sizeLru + sizeLfu) <= maximumSize);
        System.out.printf("LecarPlain finished%n");
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    private enum QueueType {
        LRU,
        LFU
    }

    private enum GhostType {
        GHOST_LRU,
        GHOST_LFU,
    }

    private enum LastOperation {
        Record,
        OnMiss,
        OnGhostLru,
        OnGhostLfu,
        OnHit
    }

    static final class Node {
        long key;

        QueueType type;
        Node prev;
        Node next;
        int weight;
        int historyStamp = 0; // discrete time when added to the cache
        int frequency;

        Node() {
            this.key = Long.MIN_VALUE;
            this.prev = this;
            this.next = this;
        }

        Node(long key) {
            this.key = key;
        }

        public Node(long key, int weight, int startingFrequency) {
            this.weight = weight;
            this.key = key;
            this.frequency = startingFrequency;
        }

        public Node(long key, int startingFrequency) {
            this.next = null;
            this.prev = null;
            this.key = key;
            this.frequency = startingFrequency;
        }

        /**
         * Appends the node to the tail of the list.
         *
         * @Param Head is only used when it's the ghost list or LRU
         */
        public void append(Node head) {
            Node tail = head.prev;
            head.prev = this;
            tail.next = this;
            next = head;
            prev = tail;
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            if (type == QueueType.LRU) {
                checkState(key != Long.MIN_VALUE);
                try {
                    prev.next = next;
                    next.prev = prev;
                    prev = next = null;
                } catch (NullPointerException e) {
                    System.out.printf("NullPtrEx: %s. weightLru=%s%n", e.toString(), toString());
                }
            }
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("type", type.toString())
                    .add("time", historyStamp)
                    .add("frequency", frequency)
                    .add("weight", weight)
                    .toString();
        }
    }
}
