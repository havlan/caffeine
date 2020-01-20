package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.FrequentlyUsedPolicy;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class LecarPolicy implements Policy.KeyOnlyPolicy {

    private final Long2ObjectMap<Node> data;
    private final Node headLru;
    private final FrequencyNode freq0;
    private final PolicyStats policyStats;
    private final int maximumSize;
    private final int partSize;
    private int sizeLru = 0;
    private int sizeLfu = 0;

    public double[] getWeights() {
        return weights;
    }

    public void setWeights(double[] weights) {
        this.weights = weights;
    }

    private double[] weights;
    private Random generator = new Random();
    private LinkedHashMap<Long, Node> HLru;
    private LinkedHashMap<Long, Node> HLfu;
    private final double discountRate = 0.45;
    private final double learningRate;

    public LecarPolicy(Config config) {
        System.out.println("Starting LeCaR");
        BasicSettings settings = new BasicSettings(config);
        this.policyStats = new PolicyStats("adaptive.Lecar");
        this.maximumSize = settings.maximumSize();
        this.partSize = maximumSize / 2;
        this.data = new Long2ObjectOpenHashMap<>();
        double initialWeight = 0.6;
        this.weights = new double[]{initialWeight, 1.0 - initialWeight};
        this.headLru = new Node();
        this.freq0 = new FrequencyNode();
        this.learningRate = Math.pow(0.005, 1.0 / maximumSize);
        this.HLru = new LinkedHashMap<Long, Node>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > maximumSize;
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
                return size() > maximumSize;
            }

            @Override
            public String toString() {
                String output = "";
                for (Long key : this.keySet()) {
                    output += String.format("Object={%d => %s}\n", key, this.get(key));
                }
                return output;
            }
        };
    }

    /**
     * Returns all variations of this policy based on the configuration parameters.
     */
    public static Set<Policy> policies(Config config) {
        return ImmutableSet.of(new LecarPolicy(config));
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    private void printState(Node node) {
        System.out.printf("STATE: { Current Node=%s, Weights=[%f, %f], sizeLru=%d, sizeLfu=%d, sizeHLru=%d, sizeHLfu=%d }\n", node, weights[0], weights[1], sizeLru, sizeLfu, HLru.size(), HLfu.size());
    }

    @Override
    public void finished() {
        checkState(sizeLru == data.values().stream().filter(node -> node.type == QueueType.LRU).count());
        checkState(sizeLfu == data.values().stream().filter(node -> node.type == QueueType.LFU).count());
        //checkState(sizeGru == HLru.size());
        //checkState(sizeGfu == HLfu.size());
        checkState((sizeLru + sizeLfu) <= maximumSize);
    }

    @Override
    public void record(long key) {
        policyStats.recordOperation();
        Node node = data.get(key);
        printState(node);
        if (node == null) {
            onMiss(key);
        } else {
            onHit(node);
        }
    }


    private QueueType pickCacheType() {
        double randomChoice = generator.nextDouble();
        QueueType type;
        if (randomChoice < weights[0]) {
            type = QueueType.LRU;
        } else {
            type = QueueType.LFU;
        }
        //System.out.printf("%s chosen\n", type);
        return type;
    }

    /**
     * @param q Node
     */
    private void updateWeights(final Node q) {
        System.out.printf("\nUpdating weights for node=%s\n", q);
        long timeInGhost = generator.nextInt(HLru.size()); // diff now and time q in history
        double regret = Math.pow(discountRate, timeInGhost);
        double[] localWeights = getWeights();

        if (q.type == QueueType.LRU) {
            localWeights[0] = localWeights[0] * Math.exp(learningRate * regret);
        } else {
            localWeights[1] = localWeights[1] * Math.exp(learningRate * regret);
        }
        localWeights[0] = localWeights[0] / (localWeights[0] + localWeights[1]); // normalization
        localWeights[1] = 1 - localWeights[0];
        setWeights(localWeights);
    }


    private void onMiss(long key) {
        //System.out.printf("Miss on key=%d\n", key);
        policyStats.recordMiss();
        Node q = null;
        boolean foundInHistory = false;
        if (HLru.containsKey(key)) {
            q = HLru.get(key);
            HLru.remove(key);
            foundInHistory = true;
        } else if (HLfu.containsKey(key)) {
            q = HLfu.get(key);
            HLfu.remove(key);
            foundInHistory = true;
        }

        if (foundInHistory) {
            updateWeights(q);
        }
        QueueType type = pickCacheType();
        Node node = null;
        if (type == QueueType.LRU) {
            // add new LRU node
            node = new Node(key);
            node.type = QueueType.LRU;
            data.put(key, node);
            node.append(headLru);
            sizeLru++;
        } else {
            // add new LFU node
            FrequencyNode freq1 = (freq0.next.count == 1)
                    ? freq0.next
                    : new FrequencyNode(1, freq0);
            node = new Node(key, freq1);
            node.type = QueueType.LFU;
            data.put(key, node);
            node.append();
            sizeLfu++;
        }

        if (data.size() >= maximumSize) {
            if (type == QueueType.LRU) {
                // delete LRU from cache since it's full
                Node victim = headLru.next;
                data.remove(victim.key);
                victim.remove();
                sizeLru--;
                // add to HLru
                HLru.put(victim.key, victim);
                //System.out.printf("Adding %s to HLRU\n", victim);
            } else {
                // delete LFU from cache since it's full, skip deleting the node we just added (from FrequentlyUsedPolicy)
                Node victim = freq0.next.nextNode.next;
                if (victim == node) {
                    victim = (victim.next == victim.prev)
                            ? victim.freq.next.nextNode.next
                            : victim.next;
                }
                data.remove(victim.key);
                victim.remove();
                sizeLfu--;
                if (victim.freq.isEmpty()) {
                    victim.freq.remove();
                }
                HLfu.put(victim.key, victim);
                //System.out.printf("Adding %s to HLFU\n", victim);

            }
            policyStats.recordEviction();
        }
    }

    private void onHit(Node node) {
        policyStats.recordHit();
        System.out.printf("Hit on node=%s\n", node);
        // if it's an LFU node
        if (node.type == QueueType.LFU) {
            int newCount = node.freq.count + 1;
            FrequencyNode freqN = (node.freq.next.count == newCount)
                    ? node.freq.next
                    : new FrequencyNode(newCount, node.freq);
            node.remove();
            if (node.freq.isEmpty()) {
                node.freq.remove();
            }
            node.freq = freqN;
            node.append();
        } else {
            // if it's an LRU node, bump its position
            node.remove();
            node.append(headLru);
        }

    }


    /**
     * T1 = LRU
     * T2 = LFU
     * B1 = Ghost LRU
     * B2 = Ghost LFU
     */
    private enum QueueType {
        LRU,
        LFU,
    }

    /**
     * A cache entry on the frequency node's chain.
     */
    static final class Node {
        long key;

        QueueType type;
        FrequencyNode freq;
        Node prev;
        Node next;

        Node() {
            this.key = Long.MIN_VALUE;
            this.prev = this;
            this.next = this;
        }

        Node(long key) {
            this.key = key;
        }

        public Node(FrequencyNode freq) {
            this.key = Long.MIN_VALUE;
            this.freq = freq;
            this.prev = this;
            this.next = this;
        }

        public Node(long key, FrequencyNode freq) {
            this.next = null;
            this.prev = null;
            this.freq = freq;
            this.key = key;
        }

        public void append() {
            System.out.printf("Appending node=%s with type=%s which is correct=%s\n", this.toString(), type, type == QueueType.LFU);
            prev = freq.nextNode.prev;
            next = freq.nextNode;
            prev.next = this;
            next.prev = this;
        }

        /**
         * Appends the node to the tail of the list.
         *
         * @Param Head is only used when it's the ghost list or LRU
         */

        public void append(Node head) {
            System.out.printf("Appending/bumping node=%s with type=%s which is correct=%s\n", this.toString(), type, type == QueueType.LRU);
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
            System.out.printf("Removing node=%s with type=%s\n", this.toString(), type);
            if (type == QueueType.LRU) {
                prev.next = next;
                next.prev = prev;
                prev = next = null;
            } else {
                prev.next = next;
                next.prev = prev;
                next = prev = null;
            }
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("type", type.toString())
                    .add("prev", prev != null)
                    .add("next", next != null)
                    .add("freq", freq != null)
                    .toString();
        }
    }

    /**
     * A frequency count and associated chain of cache entries.
     */
    static final class FrequencyNode {
        final int count;
        final Node nextNode;

        FrequencyNode prev;
        FrequencyNode next;

        public FrequencyNode() {
            System.out.println("Created a new FrequencyNode");
            nextNode = new Node(this);
            this.prev = this;
            this.next = this;
            this.count = 0;
        }

        public FrequencyNode(int count, FrequencyNode prev) {
            System.out.println("Created a new FrequencyNode");
            nextNode = new Node(this);
            this.prev = prev;
            this.next = prev.next;
            prev.next = this;
            next.prev = this;
            this.count = count;
        }

        public boolean isEmpty() {
            return (nextNode == nextNode.next);
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            System.out.println("Removing FrequencyNode");
            prev.next = next;
            next.prev = prev;
            next = prev = null;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("count", count)
                    .toString();
        }
    }
}
