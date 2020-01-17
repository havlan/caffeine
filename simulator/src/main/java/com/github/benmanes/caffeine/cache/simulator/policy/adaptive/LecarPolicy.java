package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.LinkedHashMap;
import java.util.Random;

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
    private int sizeGru = 0;
    private int sizeGfu = 0;
    private int p;
    private double[] w;
    private double initialWeight;
    private Random generator = new Random();
    private LinkedHashMap<Long, Node> HLru;
    private LinkedHashMap<Long, Node> HLfu;

    public LecarPolicy(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.policyStats = new PolicyStats("adaptive.lecar");
        this.maximumSize = settings.maximumSize();
        this.partSize = maximumSize / 2;
        this.data = new Long2ObjectOpenHashMap<>();
        this.w = new double[]{initialWeight, 1.0 - initialWeight};
        this.headLru = new Node();
        this.freq0 = new FrequencyNode();
        this.HLru = new LinkedHashMap<Long, Node>();
        this.HLfu = new LinkedHashMap<Long, Node>();
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void finished() {
        checkState(sizeLru == data.values().stream().filter(node -> node.type == QueueType.LRU).count());
        checkState(sizeLfu == data.values().stream().filter(node -> node.type == QueueType.LFU).count());
        checkState(sizeGru == HLru.size());
        checkState(sizeGfu == HLfu.size());
        checkState((sizeLru + sizeLfu) <= maximumSize);
        checkState((sizeGru + sizeGfu) <= maximumSize);
    }

    @Override
    public void record(long key) {
        policyStats.recordOperation();
        Node node = data.get(key);
        if (node == null) {
            onMiss(key);
        } else {
            onHit(node);
        }
    }


    private QueueType pickCacheType() {
        double randomChoice = generator.nextDouble();
        if (randomChoice < w[0]) {
            return QueueType.LRU;
        } else {
            return QueueType.LFU;
        }
    }

    /**
     * @param q            Node
     * @param learningRate double
     * @param discountRate double
     */
    private void updateWeights(Node q, double learningRate, double discountRate) {
        long timeInGhost = 100; // diff now and time q in history
        double regret = Math.pow(discountRate, timeInGhost);
        if (q.type == QueueType.LRU) {
            w[0] = w[0] * Math.exp(learningRate * regret);
        } else {
            w[1] = w[1] * Math.exp(learningRate * regret);
        }
        w[0] = w[0] / (w[0] + w[1]); // normalization
        w[1] = 1 - w[0];
    }


    private void onMiss(long key) {
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
            double discountRate = 1.0;
            double learningRate = 1.0;
            updateWeights(q, learningRate, discountRate);
        }

        if (data.size() == maximumSize) {
            QueueType type = pickCacheType();

            if (type == QueueType.LRU) {
                if (HLru.size() == partSize) {
                    // make room in HLru
                }
                // delete LRU from cache since it's full
                Node victim = headLru.next;
                data.remove(victim.key);
                victim.remove();
                sizeLru--;
                // add to HLru
                HLru.put(victim.key, victim);
                // add new LRU node
                Node node = new Node(key);
                node.type = QueueType.LRU;
                data.put(key, node);
                node.append(headLru);
            } else {
                if (HLfu.size() == partSize) {
                    // make room in HLfu
                }
                // add to HLfu
                // delete LFU from cache since it's full
                // add new LFU node
            }
        }
    }

    private void onHit(Node node) {
        policyStats.recordHit();

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
            node.append(headLru);
        }

    }

    private void onHitGru(Node node) {
        policyStats.recordMiss();

    }

    private void onHitGfu(Node node) {
        policyStats.recordMiss();

    }

    private void evict(Node candidate) {

    }

    private void evictEntry(Node node) {
        data.remove(node.key);
        node.remove();
        if (node.freq.isEmpty()) {
            node.freq.remove();
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
        final long key;

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
            if (type == QueueType.LFU) {
                prev.next = next;
                next.prev = prev;
                next = prev = null;
            } else {
                checkState(key != Long.MIN_VALUE);
                prev.next = next;
                next.prev = prev;
                prev = next = null;
                type = null;
            }
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("freq", freq)
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
            nextNode = new Node(this);
            this.prev = this;
            this.next = this;
            this.count = 0;
        }

        public FrequencyNode(int count, FrequencyNode prev) {
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
