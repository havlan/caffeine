package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.github.benmanes.caffeine.cache.simulator.policy.linked.FrequentlyUsedPolicy;
import com.google.common.base.MoreObjects;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import static com.google.common.base.Preconditions.checkState;

public class Lecar implements Policy.KeyOnlyPolicy {

    private final Long2ObjectMap<Node> data;
    private final Node headLru;
    private final Node headGru;
    private final Node headGfu;
    private final PolicyStats policyStats;
    private final int maximumSize;
    private int sizeLru = 0;
    private int sizeLfu = 0;
    private int sizeGru = 0;
    private int sizeGfu = 0;
    private int p;
    private double[] w;

    public Lecar(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.policyStats = new PolicyStats("adaptive.Lecar");
        this.maximumSize = settings.maximumSize();
        this.data = new Long2ObjectOpenHashMap<>();
        this.w = new double[]{0.5, 0.5};
        this.headLru = new Node();
        this.headGru = new Node();
        this.headGfu = new Node();
    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    @Override
    public void finished() {
        checkState(sizeLru == data.values().stream().filter(node -> node.type == QueueType.LRU).count());
        checkState(sizeLfu == data.values().stream().filter(node -> node.type == QueueType.LFU).count());
        checkState(sizeGru == data.values().stream().filter(node -> node.type == QueueType.GRU).count());
        checkState(sizeGfu == data.values().stream().filter(node -> node.type == QueueType.GFU).count());
        checkState((sizeLru + sizeLfu) <= maximumSize);
        checkState((sizeGru + sizeGfu) <= maximumSize);
    }

    @Override
    public void record(long key) {
        policyStats.recordOperation();
        Node node = data.get(key);
        if (node == null) {
            onMiss(key);
        } else if (node.type == QueueType.LRU) {
            onHitGru(node);
        } else if (node.type == QueueType.LFU) {
            onHitGfu(node);
        } else {
            onHit(node);
        }
    }


    private void onMiss(long key) {
        policyStats.recordMiss();

        // select node type [LRU|LFU]
        // check size of the cache to add it to
        //
        QueueType type = QueueType.LRU;
        if (type == QueueType.LRU) {
            Node node = new Node(key);
            int leftSize = sizeLru + sizeGru;
            // if LRU + ghost is full
            if (leftSize == maximumSize) {
                if (sizeLru < maximumSize) {
                    Node victim = headGru.next;
                    data.remove(victim.key);
                    victim.remove();
                    sizeGru--;

                    evict(node);
                }
            }
        } else {
            int rightSize = sizeLfu + sizeGfu;
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


    /**
     * T1 = LRU
     * T2 = LFU
     * B1 = Ghost LRU
     * B2 = Ghost LFU
     */
    private enum QueueType {
        LRU, GRU,
        LFU, GFU,
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
