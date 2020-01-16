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

public class LecarOO implements Policy.KeyOnlyPolicy {

    private final Long2ObjectMap<FNode> data;
    private final OldNode headLru;
    private final OldNode headGru;
    private final OldNode headGfu;
    private final PolicyStats policyStats;
    private final int maximumSize;
    private int sizeLru;
    private int sizeLfu;
    private int sizeGru;
    private int sizeGfu;
    private int p;

    public LecarOO(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.policyStats = new PolicyStats("adaptive.Lecar");
        this.maximumSize = settings.maximumSize();
        this.data = new Long2ObjectOpenHashMap<>();
        this.headLru = new OldNode();
        this.headGru = new OldNode();
        this.headGfu = new OldNode();
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
        OldNode node = data.get(key);
        if (node == null) {
            onMiss(key);
        } else if (node.type == QueueType.LRU) {
            onHitGru(node);
        } else if (node.type == QueueType.LFU) {
            onHitGfu(node);
        } else {
            //onHit(node);
        }
    }


    private void onMiss(long key) {
        policyStats.recordMiss();

        // select node type [LRU|LFU]
        // check size of the cache to add it to
        //


    }

    /*
    private void onHit(OldNode node) {

        if (node.type == QueueType.LFU) {
            policyStats.recordHit();

            int newCount = node.freq.count + 1;
            FrequentlyUsedPolicy.FrequencyNode freqN = (node.freq.next.count == newCount)
                    ? node.freq.next
                    : new FrequentlyUsedPolicy.FrequencyNode(newCount, node.freq);
            node.remove();
            if (node.freq.isEmpty()) {
                node.freq.remove();
            }
            node.freq = freqN;
            node.append();
        }

    }
     */

    private void onHitGru(OldNode node) {

    }

    private void onHitGfu(OldNode node) {

    }

    private void evict(OldNode candidate) {

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

    static final class Node {
        final long key;
        final long insertionTime;

        QueueType type;
        long accessTime;
        int freq;
        int index;

        public Node(long key, int index, long tick) {
            this.insertionTime = tick;
            this.accessTime = tick;
            this.index = index;
            this.key = key;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("index", index)
                    .toString();
        }
    }

    static final class FrequencyNode {
        final int count;
        final FNode nextNode;

        FrequencyNode prev;
        FrequencyNode next;

        public FrequencyNode() {
            nextNode = new FNode(this);
            this.prev = this;
            this.next = this;
            this.count = 0;
        }

        public FrequencyNode(int count, FrequencyNode prev) {
            nextNode = new FNode(this);
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

    static class OldNode {
        OldNode prev;
        OldNode next;
        long key;
        QueueType type;

        public OldNode() {
            this.key = Long.MIN_VALUE;
            this.prev = this;
            this.next = this;
        }

        public OldNode(long key) {
            this.key = key;
        }

        /**
         * Appends the node to the tail of the list.
         * LRU append
         */
        public void append(OldNode head) {
            OldNode tail = head.prev;
            head.prev = this;
            tail.next = this;
            next = head;
            prev = tail;
        }

        /**
         * Removes the node from the list.
         */
        public void remove() {
            checkState(key != Long.MIN_VALUE);
            prev.next = next;
            next.prev = prev;
            prev = next = null;
            type = null;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("type", type)
                    .toString();
        }

    }

    /**
     * A cache entry on the frequency node's chain.
     */
    static final class FNode extends OldNode {

        FrequencyNode freq;

        public FNode(FrequencyNode freq) {
            super();
            this.freq = freq;
        }

        public FNode(long key, FrequencyNode freq) {
            super(key);
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
         * Removes the node from the list.
         */
        @Override
        public void remove() {
            prev.next = next;
            next.prev = prev;
            next = prev = null;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("freq", freq)
                    .toString();
        }
    }
}
