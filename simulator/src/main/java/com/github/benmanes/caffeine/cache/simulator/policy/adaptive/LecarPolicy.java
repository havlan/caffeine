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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    private long discreteTime = 0L;

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
    private List<double[]> historicalWeights;
    private double[][] historyWeights;
    private int i = 0;

    public LecarPolicy(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.policyStats = new PolicyStats("adaptive.Lecar");
        this.maximumSize = settings.maximumSize();
        this.partSize = maximumSize / 2;
        this.data = new Long2ObjectOpenHashMap<>();
        double initialWeight = 0.5;
        this.weights = new double[]{initialWeight, 1.0 - initialWeight};
        this.headLru = new Node();
        this.freq0 = new FrequencyNode();
        this.learningRate = Math.pow(0.005, 1.0 / maximumSize);
        System.out.printf("LeCaR with learning rate=%f and discount rate=%f%n", learningRate, discountRate);
        this.historicalWeights = new ArrayList<>();
        //this.historyWeights = new double[settings.synthetic().events()][2];
        this.HLru = new LinkedHashMap<Long, Node>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > partSize;
            }
        };
        this.HLfu = new LinkedHashMap<Long, Node>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry eldest) {
                return size() > partSize;
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
        System.out.printf("STATE: { Data size=%d, Current Node=%s, Weights=[%f, %f], sizeLru=%d, sizeLfu=%d, sizeHLru=%d, sizeHLfu=%d }\n", data.size(), node, weights[0], weights[1], sizeLru, sizeLfu, HLru.size(), HLfu.size());
    }

    public void postCompletionFlushFile() {
        try (PrintWriter writer = new PrintWriter(new File("weights.csv"))) {
            historicalWeights
                    .stream()
                    .map(this::weightsToCsv)
                    .forEach(writer::println);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public String weightsToCsv(double[] w) {
        return String.format("%f\t%f", w[0], w[1]);
    }

    @Override
    public void finished() {
        System.out.printf("End state: { Data size=%d, SizeLru=%d, SizeLfu=%d, SizeHLru=%d, SizeHLfu=%d, Max=%d, Part=%d%n", data.size(), sizeLru, sizeLfu, HLru.size(), HLfu.size(), maximumSize, partSize);
        checkState(sizeLru == data.values().stream().filter(node -> node.type == QueueType.LRU).count());
        checkState(sizeLfu == data.values().stream().filter(node -> node.type == QueueType.LFU).count());
        checkState((sizeLru + sizeLfu) <= maximumSize);
        postCompletionFlushFile();
    }

    @Override
    public void record(long key) {
        policyStats.recordOperation();
        Node node = data.get(key);
        //printState(node);
        if (node == null) {
            onMiss(key);
        } else {
            onHit(node);
        }
        discreteTime++;
        double[] weights = getWeights();
        historicalWeights.add(weights);
        //historyWeights[i][0] = weights[0];
        //historyWeights[i][1] = weights[1];
        i++;
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

    /**
     * @param q Node
     */
    private void updateWeights(final Node q) {
        double regret = Math.pow(discountRate, discreteTime - q.historyStamp);
        double[] localWeights = getWeights();
        double learningE = Math.exp(learningRate * regret);

        // update opposite weight
        if (q.type == QueueType.LRU) {
            localWeights[1] = localWeights[1] * learningE;//* Math.exp(learningRate * regret);
        } else {
            localWeights[0] = localWeights[0] * learningE;//*Math.exp(learningRate * regret);
        }
        //System.out.printf("Updating before norm[%f, %f] lE=%f, R=%f based on node=%s with time=%d%n", localWeights[0], localWeights[1], learningE, regret, q.toString(), discreteTime - q.historyStamp);
        localWeights[0] = localWeights[0] / (localWeights[0] + localWeights[1]); // normalization
        localWeights[1] = 1.0 - localWeights[0];
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
        Node node;
        if (type == QueueType.LRU) {
            // add new LRU node
            if (foundInHistory) {
                node = q;
            } else {
                node = new Node(key);
            }
            node.type = QueueType.LRU;
            data.put(key, node);
            node.append(headLru);
            sizeLru++;
        } else {
            // add new LFU node
            FrequencyNode freq1 = (freq0.next.count == 1)
                    ? freq0.next
                    : new FrequencyNode(1, freq0);
            if (foundInHistory) {
                node = q;
                node.freq = freq1;
            } else {
                node = new Node(key, freq1);
            }
            node.type = QueueType.LFU;
            data.put(key, node);
            node.append();
            sizeLfu++;
        }

        // if the node we added results in a cache too large
        if (sizeLru + sizeLfu > maximumSize) {
            if (type == QueueType.LRU) {
                // delete LRU from cache since it's full
                Node victim = headLru.next;
                data.remove(victim.key);
                victim.remove();
                sizeLru--;
                victim.historyStamp = (int) discreteTime;
                HLru.put(victim.key, victim);
            } else {
                // delete LFU from cache since it's full, skip deleting the node we just added (from FrequentlyUsedPolicy)
                Node victim = getNextVictim(node);
                data.remove(victim.key);
                victim.remove();
                sizeLfu--;
                if (victim.freq.isEmpty()) {
                    victim.freq.remove();
                }
                victim.historyStamp = (int) discreteTime;
                HLfu.put(victim.key, victim);
            }
            policyStats.recordEviction();
        }
    }

    private Node getNextVictim(Node candidate) {
        Node victim = freq0.next.nextNode.next;
        if (victim == candidate) {
            victim = (victim.next == victim.prev)
                    ? victim.freq.next.nextNode.next
                    : victim.next;
        }
        return victim;
    }

    private void onHit(Node node) {
        policyStats.recordHit();
        //System.out.printf("Hit on node=%s\n", node);
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
        int historyStamp = 0;

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
            //System.out.printf("Appending node=%s with type=%s which is correct=%s\n", this.toString(), type, type == QueueType.LFU);
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
            if (type == QueueType.LRU) {
                checkState(key != Long.MIN_VALUE);
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
                    .add("time", historyStamp)
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

