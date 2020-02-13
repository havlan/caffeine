package com.github.benmanes.caffeine.cache.simulator.policy.adaptive;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;


import static com.google.common.base.Preconditions.checkState;

public class LecarPolicy implements Policy.KeyOnlyPolicy {

    private final Long2ObjectMap<Node> data;
    private final Node headLru;
    private final FrequencyNode freq0;
    private final PolicyStats policyStats;
    private final int maximumSize;
    private final int partSize;
    private final Node headGhostLru;
    private final Node headGhostLfu;
    private int sizeLru = 0;
    private int sizeLfu = 0;
    private int sizeGhostLru = 0;
    private int sizeGhostLfu = 0;
    private long discreteTime = 0L;

    public double[] getWeights() {
        return weights;
    }

    public void setWeights(double[] weights) {
        this.weights = weights;
    }

    public double[] slidingWindow = new double[10];


    private double[] weights;
    private double[] prevWeights;
    private Random generator;
    private final double defaultLearningRate = 0.3;
    private double learningRate = defaultLearningRate;
    private final double defaultDiscountRate;
    private double discountRate;
    private List<double[]> historicalWeights;
    private int weightNotChangedFor = 0;
    private boolean visualizeMode = true;
    private boolean discreTizeWeights = true;

    public LecarPolicy(Config config) {
        BasicSettings settings = new BasicSettings(config);
        this.policyStats = new PolicyStats("adaptive.Lecar");
        this.maximumSize = settings.maximumSize();
        this.partSize = maximumSize / 2;
        this.data = new Long2ObjectOpenHashMap<>();
        double initialWeight = 0.5;
        this.weights = new double[]{initialWeight, 1.0 - initialWeight};
        this.prevWeights = this.weights;

        this.headLru = new Node();
        this.freq0 = new FrequencyNode();
        this.headGhostLru = new Node();
        this.headGhostLfu = new Node();
        this.historicalAnomalies = new ArrayList<>();

        this.defaultDiscountRate = Math.pow(0.005, 1.0 / maximumSize);
        this.discountRate = defaultDiscountRate;
        System.out.printf("LeCaR with learning rate=%f and discount rate=%f%n", learningRate, discountRate);
        this.historicalWeights = new ArrayList<>();
        this.generator = new Random(settings.randomSeed());

        Arrays.fill(slidingWindow, 0.5);
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
        System.out.printf("STATE: { Data size=%d, Current Node=%s, State Ok=%s Weights=[%f, %f], sizeLru=%d, sizeLfu=%d, sizeHLru=%d, sizeHLfu=%d }\n", data.size(), node, data.size() == (sizeGhostLfu + sizeGhostLru + sizeLru + sizeLfu), weights[0], weights[1], sizeLru, sizeLfu, sizeGhostLru, sizeGhostLfu);
    }

    public void postCompletionFlushFile() throws FileNotFoundException {
        String baseFileName = "C:\\Users\\havar\\Home\\cache_simulation_results\\";
        try (PrintWriter writer = new PrintWriter(new File(baseFileName + "scaled_w_01.csv"))) {
            historicalWeights
                    .stream()
                    .map(discreTizeWeights ? this::discreteWeightsToCsv : this::weightsToCsv)
                    .forEach(writer::println);
        } catch (FileNotFoundException e) {
            throw e;
        }

        try (PrintWriter writer = new PrintWriter(new File(baseFileName + "scaled_a_01.csv"))) {
            historicalAnomalies
                    .stream()
                    .map(item ->
                            discreTizeWeights ?
                                    String.format("%d\t%d", (int) item[0], (int) (10000 * item[1])) :
                                    String.format("%d\t%f", (int) item[0], item[1]))
                    .forEach(writer::println);
        }
    }

    long numWeightsAddedToFile = 0L;

    // if weights are updated, we add them
    public void updateWeightsForIteration() {
        double[] weights = getWeights();
        if (!Arrays.equals(weights, prevWeights)) {
            //System.out.printf("Weights [%f, %f] were not equal to [%f, %f]%n", weights[0], weights[1], prevWeights[0], prevWeights[1]);
            double[] weightsWithFrequency = {discreteTime, weights[0], weights[1], weightNotChangedFor};
            historicalWeights.add(weightsWithFrequency);
            numWeightsAddedToFile++;
            weightNotChangedFor = 0;
        } else {
            weightNotChangedFor++;
        }
    }


    /**
     * @param w
     * @return Formatted string with discrete timestamp, weights and frequency of those weights to reduce csv size
     */
    public String discreteWeightsToCsv(double[] w) {
        return String.format("%d\t%d\t%d\t%d", (int) w[0], (int) (10_000 * w[1]), (int) (10_000 * w[2]), (int) w[3]);
    }

    /**
     * @param w
     * @return Formatted string with discrete timestamp, weights and frequency of those weights to reduce csv size
     */
    public String weightsToCsv(double[] w) {
        return String.format("%d\t%f\t%f\t%d", (int) w[0], w[1], w[2], (int) w[3]);
    }

    @Override
    public void finished() {
        System.out.printf("END STATE {anomalies=%d, numToFile=%d}%n", numPseudoAnomalies, numWeightsAddedToFile);
        checkState(sizeLru == data.values().stream().filter(node -> node.type == QueueType.LRU).count());
        checkState(sizeLfu == data.values().stream().filter(node -> node.type == QueueType.LFU).count());
        checkState(sizeGhostLru == data.values().stream().filter(node -> node.type == QueueType.GRU).count());
        checkState(sizeGhostLfu == data.values().stream().filter(node -> node.type == QueueType.GFU).count());
        checkState((sizeLru + sizeLfu) <= maximumSize);
        if (visualizeMode) {
            try {
                postCompletionFlushFile();
            } catch (FileNotFoundException e) {
                System.out.println("Issues running postCompletionFlushFile with e=" + e.toString());
            }
        }
    }

    @Override
    public void record(long key) {
        policyStats.recordOperation();
        Node node = data.get(key);
        if (node == null) {
            // if we miss
            onMiss(key);
        } else if (node.type == QueueType.GRU || node.type == QueueType.GFU) {
            // if we hit a ghost list
            ohGhostHit(node);
        } else {
            // if we hit
            onCacheHit(node);
        }
        if (visualizeMode) {
            updateWeightsForIteration();
        }
        //checkSlidingWindow();
        //updateAdaptivityState();

        discreteTime++;
    }

    private void updateAdaptivityState() {
        if (anomalyLastTen) {
            learningRate = 1.0;
        } else {
            learningRate = defaultLearningRate;
            discountRate = defaultDiscountRate;
        }
    }

    long numPseudoAnomalies = 0L;

    private List<double[]> historicalAnomalies;
    boolean anomalyLastTen = false;

    public void checkSlidingWindow() {
        // update the siding window
        double avgInWindow = Arrays.stream(slidingWindow).sum() / (slidingWindow.length + 0.0);
        if (Math.abs(weights[0] - avgInWindow) > 0.075 && !anomalyLastTen) {
            numPseudoAnomalies++;
            historicalAnomalies.add(new double[]{discreteTime, weights[0]});
            anomalyLastTen = true;
        }

        if (historicalAnomalies.size() > 0) {
            // if the previos anomaly timestamp is less than 10 ago
            if (discreteTime - (int) historicalAnomalies.get(historicalAnomalies.size() - 1)[0] >= 10) {
                anomalyLastTen = false;
            }
        }
        slidingWindow[(int) (discreteTime % slidingWindow.length)] = weights[0];
    }

    private void ohGhostHit(Node q) {
        policyStats.recordMiss();
        boolean foundInHistory = false;
        if (q.type == QueueType.GRU) {
            q.remove(); // unlink
            sizeGhostLru--;
            foundInHistory = true;
        } else if (q.type == QueueType.GFU) {
            q.remove();
            sizeGhostLfu--;
            foundInHistory = true;
        }

        // if we found in history, adjust weights
        if (foundInHistory) {
            updateState(q);
            addNodeFromGhost(q);
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
        return type;
    }

    /**
     * @param q Node
     */
    private void updateState(final Node q) {
        double regret = Math.pow(discountRate, discreteTime - q.historyStamp);
        double[] localWeights = getWeights();
        this.prevWeights = Arrays.copyOf(localWeights, 2);
        if (q.type == QueueType.GRU) {
            localWeights[1] = localWeights[1] * Math.exp(learningRate * regret);
        } else if (q.type == QueueType.GFU) {
            localWeights[0] = localWeights[0] * Math.exp(learningRate * regret);
        }
        localWeights[0] = localWeights[0] / (localWeights[0] + localWeights[1]); // normalization
        localWeights[0] = BigDecimal.valueOf(localWeights[0]).setScale(4, RoundingMode.HALF_UP).doubleValue();
        localWeights[1] = 1.0 - localWeights[0];
        setWeights(localWeights);
    }

    // called when a cache miss (not in ghost lists either) occurs
    private Node addToCache(long key, QueueType type) {
        Node node;
        if (type == QueueType.LRU) {
            node = new Node(key);
            node.type = QueueType.LRU;
            data.put(key, node);
            node.append(headLru);
            sizeLru++;
        } else if (type == QueueType.LFU) {
            FrequencyNode freq1 = (freq0.next.count == 1)
                    ? freq0.next
                    : new FrequencyNode(1, freq0);
            node = new Node(key, freq1);
            node.type = QueueType.LFU;
            data.put(key, node);
            node.append();
            sizeLfu++;
        } else {
            throw new IllegalArgumentException("Fuck!");
        }
        return node;
    }


    // add an existing node from the ghost lists to the cache again
    private void addNodeFromGhost(Node node) {
        QueueType type = pickCacheType();
        if (type == QueueType.LRU) {
            node.type = QueueType.LRU;
            node.append(headLru);
            sizeLru++;
        } else {
            FrequencyNode freq1 = (freq0.next.count == 1)
                    ? freq0.next
                    : new FrequencyNode(1, freq0);
            node.type = QueueType.LFU;
            node.freq = freq1;
            node.append();
            sizeLfu++;
        }
        cleanupCache(type, node);
        cleanupGhost(type);
    }

    private void cleanupCache(QueueType type, Node candidate) {
        if (type == QueueType.LRU) {
            cleanupCache();
        } else if (type == QueueType.LFU) {
            cleanupCache(candidate);
        }
    }

    // Only called from LRU parts
    private void cleanupCache() {
        if (sizeLru + sizeLfu > maximumSize) {
            Node victim = headLru.next;
            sizeLru--;
            victim.remove();
            victim.historyStamp = (int) discreteTime;
            victim.type = QueueType.GRU;
            victim.append(headGhostLru);
            sizeGhostLru++;
            policyStats.recordEviction();
        }
    }

    // Only called from LFU parts
    private void    cleanupCache(Node candidate) {
        if (sizeLru + sizeLfu > maximumSize) {
            Node victim = getNextVictim(candidate);
            victim.remove();
            sizeLfu--;
            if (victim.freq.isEmpty()) {
                victim.freq.remove();
            }
            victim.historyStamp = (int) discreteTime;
            victim.type = QueueType.GFU;
            victim.append(headGhostLfu);
            sizeGhostLfu++;
            policyStats.recordEviction();
        }
    }


    private void onMiss(long key) {
        policyStats.recordMiss();
        QueueType type = pickCacheType();
        Node node = addToCache(key, type);
        cleanupCache(type, node);
        cleanupGhost(type);
    }

    private void cleanupGhost(QueueType type) {
        if (type == QueueType.LRU) {
            if (sizeGhostLru > partSize) {
                Node victim = headGhostLru.next;
                data.remove(victim.key);
                victim.remove();
                sizeGhostLru--;
            }
        } else if (type == QueueType.LFU) {
            if (sizeGhostLfu > partSize) {
                Node victim = headGhostLfu.next;
                data.remove(victim.key);
                victim.remove();
                sizeGhostLfu--;
            }
        } else {
            throw new IllegalArgumentException("Wrong args to cleanup cache");
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

    private void onCacheHit(Node node) {
        policyStats.recordHit();
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
        GRU,
        GFU
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
        int historyStamp = 0; // discrete time when added to the cache

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
            if (type != QueueType.LFU) {
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

    static class Replayer {
        private double[] discreteTimeEventsMarked;
        private double[] oldWeights;
        private double[] newWeights;
        private int slidingWindowLength;

    }
}

