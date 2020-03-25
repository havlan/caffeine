package com.github.benmanes.caffeine.cache.simulator.policy.linked;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.Admission;
import com.github.benmanes.caffeine.cache.simulator.admission.Admittor;
import com.github.benmanes.caffeine.cache.simulator.admission.TinyLfuBoostIncrement;
import com.github.benmanes.caffeine.cache.simulator.policy.AccessEvent;
import com.github.benmanes.caffeine.cache.simulator.policy.Policy;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Sets;
import com.typesafe.config.Config;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import org.apache.commons.lang3.StringUtils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.Set;

import static com.github.benmanes.caffeine.cache.simulator.policy.Policy.Characteristic.WEIGHTED;
import static java.util.Locale.US;
import static java.util.stream.Collectors.toSet;

public class LfuHeap implements Policy {
    final PolicyStats policyStats;
    final Long2ObjectMap<Node> data;
    AgingPolicy policy;
    final PriorityQueue<Node> pQueue;
    final int maximumSize;
    final Admittor admittor;
    int currentWeightedSize = 0;
    int elementsInPq = 0;


    public LfuHeap(Admission admission, AgingPolicy policy, Config config) {
        this.policyStats = new PolicyStats(admission.format("heap." + policy.label()));
        BasicSettings settings = new BasicSettings(config);
        this.admittor = admission.from(config, policyStats);
        this.data = new Long2ObjectOpenHashMap<>();
        this.maximumSize = settings.maximumSize();
        this.pQueue = new PriorityQueue<>(maximumSize, Comparator.comparingInt(a -> a.frequency));
        this.policy = policy;
    }

    public static Set<Policy> policies(Config config, AgingPolicy policy) {
        BasicSettings settings = new BasicSettings(config);
        return settings.admission().stream().map(admission ->
                new LfuHeap(admission, policy, config)
        ).collect(toSet());
    }

    @Override
    public Set<Characteristic> characteristics() {
        return Sets.immutableEnumSet(WEIGHTED);
    }

    long now = 0L;

    @Override
    public void record(AccessEvent event) {
        policyStats.recordOperation();
        final long key = event.key();
        Node node = data.get(key);
        if (!admittor.getClass().equals(TinyLfuBoostIncrement.class)) {
            admittor.record(key);
        } else {
            admittor.record(key, (int) Math.max(1, Math.log(event.weight() * 1.0 / 512)));
        }
        if (node == null) {
            onMiss(event);
        } else {
            onHit(node, event.weight());
        }
        now++;
        ageNodes();
    }

    //         One, Skip, None;

    private void ageNodes() {
        if (now > 10_000 && now % 100 == 0) {
            Iterator<Node> it = pQueue.iterator();
            switch (policy) {
                case One:
                    it.forEachRemaining(n -> n.frequency--);
                    break;
                case Boost:
                    it.forEachRemaining(n -> {
                        n.frequency -= getDecrement(n.weight);
                    });
                    break;
                case Skip:
                    it.forEachRemaining(n -> {
                        if (!(n.weight > 10_000)) {
                            n.frequency--;
                        }
                    });
                case None:
                    break;
            }
        }
    }

    private int getDecrement(final int weight) {
        return (int) Math.max(1, Math.log(weight) * Math.exp(-(weight * 1.0 / 512)));
    }

    private void onHit(Node node, int weight) {
        policyStats.recordWeightedHit(weight);
        node.frequency++;
    }

    private void onMiss(AccessEvent event) {
        policyStats.recordWeightedMiss(event.weight());
        if (event.weight() > maximumSize) {
            policyStats.recordOperation();
            return;
        }
        final long key = event.key();
        final int weight = event.weight();
        Node node = new Node(key, weight, getStartingFrequency());
        boolean addOperation = pQueue.offer(node);
        if (addOperation) {
            currentWeightedSize += event.weight();
            data.put(key, node);
            elementsInPq++;
            evict(node);
        } else {
            System.out.printf("FAILED TO ADD TO PQ%n");
        }
    }

    private void evict(Node candidate) {
        if (currentWeightedSize > maximumSize) {
            while (currentWeightedSize > maximumSize) {
                Node victim = pQueue.peek();
                assert victim != null;
                boolean admit = admittor.admit(candidate.key, candidate.weight, victim.key, victim.weight);
                if (admit) {
                    victim = pQueue.poll();
                    assert victim != null;
                    data.remove(victim.key);
                    currentWeightedSize -= victim.weight;
                } else {
                    pQueue.remove(candidate);
                    data.remove(candidate.key);
                    currentWeightedSize -= candidate.weight;
                }
                policyStats.recordEviction();
                elementsInPq--;
            }

        }
    }

    private int getStartingFrequency() {
        int lowerBase = 1;
        if (pQueue.peek() != null) {
            lowerBase = pQueue.peek().frequency;
            //lowerBase = lowerBase + 0; // do something random here?
        }
        return lowerBase;
    }

    @Override
    public void finished() {

    }

    @Override
    public PolicyStats stats() {
        return policyStats;
    }

    public enum AgingPolicy {
        One, Boost, None, Skip;

        public String label() {
            return StringUtils.capitalize(name().toLowerCase(US));
        }
    }

    static final class Node {
        final long key;
        int weight;
        int frequency;

        public Node(long key, int weight, int startingFrequency) {
            this.weight = weight;
            this.key = key;
            this.frequency = startingFrequency;
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("key", key)
                    .add("frequency", frequency)
                    .toString();
        }
    }
}
