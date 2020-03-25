package com.github.benmanes.caffeine.cache.simulator.admission;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

import java.util.List;

public class ThresholdAdmittor implements Admittor {

    private final PolicyStats policyStats;
    private final BasicSettings settings;
    private int currentWeightThreshold = 0;
    private long sumOfWeights = 0L;
    private int stuck = 1;
    private int x = 15;

    public ThresholdAdmittor(Config config, PolicyStats policyStats) {
        this.settings = new BasicSettings(config);
        this.policyStats = policyStats;
        this.policyStats.setName(String.format("%s%d", this.policyStats.name(), x));
    }
    /*
    public static Set<Policy> policies(Config config) {
        ThresholdAdmittorSettings thresholdAdmittorSettings = new ThresholdAdmittorSettings(config);
        Set<Policy> policies = new HashSet<>();
        for (int x : thresholdAdmittorSettings.getXValues()) {
            policies.add(new ThresholdAdmittor(thresholdAdmittorSettings, new PolicyStats(String.format("%s%d", )), x));
        }
        return policies;
    }
     */

    @Override
    public void record(long key) {
    }

    @Override
    public void record(long key, int num) {
    }

    @Override
    public boolean admit(long candidateKey, long victimKey) {
        return true;
    }

    // HILL CLIMBER

    @Override
    public boolean admit(long candidateKey, int candidateWeight, long victimKey, int victimWeight) {
        if (candidateWeight > currentWeightThreshold) {
            policyStats.recordAdmission();
            sumOfWeights = sumOfWeights - victimWeight + candidateWeight;
            currentWeightThreshold = (int) (currentWeightThreshold + (sumOfWeights / settings.maximumSize()) / 2);
            stuck = 1;
            return true;
        }
        stuck += x;

        currentWeightThreshold = (int) (currentWeightThreshold + ((sumOfWeights * (100.0 - stuck)) / settings.maximumSize()) / 2);
        policyStats.recordRejection();
        return false;
    }

    public static final class ThresholdAdmittorSettings extends BasicSettings {
        public ThresholdAdmittorSettings(Config config) {
            super(config);
        }

        public List<Integer> getXValues() {
            return config().getIntList("threshold");
        }
    }
}
