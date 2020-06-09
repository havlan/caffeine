package com.github.benmanes.caffeine.cache.simulator.admission;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

import java.util.List;

public class ThresholdAdmittor implements Admittor {

    private final PolicyStats policyStats;
    private final BasicSettings settings;
    private final boolean flipBooleanBasedOnCost;
    private double currentWeightThreshold = 0;
    private long sumOfWeights = 0L;
    private double stuck = 1.0;
    private double x = 0.01;

    public ThresholdAdmittor(Config config, PolicyStats policyStats) {
        this.settings = new BasicSettings(config);
        this.policyStats = policyStats;
        this.policyStats.setName(String.format("%s", this.policyStats.name()));
        this.flipBooleanBasedOnCost = settings.isCost();
        currentWeightThreshold = flipBooleanBasedOnCost ? Double.MIN_VALUE : Double.MAX_VALUE;

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
        if (flipBooleanBasedOnCost) {
            if (candidateWeight > currentWeightThreshold) {
                policyStats.recordAdmission();
                sumOfWeights = sumOfWeights - victimWeight + candidateWeight;
                stuck = 1.0;
                currentWeightThreshold = getNewThreshold(sumOfWeights, stuck);
                return true;
            }
            stuck -= x;
        } else {
            if (candidateWeight < currentWeightThreshold) {
                policyStats.recordAdmission();
                sumOfWeights = sumOfWeights - victimWeight + candidateWeight;
                stuck = 1.0;
                currentWeightThreshold = getNewThreshold(sumOfWeights, stuck);
                return true;
            }
            stuck += x;
        }
        currentWeightThreshold = getNewThreshold(sumOfWeights, stuck);
        policyStats.recordRejection();
        return false;
    }

    public double getNewThreshold(final long sumOfWeights, final double tip) {
        double adjusted = sumOfWeights * tip;
        double avgAdjusted = adjusted / settings.maximumSize();
        double sumAdjusted = currentWeightThreshold + avgAdjusted;
        currentWeightThreshold = sumAdjusted / 2.0;
        return currentWeightThreshold;
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
