package com.github.benmanes.caffeine.cache.simulator.admission;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

public class ComparisonAdmittor implements Admittor {

    private final PolicyStats policyStats;
    private final BasicSettings settings;
    private long prevVictimKey = 0L;
    private int prevVictimWeight = 0;
    private int iterationsWithSame = 1;

    public ComparisonAdmittor(Config config, PolicyStats policyStats) {
        this.settings = new BasicSettings(config);
        this.policyStats = policyStats;
    }


    @Override
    public void record(long key) {
    }

    @Override
    public void record(long key, int num) {

    }

    @Override
    public boolean admit(long candidateKey, long victimKey) {
        return false;
    }

    @Override
    public boolean admit(long candidateKey, int candidateWeight, long victimKey, int victimWeight) {
        if (victimKey == prevVictimKey && victimWeight == prevVictimWeight) {
            iterationsWithSame++;
            victimWeight /= iterationsWithSame;
        } else {
            iterationsWithSame = 1;
        }

        prevVictimKey = victimKey;
        prevVictimWeight = victimWeight;
        if (candidateWeight > victimWeight) {
            policyStats.recordAdmission();
            return true;
        }
        //System.out.printf("Not admitted %s cand=%d vict=%d%n", policyStats.name(), candidateWeight, victimWeight);
        policyStats.recordRejection();
        return false;
    }
}
