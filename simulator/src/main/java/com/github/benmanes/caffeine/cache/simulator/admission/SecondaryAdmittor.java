package com.github.benmanes.caffeine.cache.simulator.admission;

import com.github.benmanes.caffeine.cache.simulator.BasicSettings;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.ClimberResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.IncrementalResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.IndicatorResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin4.PeriodicResetCountMin4;
import com.github.benmanes.caffeine.cache.simulator.admission.countmin64.CountMin64TinyLfu;
import com.github.benmanes.caffeine.cache.simulator.admission.perfect.PerfectFrequency;
import com.github.benmanes.caffeine.cache.simulator.admission.table.RandomRemovalFrequencyTable;
import com.github.benmanes.caffeine.cache.simulator.admission.tinycache.TinyCacheAdapter;
import com.github.benmanes.caffeine.cache.simulator.policy.PolicyStats;
import com.typesafe.config.Config;

public class SecondaryAdmittor implements Admittor {

    private final BasicSettings settings;
    private final PolicyStats policyStats;
    private final Frequency sketch;
    private final boolean flipBooleanBasedOnCost;

    public SecondaryAdmittor(Config config, PolicyStats policyStats) {
        this.settings = new BasicSettings(config);
        this.policyStats = policyStats;
        this.flipBooleanBasedOnCost = settings.isCost();
        this.sketch = makeSketch(config);
    }

    public int frequency(long key) {
        return sketch.frequency(key);
    }

    private Frequency makeSketch(Config config) {
        BasicSettings settings = new BasicSettings(config);
        String type = settings.tinyLfu().sketch();
        if (type.equalsIgnoreCase("count-min-4")) {
            String reset = settings.tinyLfu().countMin4().reset();
            if (reset.equalsIgnoreCase("periodic")) {
                return new PeriodicResetCountMin4(config);
            } else if (reset.equalsIgnoreCase("incremental")) {
                return new IncrementalResetCountMin4(config);
            } else if (reset.equalsIgnoreCase("climber")) {
                return new ClimberResetCountMin4(config);
            } else if (reset.equalsIgnoreCase("indicator")) {
                return new IndicatorResetCountMin4(config);
            }
        } else if (type.equalsIgnoreCase("count-min-64")) {
            return new CountMin64TinyLfu(config);
        } else if (type.equalsIgnoreCase("random-table")) {
            return new RandomRemovalFrequencyTable(config);
        } else if (type.equalsIgnoreCase("tiny-table")) {
            return new TinyCacheAdapter(config);
        } else if (type.equalsIgnoreCase("perfect-table")) {
            return new PerfectFrequency(config);
        }
        throw new IllegalStateException("Unknown sketch type: " + type);
    }

    @Override
    public void record(long key) {
        sketch.increment(key);
    }

    @Override
    public void record(long key, int num) {
        record(key);
    }

    @Override
    public boolean admit(long candidateKey, long victimKey) {
        return true;
    }

    @Override
    public boolean admit(long candidateKey, int candidateWeight, long victimKey, int victimWeight) {
        sketch.reportMiss();

        long candidateFreq = sketch.frequency(candidateKey);
        long victimFreq = sketch.frequency(victimKey);
        if (candidateFreq > victimFreq) {
            policyStats.recordAdmission();
            return true;
        } else if (candidateFreq == victimFreq) {
            if (flipBooleanBasedOnCost) {
                if (candidateWeight > victimWeight) {
                    policyStats.recordAdmission();
                    return true;
                }
            } else {
                if (candidateWeight < victimWeight) {
                    policyStats.recordAdmission();
                    return true;
                }
            }
        }
        policyStats.recordRejection();
        return false;
    }
}
