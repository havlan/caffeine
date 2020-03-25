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
import com.google.common.math.DoubleMath;
import com.typesafe.config.Config;

import java.math.RoundingMode;
import java.util.Random;

/*
 * Copyright 2015 Ben Manes. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Admits new entries based on the estimated frequency of its historic use.
 *
 * @author havard.langdal@gmail.com (Håvard Langdal)
 */
public final class TinyLfuBoostIncrement implements Admittor {
    private final PolicyStats policyStats;
    private final Frequency sketch;
    private final Random random;

    public TinyLfuBoostIncrement(Config config, PolicyStats policyStats) {
        this.policyStats = policyStats;
        this.sketch = makeSketch(config);
        BasicSettings settings = new BasicSettings(config);
        this.random = new Random(settings.randomSeed());
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

    public int frequency(long key) {
        return sketch.frequency(key);
    }


    @Override
    public void record(long key) {
        sketch.increment(key);
    }

    @Override
    public void record(long key, int num) {
        for (int i = 0; i <num ; i++) {
            sketch.increment(key);
        }
    }


    public boolean admit(long candidateKey, int candWeight, long victimKey, int victimWeight) {
        sketch.reportMiss();

        long candidateFreq = sketch.frequency(candidateKey);
        long candidateTotal = candidateFreq * DoubleMath.log2((candWeight * 1.0), RoundingMode.DOWN);
        long victimFreq = sketch.frequency(victimKey);
        long victimTotal = victimFreq * DoubleMath.log2((victimWeight * 1.0), RoundingMode.DOWN);

        if (candidateFreq > victimFreq) {
            policyStats.recordAdmission();
            return true;
        }
        policyStats.recordRejection();
        return false;
    }

    @Override
    public boolean admit(long candidateKey, long victimKey) {
        sketch.reportMiss();

        long candidateFreq = sketch.frequency(candidateKey);
        long victimFreq = sketch.frequency(victimKey);
        if (candidateFreq > victimFreq) {
            policyStats.recordAdmission();
            return true;
        }
        policyStats.recordRejection();
        return false;
    }
}
