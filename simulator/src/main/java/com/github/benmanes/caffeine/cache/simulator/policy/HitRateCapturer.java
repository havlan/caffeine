package com.github.benmanes.caffeine.cache.simulator.policy;

import com.google.common.math.DoubleMath;
import smile.classification.DecisionTree;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class HitRateCapturer {
    public double prevHitRate = 0.0;
    public double prevLruWeight = 0.0;
    private List<DecisionTreeBlock> hitRates;
    private String baseFileName;

    public HitRateCapturer(String baseFilename) {
        hitRates = new ArrayList<>();
        this.baseFileName = baseFilename;
    }

    public void flushHitRatesToFile(String filename) throws IOException {
        System.out.printf("Writing %d hit rates to file=%s%n", hitRates.size(), filename);
        try (PrintWriter writer = new PrintWriter(new File(baseFileName + filename + "r.csv"), Charset.defaultCharset())) {
            hitRates
                    .stream()
                    .map(item -> String.format("%d\t%f", (int) item.discreteTime, item.currentHitRate))
                    .forEach(writer::println);
        }
    }

    public void captureHitRatio(double currentHitRate, long discreteTime, double wLru, double wLfu) {
        if (!DoubleMath.fuzzyEquals(currentHitRate, prevHitRate, 0.000001)) { // 69
            boolean increased = currentHitRate > prevHitRate;
            hitRates.add(new DecisionTreeBlock(discreteTime, currentHitRate, wLru, wLfu, increased, currentHitRate - prevHitRate, wLru - prevLruWeight));
            prevHitRate = currentHitRate;
            prevLruWeight = wLru;
        }
    }

    public void buildDecisionTree() {
        int trainSize = (int) (hitRates.size() * 0.8);
        int testSize = hitRates.size() - trainSize;
        double[][] x = new double[trainSize][];
        int[] y = new int[trainSize];

        for (int i = 0; i < trainSize; i++) {
            x[i] = hitRates.get(i).toArray();
            y[i] = hitRates.get(i).isIncreased() ? 1 : 0;
        }


        double[][] testX = new double[testSize][];
        int[] testY = new int[testSize];

        int k = 0;
        for (int i = trainSize; i < hitRates.size(); i++) {
            testX[k] = hitRates.get(i).toArray();
            testY[k] = hitRates.get(i).isIncreased() ? 1 : 0;
            k++;
        }

        DecisionTree decisionTree = new DecisionTree(x, y, 10);

        int right = 0;
        int wrong = 0;
        for (int i = 0; i < testX.length; i++) {
            if (decisionTree.predict(testX[i]) == testY[i]) {
                right++;
            } else {
                wrong++;
            }
        }
        System.out.printf("Predicted test set with right=%d, wrong=%d and acc=%f%n", right, wrong, (right * 1.0 / testX.length * 1.0));
        System.out.println(decisionTree.dot());
        try (PrintWriter writer = new PrintWriter(new File(baseFileName + "d3.gv"), Charset.defaultCharset())) {
            writer.write(decisionTree.dot());
        } catch (IOException e) {
            System.out.println("HitRateCapturer.buildDecisionTree: " + e.toString());
        }
        try (ObjectOutputStream writer = new ObjectOutputStream(new FileOutputStream(baseFileName + "d3.model"))) {
            writer.writeObject(decisionTree);
        } catch (IOException e) {
            System.out.println("HitRateCapturer.buildDecisionTree: " + e.toString());
        }
    }

    static class DecisionTreeBlock {
        private final long discreteTime;
        private final double currentHitRate;
        private final double wLru;
        private final double wLfu;
        private final double deltaHitRate;
        private final double deltaLruWeight;

        public boolean isIncreased() {
            return increased;
        }

        private final boolean increased;

        public DecisionTreeBlock(long discreteTime, double currentHitRate, double wLru, double wLfu, boolean gain, double deltaHitRate, double deltaLruWeight) {
            this.discreteTime = discreteTime;
            this.currentHitRate = currentHitRate;
            this.wLru = wLru;
            this.wLfu = wLfu;
            this.increased = gain;
            this.deltaHitRate = deltaHitRate;
            this.deltaLruWeight = deltaLruWeight;
        }

        public double[] toArray() {
            return new double[]{discreteTime, currentHitRate, wLru, wLfu, deltaHitRate, deltaLruWeight};
        }

        @Override
        public String toString() {
            return "DecisionTreeBlock{" +
                    "discreteTime=" + discreteTime +
                    ", currentHitRate=" + currentHitRate +
                    ", wLru=" + wLru +
                    ", wLfu=" + wLfu +
                    ", gain=" + increased +
                    '}';
        }
    }
}