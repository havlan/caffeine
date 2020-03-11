package com.github.benmanes.caffeine.cache.simulator.policy.adaptive.Lecar;

import com.google.common.math.DoubleMath;
import smile.base.cart.SplitRule;
import smile.classification.DecisionTree;
import smile.data.DataFrame;
import smile.data.Tuple;
import smile.data.type.DataType;
import smile.data.type.StructField;
import smile.data.type.StructType;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class HitRateCapturer {
    public double prevHitRate = 0.0;
    public double prevLruWeight = 0.0;
    private List<DecisionTreeBlock> hitRates;
    private String baseFileName;
    private final static int windowSize = 50;
    private static int currentIndexInWindow = 0;
    private static int hitsInWindow = 0;

    private static StructType structType = new StructType(
            new StructField("discreteTime", DataType.infer("discreteTime")),
            new StructField("currentHitRate", DataType.infer("currentHitRate")),
            new StructField("wLru", DataType.infer("wLru")),
            new StructField("wLfu", DataType.infer("wLfu")),
            new StructField("deltaHitRate", DataType.infer("deltaHitRate")),
            new StructField("deltaLruWeight", DataType.infer("deltaLruWeight"))
    );

    public HitRateCapturer(String baseFilename) {
        hitRates = new ArrayList<>();
        this.baseFileName = baseFilename;
    }

    public void flushHitRatesToFile(String filename) throws IOException {
        System.out.printf("Writing %d hit rates to file=%s%n", hitRates.size(), filename);
        try (PrintWriter writer = new PrintWriter(new File(baseFileName + filename + ".csv"))) {
            hitRates
                    .stream()
                    .map(item -> String.format("%d\t%f\t%b", (int) item.discreteTime, item.currentHitRate, item.isFlagged()))
                    .forEach(writer::println);
        }
    }

    public void captureHitRatio(boolean hit, long discreteTime) {
        currentIndexInWindow++;
        if (hit) {
            hitsInWindow++;
        }

        if (currentIndexInWindow == 200) {
            double hitRateForWindow = (hitsInWindow * 1.0 / (200 * 1.0));
            boolean labelFlagged = flagIfInteresting(
                    hitRates.size() < windowSize ? 0 : hitRates.size() - windowSize,
                    hitRates.size()
            );
            hitRates.add(new DecisionTreeBlock(discreteTime, hitRateForWindow, 0.0, 0.0, hitRateForWindow > prevHitRate, labelFlagged, hitRateForWindow - prevHitRate, 0.0));
            hitsInWindow = 0;
            currentIndexInWindow = 0;
        }
    }

    public void captureHitRatio(double currentHitRate, long discreteTime, double wLru, double wLfu) {
        if (!DoubleMath.fuzzyEquals(currentHitRate, prevHitRate, 0.000001)) { // 69
            //boolean labelFlagged = currentHitRate > prevHitRate;
            boolean labelFlagged = flagIfInteresting(
                    hitRates.size() < windowSize ? 0 : hitRates.size() - windowSize,
                    hitRates.size()
            );
            hitRates.add(new DecisionTreeBlock(discreteTime, currentHitRate, wLru, wLfu, currentHitRate > prevHitRate, labelFlagged, currentHitRate - prevHitRate, wLru - prevLruWeight));
            prevHitRate = currentHitRate;
            prevLruWeight = wLru;
        }
    }

    public boolean flagIfInteresting(int startIdx, int endIdx) {
        if (endIdx - startIdx < 3) {
            return false;
        }
        int numHitRateDecrease = 0;
        for (int i = startIdx; i < endIdx; i++) {
            if (!hitRates.get(i).increased) {
                numHitRateDecrease++;
            }
        }
        //System.out.println("HitRateCapturer.flagIfInteresting, flagged=" + shouldFlag + " numHitInc=" + numHitIncrease + " ");
        return (numHitRateDecrease * 1.0 / (endIdx - startIdx) * 1.0) > 0.7;
    }

    public static StructType getDecisionTreeStructType() {
        return structType;
    }

    public void buildDecisionTree() {
        if (!(hitRates.size() > 0)) {
            return;
        }
        int trainSize = (int) (hitRates.size() * 0.8);
        int testSize = hitRates.size() - trainSize;
        double[][] x = new double[trainSize][];
        int[] y = new int[trainSize];


        for (int i = 0; i < trainSize; i++) {
            x[i] = hitRates.get(i).toArray();
            y[i] = hitRates.get(i).isFlagged() ? 1 : 0;
        }


        double[][] testX = new double[testSize][];
        int[] testY = new int[testSize];

        int k = 0;
        for (int i = trainSize; i < hitRates.size(); i++) {
            testX[k] = hitRates.get(i).toArray();
            testY[k] = hitRates.get(i).isFlagged() ? 1 : 0;
            k++;
        }
        DecisionTree decisionTree = new DecisionTree(
                DataFrame.of(x, "discreteTime", "currentHitRate", "wLru", "wLfu", "deltaHitRate", "deltaLruWeight"),
                y,
                new StructField("inc", DataType.infer("Boolean")),
                2, SplitRule.GINI, 10, 20, 100, 4, null, null);

        int right = 0;
        int wrong = 0;
        for (int i = 0; i < testX.length; i++) {
            if (decisionTree.predict(Tuple.of(testX[i], structType)) == testY[i]) {
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
        /*
        DENCLUE scanRes = DENCLUE.fit(x, 1.0, (int) (hitRates.size() * 0.2));
        for (int i = 0; i < x.length; i++) {
            x[i] = new double[]{x[i][0], x[i][1]};
        }
        PlotCanvas plot = ScatterPlot.plot(x, scanRes.y, '.', Palette.COLORS);
        System.out.println(scanRes.toString());
        JPanel pane = new JPanel(new GridLayout(1, 2));
        pane.add(plot);
        JFrame f = new JFrame("DBSCAN");
        f.setSize(1000, 1000);
        f.setLocationRelativeTo(null);
        f.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        f.getContentPane().add(plot);
        f.setVisible(true);
         */
    }

    static class DecisionTreeBlock {
        private final long discreteTime;
        private final double currentHitRate;
        private final double wLru;
        private final double wLfu;
        private final double deltaHitRate;
        private final boolean flagged;

        public boolean isIncreased() {
            return increased;
        }

        public final boolean increased;

        public DecisionTreeBlock(long discreteTime, double currentHitRate, double wLru, double wLfu, boolean gain, boolean flagged, double deltaHitRate, double deltaLruWeight) {
            this.discreteTime = discreteTime;
            this.currentHitRate = currentHitRate;
            this.wLru = wLru;
            this.wLfu = wLfu;
            this.increased = gain;
            this.deltaHitRate = deltaHitRate;
            this.flagged = flagged;
        }

        public double[] toArray() {
            return new double[]{discreteTime, currentHitRate, deltaHitRate};
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

        public boolean isFlagged() {
            return flagged;
        }
    }
}