package com.github.benmanes.caffeine.cache.simulator.policy.adaptive.Lecar;

import smile.classification.DecisionTree;
import smile.data.Tuple;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.benmanes.caffeine.cache.simulator.policy.adaptive.Lecar.HitRateCapturer.getDecisionTreeStructType;
import static org.apache.commons.math3.special.Erf.erf;

enum AdaptionMode {
    D3,
    SLIDING,
    None
}

class Adaptor {
    enum WindowType {
        AVGWINDOW,
        CHAUVENET,
        HILLCLIMB
        // The Welford’s algorithm
        // The quartiles-based solution
        // The z-score metric-based solution
        // Half-Space Trees
        // Local Outlier Factor
        // Grubb test
        // Tietjen y Moore
    }

    public AdaptionMode getAdaptionMode() {
        return adaptionMode;
    }

    private AdaptionMode adaptionMode;

    // file utilities
    private final String baseFileName = "C:\\Users\\havar\\Home\\cache_simulation_results\\";
    private boolean discreTizeWeights = false;
    private List<double[]> historicalAnomalies;
    private long numAnomaliesFlagged = 0L;
    private boolean visualizing = false;

    private WindowType type;
    // window attrs
    private double[] slidingWindow;
    private boolean currentWindowFlagged = false;
    private double sumInWindow = 0.0;
    private long lastFlaggedDiscreteTime = 0L;
    private double criterion;
    private final double eps = 0.075;
    boolean anomalyMode = false;

    // D3
    private DecisionTree decisionTree;

    public double getPrevHitRate() {
        return prevHitRate;
    }

    public double getPrevLruWeight() {
        return prevLruWeight;
    }

    private double prevHitRate = 0.0;
    private double prevLruWeight = 0.0;

    public int getAmountOfIncreaseLr() {
        return amountOfIncreaseLr;
    }

    public int getAmountOfKeepLr() {
        return amountOfKeepLr;
    }

    private int amountOfIncreaseLr = 0;
    private int amountOfKeepLr = 0;


    public Adaptor(AdaptionMode adaptionMode) {
        this.slidingWindow = new double[10];
        this.historicalAnomalies = new ArrayList<>();
        this.criterion = 1.0 / (2.0 * slidingWindow.length);
        this.type = WindowType.AVGWINDOW;
        this.adaptionMode = adaptionMode;
        Arrays.fill(slidingWindow, 0.0);
        readDecisionTreeFromFile();
    }

    public Adaptor(int size) {
        slidingWindow = new double[size];
        this.historicalAnomalies = new ArrayList<>();
        this.criterion = 1.0 / (2.0 * slidingWindow.length);
        this.type = WindowType.AVGWINDOW;
        Arrays.fill(slidingWindow, 0.0);
        readDecisionTreeFromFile();
    }

    void readDecisionTreeFromFile() {
        System.out.printf("Trying to read file %s %n", baseFileName + "d3.model");
        try (ObjectInputStream reader = new ObjectInputStream(new FileInputStream(baseFileName + "d3.model"))) {
            this.decisionTree = (DecisionTree) reader.readObject();
        } catch (IOException | ClassNotFoundException e) {
            System.out.println("LecarPolicy.readDecisionTreeFromFile: " + e.toString());
        }
    }

    public boolean getCurrentWindowFlagged() {
        return currentWindowFlagged;
    }

    public long getNumAnomaliesFlagged() {
        return numAnomaliesFlagged;
    }

    public void setType(WindowType type) {
        switch (type) {
            case CHAUVENET:
                break;
            case AVGWINDOW:
                break;
            case HILLCLIMB:
                break;
        }
        this.type = type;
    }

    public WindowType getType() {
        return type;
    }

    public double adapt(long discreteTime, double hitRate, double wLru, double wLfu, double deltaHitRate, double deltaLruWeight) {
        double adjustedWeight = 0.3;

        switch (adaptionMode) {
            case D3:
                if (decisionTree.predict(Tuple.of(new double[]{discreteTime, hitRate, wLru, wLfu, deltaHitRate, deltaLruWeight}, getDecisionTreeStructType())) == 1) {
                    amountOfKeepLr++;
                } else {
                    amountOfIncreaseLr++;
                    adjustedWeight = 0.9; // detected boost
                }
                break;

            case SLIDING:
                if (detectAnomalyInWindow(wLru, discreteTime)) {
                    amountOfIncreaseLr++;
                    adjustedWeight = 0.9;
                } else {
                    amountOfKeepLr++;
                }
                break;

            case None:
                break;

        }
        prevHitRate = hitRate;
        prevLruWeight = wLru;
        return adjustedWeight;
    }

    /**
     * @param currentWeight
     * @param discreteTime
     * @return Husk at man må skille på oppdateringen som skjer når man bruker vektene i lecar og hitrate. Ikke "oppmuntre"
     * en negativ endring i hit rate ved å endre lr.
     */
    public boolean detectAnomalyInWindow(double currentWeight, long discreteTime) {
        if (!currentWindowFlagged) {

            switch (type) {
                case CHAUVENET: {
                    // 1. Calculate mean
                    double avgInWindow = sumInWindow / (slidingWindow.length * 1.0);

                    // 2. Calculate standard deviation
                    double stdeviation = calculateStDeviation(avgInWindow);

                    // 3. Calculate diffs from window
                    double diff = Math.abs(currentWeight - avgInWindow) / stdeviation;

                    // 4. Compare with nd
                    //NormalDistribution normalDistribution = new NormalDistribution(avgInWindow, stdeviation);
                    double prob = erf(diff);

                    if (prob < criterion) {
                        numAnomaliesFlagged++;
                        currentWindowFlagged = true;
                        lastFlaggedDiscreteTime = discreteTime;
                    }
                    break;
                }
                case AVGWINDOW: {
                    double avgInWindow = sumInWindow / (slidingWindow.length * 1.0);
                    if (Math.abs(currentWeight - avgInWindow) > eps) {
                        currentWindowFlagged = true;
                        numAnomaliesFlagged++;
                        lastFlaggedDiscreteTime = discreteTime;
                    }
                    break;
                }
                case HILLCLIMB: {
                    double prevWeight = 0.0;
                    if (currentWeight > prevWeight) {
                        currentWindowFlagged = true;
                        numAnomaliesFlagged++;
                        lastFlaggedDiscreteTime = discreteTime;
                    }
                    break;
                }
                default:
                    break;

            }
            if (currentWindowFlagged) {
                if (visualizing) {
                    historicalAnomalies.add(new double[]{discreteTime, currentWeight});
                }
            }
        } else {
            // if the previos anomaly timestamp is less than 10 ago
            if (discreteTime - lastFlaggedDiscreteTime >= slidingWindow.length) {
                currentWindowFlagged = false;
            }
        }
        // update sum, add new weight, remove oldest from window.
        sumInWindow = sumInWindow + currentWeight - slidingWindow[(int) (discreteTime % slidingWindow.length)];
        slidingWindow[(int) (discreteTime % slidingWindow.length)] = currentWeight;
        return currentWindowFlagged;
    }

    // std = sqrt(mean(abs(x - x.mean())**2))
    private double calculateStDeviation(double mean) {
        double runningSum = 0.0;
        for (double v : slidingWindow) {
            runningSum += Math.pow(Math.abs(v - mean), 2.0);
        }
        return Math.sqrt(runningSum / (slidingWindow.length * 1.0));
    }

    public void flushAnomaliesToFile(String filename) throws IOException {
        System.out.printf("Writing anomalies to file=%s%n", filename);
        try (PrintWriter writer = new PrintWriter(new File(baseFileName + filename + "a.csv"), Charset.defaultCharset())) {
            historicalAnomalies
                    .stream()
                    .map(item ->
                            discreTizeWeights ?
                                    String.format("%d\t%d", (int) item[0], (int) (10000 * item[1])) :
                                    String.format("%d\t%f", (int) item[0], item[1]))
                    .forEach(writer::println);
        }
    }
}