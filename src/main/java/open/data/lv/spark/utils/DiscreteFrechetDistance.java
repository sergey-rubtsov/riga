package open.data.lv.spark.utils;

// This class will compute the Discrete Frechet Distance given two sets of time
// series by using dynamic programming to increase performance.
//
// How to run: Run the file, and the console will ask for the first and second
// time series which should be supplied as a String. Each time series is given
// as pairs of values separated by semicolons and the values for each pair are
// separated by a comma.
//
// Pseudocode of computing DFD from page 5 of
// http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
//
// @author - Stephen Bahr (sbahr@bu.edu)

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

public class DiscreteFrechetDistance {

    /** Dimensions of the time series */
    private static int DIM = 2;
    /** Dynamic programming memory array */
    private static double[][] mem;
    /** First time series */
    private static List<Point> timeSeriesP;
    /** Second time series */
    private static List<Point> timeSeriesQ;

    public static void main(String[] args) {

        Point p0 = new Point(new double[]{0.0, 0.0});
        Point p1 = new Point(new double[]{1.0, 0.0});
        Point p2 = new Point(new double[]{1.0, 1.0});
        Point p3 = new Point(new double[]{0.0, 2.0});
        Point p4 = new Point(new double[]{2.0, 3.0});

        Point q0 = new Point(new double[]{1.0, 0.0});
        Point q1 = new Point(new double[]{3.0, 0.0});
        Point q2 = new Point(new double[]{3.0, 2.0});
        Point q3 = new Point(new double[]{0.0, 3.0});
        Point q4 = new Point(new double[]{1.0, 3.0});
        timeSeriesP = new ArrayList<>();
        timeSeriesQ = new ArrayList<>();

        timeSeriesP.add(p0);
        timeSeriesP.add(p1);
        timeSeriesP.add(p2);
        timeSeriesP.add(p3);
        timeSeriesP.add(p4);

        timeSeriesQ.add(q0);
        timeSeriesQ.add(q1);
        timeSeriesQ.add(q2);
        timeSeriesQ.add(q3);
        timeSeriesQ.add(q4);
        long startTime = System.currentTimeMillis();
        double res = computeDiscreteFrechet(timeSeriesP, timeSeriesQ);
        long runTime = System.currentTimeMillis() - startTime;
        System.out.println("The Discrete Frechet Distance between these two time series is "
                + res + ". (" + runTime + " ms)");
        timeSeriesP = new ArrayList<>();
        timeSeriesQ = new ArrayList<>();

        timeSeriesP.add(p0);
        timeSeriesP.add(p1);
        timeSeriesP.add(p2);
        timeSeriesP.add(p3);
        timeSeriesP.add(p4);

        timeSeriesQ.add(p4);
        timeSeriesQ.add(p3);
        timeSeriesQ.add(p2);
        timeSeriesQ.add(p1);
        timeSeriesQ.add(p0);
        startTime = System.currentTimeMillis();
        res = computeDiscreteFrechet(timeSeriesP, timeSeriesQ);
        runTime = System.currentTimeMillis() - startTime;
        System.out.println("The Discrete Frechet Distance between these two time series is "
                + res + ". (" + runTime + " ms)");

        timeSeriesP = new ArrayList<>();
        timeSeriesQ = new ArrayList<>();

        timeSeriesP.add(p0);
        timeSeriesP.add(p1);
        timeSeriesP.add(p2);
        timeSeriesP.add(p3);
        timeSeriesP.add(p4);

        timeSeriesQ.add(p0);
        timeSeriesQ.add(p1);
        timeSeriesQ.add(p2);
        timeSeriesQ.add(p3);
        timeSeriesQ.add(new Point(new double[]{2.0, 3.0}));
        startTime = System.currentTimeMillis();
        res = computeDiscreteFrechet(timeSeriesP, timeSeriesQ);
        runTime = System.currentTimeMillis() - startTime;
        System.out.println("The Discrete Frechet Distance between these two time series is "
                + res + ". (" + runTime + " ms)");
    }

    /**
     * Wrapper that makes a call to computeDFD. Initializes mem array with all
     * -1 values.
     *
     * @param P - the first time series
     * @param Q - the second time series
     *
     * @return The length of the shortest distance that can traverse both time
     *         series.
     */
    private static double computeDiscreteFrechet(List<Point> P, List<Point> Q) {

        mem = new double[P.size()][Q.size()];

        // initialize all values to -1
        for (int i = 0; i < mem.length; i++) {
            for (int j = 0; j < mem[i].length; j++) {
                mem[i][j] = -1.0;
            }
        }

        return computeDFD(P.size() - 1, Q.size() - 1);
    }

    /**
     * Compute the Discrete Frechet Distance (DFD) given the index locations of
     * i and j. In this case, the bottom right hand corner of the mem two-d
     * array. This method uses dynamic programming to improve performance.
     *
     * Pseudocode of computing DFD from page 5 of
     * http://www.kr.tuwien.ac.at/staff/eiter/et-archive/cdtr9464.pdf
     *
     * @param i - the row
     * @param j - the column
     *
     * @return The length of the shortest distance that can traverse both time
     *         series.
     */
    private static double computeDFD(int i, int j) {

        // if the value has already been solved
        if (mem[i][j] > -1)
            return mem[i][j];
            // if top left column, just compute the distance
        else if (i == 0 && j == 0)
            mem[i][j] = euclideanDistance(timeSeriesP.get(i), timeSeriesQ.get(j));
            // can either be the actual distance or distance pulled from above
        else if (i > 0 && j == 0)
            mem[i][j] = max(computeDFD(i - 1, 0), euclideanDistance(timeSeriesP.get(i), timeSeriesQ.get(j)));
            // can either be the distance pulled from the left or the actual
            // distance
        else if (i == 0 && j > 0)
            mem[i][j] = max(computeDFD(0, j - 1), euclideanDistance(timeSeriesP.get(i), timeSeriesQ.get(j)));
            // can be the actual distance, or distance from above or from the left
        else if (i > 0 && j > 0) {
            mem[i][j] = max(min(computeDFD(i - 1, j), computeDFD(i - 1, j - 1), computeDFD(i, j - 1)), euclideanDistance(timeSeriesP.get(i), timeSeriesQ.get(j)));
        }
        // infinite
        else
            mem[i][j] = Integer.MAX_VALUE;

        // printMemory();
        // return the DFD
        return mem[i][j];
    }

    /**
     * Get the max value of all the values.
     *
     * @param values - the values being compared
     *
     * @return The max value of all the values.
     */
    private static double max(double... values) {
        double max = Integer.MIN_VALUE;
        for (double i : values) {
            if (i >= max)
                max = i;
        }
        return max;
    }

    /**
     * Get the minimum value of all the values.
     *
     * @param values - the values being compared
     *
     * @return The minimum value of all the values.
     */
    private static double min(double... values) {
        double min = Integer.MAX_VALUE;
        for (double i : values) {
            if (i <= min)
                min = i;
        }
        return min;
    }

    /**
     * Given two points, calculate the Euclidean distance between them, where
     * the Euclidean distance: sum from 1 to n dimensions of ((x - y)^2)^1/2
     *
     * @param i - the first point
     * @param j - the second point
     *
     * @return The total Euclidean distance between two points.
     */
    private static double euclideanDistance(Point i, Point j) {

        double distance = 0;
        // for each dimension
        for (int n = 0; n < DIM; n++) {
            distance += Math.sqrt(Math.pow(Math.abs((i.dimensions[n] - j.dimensions[n])), 2));
        }

        return distance;
    }

}

