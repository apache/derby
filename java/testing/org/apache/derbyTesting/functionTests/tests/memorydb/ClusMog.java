/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.ClusMog

   Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
 */

package org.apache.derbyTesting.functionTests.tests.memorydb;

import java.util.Arrays;
import java.util.Random;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Simple utility to compute/recover the parameters of a mixture-of-Gaussian
 * distribution from independent samples.
 */
public class ClusMog
{
  /** default constructor */
  public ClusMog() {}

  /**
   * Compute/recover the parameters of a mixture-of-Gaussian distribution
   * from given independent samples.
   * @param n number of clusters (Gaussian components) to output
   * @param center initial cluster centers for iterative refinement
   * @param ns number of input samples
   * @param sample input samples; will be sorted in ascending order during use
   */
  public void cluster(int n, double center[], int ns, double sample[])
  {
    // Record input parameters.
    setCenters(n, center);
    setSamples(ns, sample);
    // Initialize EM iterations.
    initEM();
    // Perform EM iterations until convergence.
    final double thresh = 1.0e-6;
    double oldmsr = Double.MAX_VALUE;
    for (int it=1;; ++it) {
      // one EM iteration
      expectation();
      maximization();
      // Check for convergence.
      final double msr = measure();
      final double dif = Math.abs(msr - oldmsr);
      final double err = dif / (1.0 + oldmsr);
      oldmsr = msr;
      if (err < thresh) { break; }
    }
    // Compute cluster weights.
    computeWeights();

    // diagnostic messages
    printMog("JAVA-COMPUTED", n, weight, mean, var);
    BaseTestCase.println("msr = (" + oldmsr + ")");
  }

  /**
   * Compute an initial configuration of cluster centers uniformly spaced
   * over the range of the input samples, for subsequent iterative refinement.
   * @param n number of clusters to output
   * @param center initial uniform cluster centers to compute
   * @param ns number of input samples
   * @param sample array of input samples
   */
  public static void uniform(int n, double center[], int ns, double sample[])
  {
    double min_x = Double.MAX_VALUE, max_x = -Double.MAX_VALUE;
    for (int i=0; i<ns; ++i) {
      final double x = sample[i];
      if (min_x > x) { min_x = x; }
      if (max_x < x) { max_x = x; }
    }

    final double length = max_x - min_x;
    final double increment = length / n;
    center[0] = increment / 2;
    for (int i=1; i<n; ++i) { center[i] = center[i-1] + increment; }
  }

  /**
   * Compute an initial configuration of cluster centers uniformly distributed
   * over the range of the input samples, for subsequent iterative refinement.
   * @param n number of clusters to output
   * @param center initial uniform cluster centers to compute
   * @param ns number of input samples
   * @param sample array of input samples
   * @param rng random number generator
   */
  public static void random(int n, double center[], int ns, double sample[],
                            Random rng)
  {
    double min_x = Double.MAX_VALUE, max_x = -Double.MAX_VALUE;
    for (int i=0; i<ns; ++i) {
      final double x = sample[i];
      if (min_x > x) { min_x = x; }
      if (max_x < x) { max_x = x; }
    }

    final double length = max_x - min_x;
    for (int i=0; i<n; ++i) {
      final double r = rng.nextDouble();
      final double x = min_x + r * length;
      center[i] = x;
    }
  }

  /** Initialize cluster centers for EM iterations. */
  void setCenters(int n, double center[])
  {
    if (1 <= n && n <= max_n) {
      this.n = n;
      System.arraycopy(center, 0, mean, 0, n);
    }
    else {
      final String msg =
        "Number of Gaussian components (" + n + ") not in [1, " + max_n + "].";
      throw new IllegalArgumentException(msg);
    }
  }

  /** Specify the input samples to work with. */
  void setSamples(int ns, double sample[])
  {
    final int min_sample_size = n * min_sample_size_per_cluster;
    if (ns >= min_sample_size) {
      this.ns = ns;
      this.sample = sample;
    }
    else {
      final String msg =
        "Insufficient sample size (" + ns + " < " + min_sample_size + ").";
      throw new IllegalArgumentException(msg);
    }
  }

  /** Initialize the EM (expectation-maximization) iterations. */
  void initEM()
  {
    // Sort the input samples in ascending order.
    Arrays.sort(sample, 0, ns);
    // Sort the initial cluster centers in ascending order.
    Arrays.sort(mean, 0, n);
    // Initialize the cluster brackets.
    maximization();
  }

  /** (Re-)compute cluster centers while holding cluster brackets fixed. */
  void expectation()
  {
    // Remove empty clusters.
    for (int i=0, j=1;;) {
      // Examine the value at the current location.
      final int bi = bracket[i];
      // Locate the next larger value.
      for (; j<n; ++j) {
        final int bj = bracket[j];
        if (bi < bj) {
          // Move the larger value up to be adjacent to current value.
          bracket[i+1] = bj;
          // Advance loop variables.
          ++i;  ++j;  break;
        }
      }
      // Check for loop termination.
      if (j >= n) { n = i+1;  break; }
    }

    // Compute cluster parameters.
    for (int i=0; i<n; ++i) {
      final int ini = bracket[i];
      final int lim = bracket[i+1];
      final int nb = (lim - ini);
      // Computer cluster mean.
      double sum = 0.0;
      for (int j=ini; j<lim; ++j) {
        final double x = sample[j];
        sum += x;
      }
      final double m = (sum / nb);
      mean[i] = m;
      // Compute cluster variance.
      sum = 0.0;
      for (int j=ini; j<lim; ++j) {
        final double x = sample[j];
        final double d = x - m;
        sum += d * d;
      }
      final double v = ((nb > 1) ? (sum / (nb-1)) : 0.0);
      var[i] = v;
    }
  }

  /** (Re-)compute cluster brackets while holding cluster centers fixed. */
  void maximization()
  {
    bracket[0] = 0;
    for (int i=1; i<n; ++i) {
      final double mlo = mean[i-1];
      final double mhi = mean[i];
      // Compute the dividing point between clusters (i-1) and (i).
      int lo = bracket[i-1], hi = ns;
      while (lo < (hi-1)) {
        final int mid = (lo + hi) >> 1;
        final double sam = sample[mid];
        final double dlo = Math.abs(sam - mlo);
        final double dhi = Math.abs(mhi - sam);
        if (dlo < dhi) { lo = mid; } else { hi = mid; }
      }
      bracket[i] = hi;
    }
    bracket[n] = ns;
  }

  /** Compute a measure of total quantization error. */
  double measure()
  {
    double sum = 0.0;
    for (int i=0; i<n; ++i) {
      final int ini = bracket[i];
      final int lim = bracket[i+1];
      final int nb = lim - ini;
      final double v = var[i];
      sum += v * (nb-1);
    }
    sum /= ns;
    return sum;
  }

  /** Compute cluster weights. */
  void computeWeights()
  {
    for (int i=0; i<n; ++i) {
      final int ini = bracket[i];
      final int lim = bracket[i+1];
      final int siz = lim - ini;
      final double wt = ((ns > 0) ? ((double) siz / (double) ns) : 0.0);
      weight[i] = wt;
    }
  }

  /** Print out the clustering configuration. */
  void printMog(String label, int n, double weight[], double mean[], double var[])
  {
    BaseTestCase.println(label + ": n = " + n);
    for (int i=0; i<n; ++i) {
      BaseTestCase.println("(w, m, v) = (" +
              weight[i] + ", " + mean[i] + ", " + var[i] + ")");
    }
  }

  /** maximum number of Gaussian components */
  public final static int max_n = 6;

  /** actual number of Gaussian components */
  public int n = 0;
  /** weights associated with the Gaussian components */
  public final double weight[] = new double[max_n];
  /** mean parameters for the Gaussian components */
  public final double mean[] = new double[max_n];
  /** variance parameters for the Gaussian components */
  public final double var[] = new double[max_n];

  /** cluster brackets on the input samples */
  protected int bracket[] = new int[max_n+1];
  /** number of input samples */
  protected int ns = 0;
  /** array of input samples */
  protected double sample[] = null;

  /** minimum sample size per output cluster */
  public static final int min_sample_size_per_cluster = 32;
}
