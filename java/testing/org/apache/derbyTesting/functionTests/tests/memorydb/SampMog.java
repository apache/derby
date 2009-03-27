/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.SampMog

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

/**
 * Simple utility to generate samples from a mixture-of-Gaussian distribution.
 */
public class SampMog
{
  /** default constructor */
  public SampMog() { rng = new Random(System.currentTimeMillis()); }

  /** constructor with specified RNG */
  public SampMog(Random rng) { this.rng = rng; }

  /**
   * Generate samples from the specified mixture-of-Gaussian distribution.
   * @param ns number of samples to generate
   * @param sample output array of generated samples
   */
  public void generate(int ns, double sample[])
  {
    // First, generate standard normal samples, drawn from N(0, 1).
    for (int i=0; i<ns; ++i) {
      final double r = rng.nextGaussian();
      sample[i] = r;
    }
    // Then, transform the samples to conform to the Gaussian components
    // according to their weights.
    for (int i=0; i<ns; ++i) {
      // Pick a Gaussian component, represented by (idx).
      final double w = rng.nextDouble();
      int idx = Arrays.binarySearch(cumulative, w);
      if (idx < 0) { idx = -(idx + 1); }
      if (idx >= n) { idx = n-1; }
      final double m = mean[idx];
      final double s = stdv[idx];
      // Transform the sample to conform to the Gaussian component.
      final double r = sample[i];
      final double x = m + s * r;
      sample[i] = x;
    }
  }

  /** Get the maximum number of Gaussian components. */
  public static int getMaxNumber() { return max_n; }

  /** Get the number of Gaussian components. */
  public int getNumber() { return n; }

  /** Specify the mixture-of-Gaussian configuration. */
  public void set(int n, double wts[], double mm[], double vv[])
  {
    setNumber(n);
    setWeights(wts);
    setMeans(mm);
    setVars(vv);
  }

  /** Set the number of Gaussian components. */
  public void setNumber(int n)
  {
    if (1 <= n && n <= max_n) {
      this.n = n;
    }
    else {
      final String msg =
        "Number of Gaussian components (" + n + ") not in [1, " + max_n + "].";
      throw new IllegalArgumentException(msg);
    }
  }

  /** Specify the weights for the Gaussian components. */
  public void setWeights(double wts[])
  {
    // Copy weights to internal state array.
    System.arraycopy(wts, 0, weight, 0, n);

    // Normalize the weights to sum to 1.
    IllegalArgumentException ex = null;
    double sum = 0.0;
    for (int i=0; i<n; ++i) {
      final double wt = weight[i];
      if (wt > 0.0) {
        sum += wt;
      }
      else {
        if (ex == null) {
          final String msg = "Invalid weight (" + wt + ").";
          ex = new IllegalArgumentException(msg);
        }
      }
    }
    if (sum > 0.0) {
      for (int i=0; i<n; ++i) { weight[i] /= sum; }
    }
    else {
      if (ex == null) {
        final String msg = "Invalid total weight (" + sum + ").";
        ex = new IllegalArgumentException(msg);
      }
    }
    if (ex != null) { throw ex; }

    // Compute cumulative weights.
    cumulative[0] = weight[0];
    for (int i=1; i<n; ++i) { cumulative[i] = Math.min(cumulative[i-1] + weight[i], 1.0); }
    for (int i=n; i<max_n; ++i) { cumulative[i] = 1.0; }
  }

  /** Specify the mean parameters for the Gaussian components. */
  public void setMeans(double mm[])
  {
    System.arraycopy(mm, 0, mean, 0, n);
  }

  /** Specify the variance parameters for the Gaussian components. */
  public void setVars(double vv[])
  {
    for (int i=0; i<n; ++i) {
      final double v = Math.abs(vv[i]);
      var[i] = v;
      stdv[i] = Math.sqrt(v);
    }
  }

  /** random number generator in use */
  public Random rng;

  /** maximum number of Gaussian components */
  public final static int max_n = 6;

  /** actual number of Gaussian components */
  int n = 3;
  /** weights associated with the Gaussian components */
  final double weight[] = new double[max_n];
  /** mean parameters for the Gaussian components */
  final double mean[] = new double[max_n];
  /** variance parameters for the Gaussian components */
  final double var[] = new double[max_n];

  /** cumulative weights, for sample generation */
  final double cumulative[] = new double[max_n];
  /** standard deviation parameters for the Gaussian components */
  final double stdv[] = new double[max_n];
}
