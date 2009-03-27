/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.GenMog

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

import java.util.Random;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Simple utility to generate a mixture-of-Gaussian configuration.
 */
public class GenMog
{
  /** default constructor */
  public GenMog() { rng = new Random(System.currentTimeMillis()); }

  /** constructor with specified RNG */
  public GenMog(Random rng) { this.rng = rng; }

  /** Generate a mixture-of-Gaussian configuration. */
  public void generate()
  {
    // number of Gaussian components
    n = 1 + rng.nextInt(max_n);

    // weights associated with the Gaussian components
    double sum = 0.0;
    for (int i=0; i<n; ++i) {
      double w;
      do { w = rng.nextDouble(); } while (w <= 0.0);
      weight[i] = w;
      sum += w;
    }
    for (int i=0; i<n; ++i) { weight[i] /= sum; }

    // (mean, var) parameters for the Gaussian components
    double oldm = 0.0, olds = 0.0;
    for (int i=0; i<n; ++i) {
      final double s = min_s + rng.nextDouble() * (max_s - min_s);
      final double m = oldm + 2.0 * olds + 2.0 * s;
      mean[i] = m;
      var[i] = s * s;
      oldm = m;
      olds = s;
    }

    // diagnostic messages
    BaseTestCase.println("GENERATED: n = " + n);
    for (int i=0; i<n; ++i) {
      BaseTestCase.println("(w, m, v) = (" +
              weight[i] + ", " + mean[i] + ", " + var[i] + ")");
    }
  }

  /** random number generator in use */
  public Random rng;

  /** maximum number of Gaussian components */
  public final static int max_n = 6;
  /** minimum value for the standard deviation parameter */
  public final static double min_s = 1.0;
  /** maximum value for the standard deviation parameter */
  public final static double max_s = 6.0;

  /** actual number of Gaussian components */
  public int n;
  /** weights associated with the Gaussian components */
  public final double weight[] = new double[max_n];
  /** mean parameters for the Gaussian components */
  public final double mean[] = new double[max_n];
  /** variance parameters for the Gaussian components */
  public final double var[] = new double[max_n];
}
