/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.ClusMogSQL

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
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.derbyTesting.junit.BaseTestCase;

/**
 * Simple utility to compute/recover the parameters of a mixture-of-Gaussian
 * distribution from independent samples, using SQL.
 */
public class ClusMogSQL extends ClusMog
{
  /** constructor */
  public ClusMogSQL(Connection conn) { this.conn = conn; }

  /** Set unique ID for this object. */
  public void setUnique(int uniq)
  {
    this.uniq = (uniq & 0x7fffffff);
  }

  /**
   * Compute/recover the parameters of a mixture-of-Gaussian distribution
   * from given independent samples, using SQL.
   * @param n number of clusters (Gaussian components) to output
   * @param center initial cluster centers for iterative refinement
   * @param ns number of input samples
   * @param sample input samples; will be sorted in ascending order during use
   */
  public void clusterSQL(int n, double center[], int ns, double sample[])
  throws SQLException
  {
    // Record input parameters.
    setCenters(n, center);
    setSamples(ns, sample);
    // Initialize EM iterations.
    init();
    // Perform EM iterations until convergence.
    final double thresh = 1.0e-6;
    double oldmsr = Double.MAX_VALUE;
    for (int it=1;; ++it) {
      // one EM iteration
      final double msr = expect();
      maximize();
      // Check for convergence.
      final double dif = Math.abs(msr - oldmsr);
      final double err = dif / (1.0 + oldmsr);
      oldmsr = msr;
      if (err < thresh) { break; }
    }
    // Download the cluster configuration.
    download();
    // Clean up working tables after use.
    cleanup();

    // diagnostic messages
    printMog("SQL-COMPUTED", n, weight, mean, var);
    BaseTestCase.println("msr = (" + oldmsr + ")");
  }

  /** Initialize the EM (expectation-maximization) iterations. */
  void init() throws SQLException
  {
    // Sort the input samples in ascending order.
    Arrays.sort(sample, 0, ns);
    // Sort the initial cluster centers in ascending order.
    Arrays.sort(mean, 0, n);

    // working table names
    final String clusterN = "cluster" + uniq;
    final String sampleN = "sample" + uniq;

    // Initialize database tables.
    PreparedStatement pstmt = null;
    Statement stmt = conn.createStatement();
    try {
      stmt.executeUpdate("CREATE TABLE " + clusterN + "(weight double, mean double, var double, bucket int PRIMARY KEY)");
      stmt.executeUpdate("CREATE TABLE " + sampleN + "(value double, id int PRIMARY KEY, bucket int)");

      pstmt = conn.prepareStatement("INSERT INTO " + sampleN + "(value, id) VALUES (?, ?)");
      for (int i=0; i<ns; ++i) {
        final double x = sample[i];
        pstmt.setDouble(1, x);
        pstmt.setInt(2, i);
        pstmt.executeUpdate();
      }
      pstmt.close();
      pstmt = conn.prepareStatement("INSERT INTO " + clusterN + "(mean, bucket) VALUES (?, ?)");
      for (int i=0; i<n; ++i) {
        final double x = mean[i];
        pstmt.setDouble(1, x);
        pstmt.setInt(2, i);
        pstmt.executeUpdate();
      }
    }
    finally {
      if (stmt != null) { stmt.close();  stmt = null; }
      if (pstmt != null) { pstmt.close();  pstmt = null; }
    }

    // Initialize sample-to-cluster assignment.
    maximize();
  }

  /**
   * (Re-)compute cluster centers while holding sample-to-cluster assignment fixed.
   * @return mean square error of resulting clustering configuration
   * @throws SQLException
   */
  double expect() throws SQLException
  {
    // working table names
    final String clusterN = "cluster" + uniq;
    final String sampleN = "sample" + uniq;
    final String mm = "mm" + uniq;
    final String vv = "vv" + uniq;
    final String ee = "ee" + uniq;

    double msr = Double.MAX_VALUE;
    Statement stmt = null;
    ResultSet rset = null;
    try {
      stmt = conn.createStatement();

      stmt.executeUpdate("CREATE TABLE " + mm + "(bucket int PRIMARY KEY, mean double)");
      stmt.executeUpdate("CREATE TABLE " + vv + "(bucket int PRIMARY KEY, var double)");
      stmt.executeUpdate("CREATE TABLE " + ee + "(bucket int PRIMARY KEY, err double, size int)");

      stmt.executeUpdate("INSERT INTO " + mm + "(bucket, mean) \n" +
                         "SELECT bucket, avg(value) \n" +
                         "  FROM " + sampleN + " \n" +
                         " GROUP BY bucket \n");

      stmt.executeUpdate("INSERT INTO " + ee + "(bucket, err, size) \n" +
                         "SELECT S.bucket, sum((S.value - M.mean) * (S.value - M.mean)), count(*) \n" +
                         "  FROM " + sampleN + " S JOIN " + mm + " M ON S.bucket = M.bucket \n" +
                         " GROUP BY S.bucket \n");

      stmt.executeUpdate("INSERT INTO " + vv + "(bucket, var) \n" +
                         "SELECT bucket, \n" +
                         "       CASE WHEN (size > 1) THEN (err / (size - 1)) ELSE 0.0 END \n" +
                         "  FROM " + ee + " \n");

      stmt.executeUpdate("DELETE FROM " + clusterN);

      stmt.executeUpdate("INSERT INTO " + clusterN + "(mean, var, bucket) \n" +
                         "SELECT M.mean, V.var, V.bucket \n" +
                         "  FROM " + mm + " M JOIN " + vv + " V ON M.bucket = V.bucket \n");

      rset = stmt.executeQuery("SELECT (sum(err) / sum(size)) AS measure FROM " + ee);
      while (rset.next()) { msr = rset.getDouble(1); }

      stmt.executeUpdate("DROP TABLE " + mm);
      stmt.executeUpdate("DROP TABLE " + vv);
      stmt.executeUpdate("DROP TABLE " + ee);
    }
    finally {
      if (rset != null) { rset.close();  rset = null; }
      if (stmt != null) { stmt.close();  stmt = null; }
    }
    return msr;
  }

  /**
   * (Re-)compute sample-to-cluster assignment while holding cluster centers fixed.
   * @throws SQLException
   */
  void maximize() throws SQLException
  {
    // working table names
    final String clusterN = "cluster" + uniq;
    final String sampleN = "sample" + uniq;
    final String gg = "gg" + uniq;
    final String jj = "jj" + uniq;

    Statement stmt = null;
    try {
      stmt = conn.createStatement();

      stmt.executeUpdate("CREATE TABLE " + gg + "(id int PRIMARY KEY, diff double)");
      stmt.executeUpdate("CREATE TABLE " + jj + "(value double, id int, diff double, bucket int)");

      stmt.executeUpdate("INSERT INTO " + gg + "(id, diff) \n" +
                         "SELECT S.id, min(abs(S.value - C.mean)) \n" +
                         "  FROM " + sampleN + " S, " + clusterN + " C \n" +
                         " GROUP BY S.id \n");

      stmt.executeUpdate("INSERT INTO " + jj + "(value, id, diff, bucket) \n" +
                         "SELECT S.value, S.id, abs(S.value - C.mean), C.bucket \n" +
                         "  FROM " + sampleN + " S, " + clusterN + " C \n");

      stmt.executeUpdate("DELETE FROM " + sampleN);

      stmt.executeUpdate("INSERT INTO " + sampleN + "(value, id, bucket) \n" +
                         "SELECT J.value, J.id, min(J.bucket) \n" +
                         "  FROM " + jj + " J \n" +
                         "  JOIN " + gg + " G \n" +
                         "    ON J.id   = G.id \n" +
                         "   AND J.diff = G.diff \n" +
                         " GROUP BY J.id, J.value \n");

      stmt.executeUpdate("DROP TABLE " + gg);
      stmt.executeUpdate("DROP TABLE " + jj);
    }
    finally {
      if (stmt != null) { stmt.close(); }
    }
  }

  /**
   * Download the computed cluster configuration.
   * @throws SQLException
   */
  void download() throws SQLException
  {
    // working table names
    final String clusterN = "cluster" + uniq;
    final String sampleN = "sample" + uniq;
    final String ww = "ww" + uniq;
    final String cc = "cc" + uniq;

    Statement stmt = null;
    ResultSet rset = null;
    try {
      stmt = conn.createStatement();

      stmt.executeUpdate("CREATE TABLE " + ww + "(bucket int PRIMARY KEY, size int)");
      stmt.executeUpdate("CREATE TABLE " + cc + "(weight double, mean double, var double, bucket int PRIMARY KEY)");

      stmt.executeUpdate("INSERT INTO " + ww + "(bucket, size) \n" +
                         "SELECT bucket, count(*) \n" +
                         "  FROM " + sampleN + " \n" +
                         " GROUP BY bucket \n");

      stmt.executeUpdate("INSERT INTO " + cc + "(weight, mean, var, bucket) \n" +
                         "SELECT (CAST(W.size AS double) / (SELECT sum(size) FROM " + ww + ")), C.mean, C.var, C.bucket \n" +
                         "  FROM " + clusterN + " C JOIN " + ww + " W ON C.bucket = W.bucket \n");

      stmt.executeUpdate("DELETE FROM " + clusterN);

      stmt.executeUpdate("INSERT INTO " + clusterN + "(weight, mean, var, bucket) \n" +
                         "SELECT weight, mean, var, bucket FROM " + cc + " \n");

      stmt.executeUpdate("DROP TABLE " + ww);
      stmt.executeUpdate("DROP TABLE " + cc);

      rset = stmt.executeQuery("SELECT weight, mean, var FROM " + clusterN + " ORDER BY mean");
      n = 0;
      while (rset.next()) {
        final double w = rset.getDouble(1);
        final double m = rset.getDouble(2);
        final double v = rset.getDouble(3);
        weight[n] = w;
        mean[n] = m;
        var[n] = v;
        ++n;
      }
    }
    finally {
      if (rset != null) { rset.close();  rset = null; }
      if (stmt != null) { stmt.close();  stmt = null; }
    }
  }

  /**
   * Clean up working tables after use.
   * @throws SQLException
   */
  void cleanup() throws SQLException
  {
    // working table names
    final String clusterN = "cluster" + uniq;
    final String sampleN = "sample" + uniq;

    Statement stmt = null;
    try {
      stmt = conn.createStatement();
      stmt.executeUpdate("DROP TABLE " + sampleN);
      stmt.executeUpdate("DROP TABLE " + clusterN);
    }
    finally {
      if (stmt != null) { stmt.close();  stmt = null; }
    }
  }

  /** database connection */
  Connection conn;

  /** unique ID for generating working table names */
  int uniq = 0;
}
