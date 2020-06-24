/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.MogTest

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

import junit.framework.Test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test consistency among (GenMog), (SampMog), and (ClusMog).
 */
public class MogTest extends BaseJDBCTestCase {

  public MogTest(String name) { super(name); }
	
  public static Test suite() {
    return TestConfiguration.defaultSuite(MogTest.class);
  }

  /** Dispose of objects after testing. */
  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  /**
   * Calculates by using the default directory/disk storage back end.
   *
   * @throws SQLException if the test fails
   */
  public void testClusMogOnDisk()
          throws SQLException {
      long start = System.currentTimeMillis();
      doTestClusMog(getConnection());
      println("duration-on-disk: " + (System.currentTimeMillis() - start));
  }

  /**
   * Calculates by using the in-memory storage back end.
   *
   * @throws SQLException if the test fails
   */
  public void testClusMogInMemory()
          throws SQLException {
      long start = System.currentTimeMillis();
      // Close the connection explicitly here, since the test framework won't
      // do it for us when we create it manually.
      Connection conn = obtainConnection();
      try {
          doTestClusMog(conn);
      } finally {
          try {
              conn.rollback();
              conn.close();
          } catch (SQLException sqle) {
              // Ignore exceptions during close.
          }
//IC see: https://issues.apache.org/jira/browse/DERBY-4428
          dropInMemoryDb();
      }
      println("duration-in-memory: " + (System.currentTimeMillis() - start));
  }

  /**
   * Test consistency between (ClusMog) and (ClusMogSQL).
   * @throws SQLException if something goes wrong
   */
  public void doTestClusMog(Connection conn)
          throws SQLException {

    // Initialize test objects.
    // NOTE: Due to instability in the test, the seed has been fixed.
    //       See DERBY-4209 (and DERBY-4085).
    //final long _seed = System.currentTimeMillis();
//IC see: https://issues.apache.org/jira/browse/DERBY-4085
    final long _seed = 1241411544935L;
    java.util.Random rng = new java.util.Random(_seed);
    /** MOG generator being tested */
    GenMog genMog = new GenMog(rng);
    /** MOG sampler being tested */
    SampMog sampMog = new SampMog(rng);
    /** clustering object being tested */
    ClusMog clusMog = new ClusMog();
    /** clustering object being tested */
    ClusMogSQL clusMogSql = new ClusMogSQL(conn);
    clusMogSql.setUnique(rng.nextInt());

    println(getName() + " using random seed: " + _seed);
    final int max_ns = 10 * ClusMog.min_sample_size_per_cluster * GenMog.max_n;
    final double sample[] = new double[max_ns];
    final double center[] = new double[GenMog.max_n];
    final int niter = 1;
    for (int i=niter; i>0; --i) {
      // Generate a MOG configuration.
      genMog.generate();
      // Compute a sample size.
      final int min_ns = ClusMog.min_sample_size_per_cluster * genMog.n;
      final int ns = min_ns + rng.nextInt(max_ns - min_ns);
      println("ns = " + ns);
      // Generate samples from the MOG distribution.
      sampMog.set(genMog.n, genMog.weight, genMog.mean, genMog.var);
      sampMog.generate(ns, sample);

      // Produce an initial cluster center configuration.
      ClusMog.uniform(genMog.n, center, ns, sample);
      // Cluster the samples to recover the MOG configuration.
      clusMog.cluster(genMog.n, center, ns, sample);
      // Cluster the samples again, using SQL.
      clusMogSql.clusterSQL(genMog.n, center, ns, sample);
      // Compare the computed MOG configurations.
//IC see: https://issues.apache.org/jira/browse/DERBY-4085
      assertEquals("MOG configurations differ, seed=" + _seed,
              clusMog.n, clusMogSql.n);
      compare(clusMog.n, clusMog.weight, clusMogSql.weight, _seed);
      compare(clusMog.n, clusMog.mean, clusMogSql.mean, _seed);
      compare(clusMog.n, clusMog.var, clusMogSql.var, _seed);

      // Produce another initial cluster center configuration.
      ClusMog.random(genMog.n, center, ns, sample, rng);
      // Cluster the samples to recover the MOG configuration.
      clusMog.cluster(genMog.n, center, ns, sample);
      // Cluster the samples again, using SQL.
      clusMogSql.clusterSQL(genMog.n, center, ns, sample);
      // Compare the computed MOG configurations.
//IC see: https://issues.apache.org/jira/browse/DERBY-4085
      assertEquals("MOG configurations differ, seed=" + _seed,
              clusMog.n, clusMogSql.n);
      compare(clusMog.n, clusMog.weight, clusMogSql.weight, _seed);
      compare(clusMog.n, clusMog.mean, clusMogSql.mean, _seed);
      compare(clusMog.n, clusMog.var, clusMogSql.var, _seed);
    }
  }

  /** Compare two floating-point arrays, with tolerance. */
  private void compare(int n, double ones[], double oths[], final long seed)
  {
    final double thresh = 1.0e-6;
    for (int i=0; i<n; ++i) {
      final double one = ones[i];
      final double oth = oths[i];
      final double dif = Math.abs(one - oth);
      final double err = dif / (1.0 + Math.abs(one));
      // Use if to avoid unnecessary string concatenation.
      if (err >= thresh) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4085
        fail("Error too big;" + err + " >= " + thresh + ", seed=" + seed);
      }
    }
  }

  /**
   * Obtains a connection to an in-memory database.
   *
   * @return A connection to an in-memory database.
   * @throws SQLException if obtaining the connection fails
   */
  private Connection obtainConnection()
        throws SQLException {
    try {
        if (usingDerbyNetClient()) {
            Class.forName("org.apache.derby.jdbc.ClientDriver");
        } else {
            Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        }
    } catch (Exception e) {
        SQLException sqle =  new SQLException(e.getMessage());
        sqle.initCause(e);
        throw sqle;
    }
//IC see: https://issues.apache.org/jira/browse/DERBY-4436
//IC see: https://issues.apache.org/jira/browse/DERBY-4428
    StringBuffer sb = constructUrl().append(";create=true");
    return DriverManager.getConnection(sb.toString());
  }

  /**
   * Drops the database used by the test.
   *
   * @throws SQLException if dropping the database fails
   */
    private void dropInMemoryDb()
            throws SQLException {
        StringBuffer sb = constructUrl().append(";drop=true");
        try {
            DriverManager.getConnection(sb.toString());
            fail("Dropping database should have raised exception.");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }
    }

    /**
     * Constructs the default URL for the in-memory test database.
     *
     * @return A database URL (without any connection attributes).
     */
    private StringBuffer constructUrl() {
        StringBuffer sb = new StringBuffer("jdbc:derby:");
        if (usingEmbedded()) {
            sb.append("memory:");
        } else {
            // This is a hack. Change this when proper support for the in-memory
            // back end has been implemented.
            sb.append("//");
            sb.append(TestConfiguration.getCurrent().getHostName());
            sb.append(':');
            sb.append(TestConfiguration.getCurrent().getPort());
            sb.append('/');
            sb.append("memory:");
        }
        sb.append("MogTestDb");
        return sb;
    }
}
