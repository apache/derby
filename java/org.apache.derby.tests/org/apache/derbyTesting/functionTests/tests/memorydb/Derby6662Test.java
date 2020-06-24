/*
   Derby - Class org.apache.derbyTesting.functionTests.tests.memorydb.Derby6662Test

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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class Derby6662Test extends BaseJDBCTestCase {
    
    public Derby6662Test(String name) { super(name); }
    
    public static Test suite() {
      return TestConfiguration.defaultSuite(Derby6662Test.class);
    }
    
    /** Dispose of objects after testing. */
    protected void tearDown() throws Exception
    {
      super.tearDown();
//IC see: https://issues.apache.org/jira/browse/DERBY-6662
      dropInMemoryDb();
    }
    
    public void testDatabaseMetaDataCalls() throws SQLException
    {
        DatabaseMetaData dmd = getDMD();
        assertFalse(dmd.usesLocalFilePerTable());
        assertFalse(dmd.usesLocalFiles());
    }
    
    public void testOptionalToolMetaData() throws SQLException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6662
        Connection conn = obtainConnection();
        // register the database metadata wrapper
        goodStatement( conn, "call syscs_util.syscs_register_tool" +
            "('databaseMetaData', true)");
        // run the routines
        assertResults(conn,"values usesLocalFiles()",
             new String[][]{ { "false" }},false);
        assertResults(conn,"values usesLocalFilePerTable()",
             new String[][]{ { "false" }},false);
        // unregister the database metadata wrapper
        goodStatement( conn, "call syscs_util.syscs_register_tool" +
            "('databaseMetaData', false)");
    }
    
    private DatabaseMetaData getDMD() throws SQLException
    {
        return obtainConnection().getMetaData();
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
      StringBuffer sb = constructUrl().append(";create=true");
      return DriverManager.getConnection(sb.toString());
    }
    
    /**
     * Drops the database used by the test.
     *
     * @throws SQLException if dropping the database fails
     */
    private void dropInMemoryDb() throws SQLException {
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6662
        sb.append("DBMDTestDb");
        return sb;
    }
}
