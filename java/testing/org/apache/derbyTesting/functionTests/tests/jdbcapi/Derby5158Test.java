/*
  Class org.apache.derbyTesting.functionTests.tests.jdbcapi.Derby5158Test

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to you under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

*/
package org.apache.derbyTesting.functionTests.tests.jdbcapi;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.TestConfiguration;


public class Derby5158Test extends BaseJDBCTestCase
{

    public Derby5158Test(String name)
    {
        super(name);
    }

    protected static Test makeSuite(String name)
    {
        BaseTestSuite suite = new BaseTestSuite(name);

        Test cst = TestConfiguration.defaultSuite(Derby5158Test.class);

        suite.addTest(cst);

        return suite;
    }

    public static Test suite()
    {
        String testName = "Derby5158Repro";

        return makeSuite(testName);
    }

    protected void setUp()
            throws java.lang.Exception {
        super.setUp();
        setAutoCommit(false);
    }


    /**
     * DERBY-5158
     */
    public void testCommitRollbackAfterShutdown() throws SQLException {

        Statement s = createStatement();
        ResultSet rs = s.executeQuery("select 1 from sys.systables");
        rs.close();
        s.close(); // just so we have a transaction, otherwise the commit is
                   // short-circuited in the client.

        TestConfiguration.getCurrent().shutdownDatabase();

        try {
            commit();
        } catch (SQLException e) {
            if (usingEmbedded()) {
                assertSQLState("08003", e);
            } else {
                // Before DERBY-5158, we saw "58009" instead with c/s.
                assertSQLState("08006", e);
            }
        }


        // bring db back up and start a transaction
        s = createStatement();
        rs = s.executeQuery("select 1 from sys.systables");
        rs.close();
        s.close(); 

        TestConfiguration.getCurrent().shutdownDatabase();

        try {
            rollback();
        } catch (SQLException e) {
            if (usingEmbedded()) {
                assertSQLState("08003", e);
            } else {
                // Before DERBY-5158, we saw "58009" instead with c/s.
                assertSQLState("08006", e);
            }
        }
    }
}
