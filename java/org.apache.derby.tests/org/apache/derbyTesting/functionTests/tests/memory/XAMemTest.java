/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.memory.XAMemTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.memory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestSuite;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
import org.apache.derbyTesting.junit.XATestUtil;

/**
 * Tests verifying that the memory usage for certain XA operations / access
 * patterns isn't unreasonably high.
 */
public class XAMemTest
        extends BaseJDBCTestCase {

    public XAMemTest(String name) {
        super(name);
    }

    public void setUp()
            throws SQLException {
        // Create the default wombat database if it doesn't exist.
        // Required to run some of the test cases individually.
        getConnection();
    }

    /**
     * DERBY-4137: Execute a bunch of successful XA transactions with a
     * transaction timeout specified.
     *
     * @throws Exception if something goes wrong
     */
    public void testDerby4137_TransactionTimeoutSpecifiedNotExceeded()
            throws Exception {
        XADataSource xads = J2EEDataSource.getXADataSource();
        XAConnection xac = xads.getXAConnection();
        XAResource xar = xac.getXAResource();
        Xid xid = XATestUtil.getXid(8, 9, 10);
        Connection con = xac.getConnection();
        Statement stmt = con.createStatement();
        
        // Set a long timeout such that the queue won't be able to clean
        // itself as part of normal processing.
        xar.setTransactionTimeout(100000);
        
        // 60'000 iterations was selected to balance duration versus chance of
        // detecting a "memory leak". The test failed before DERBY-4137 was
        // addressed.
        for (int i=0; i < 60000; i++) {
            xar.start(xid, XAResource.TMNOFLAGS);
            stmt.executeQuery("values 1");    
            xar.end(xid, XAResource.TMSUCCESS);
            xar.commit(xid, true);
        }
        xac.close();
    }

    public static Test suite() {
        if (JDBC.vmSupportsJDBC3()) {
            return TestConfiguration.defaultSuite(XAMemTest.class);
        }

        return new BaseTestSuite(
            "XAMemTest skipped - XADataSource not available");
    }
}
