/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.UniqueConstraintMultiThreadedTest

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

package org.apache.derbyTesting.functionTests.tests.lang;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.TestConfiguration;

public class UniqueConstraintMultiThreadedTest extends BaseJDBCTestCase {
    
    public UniqueConstraintMultiThreadedTest() {
        super ("Multi Threaded Unique Constraint Test");
    }
    
    /**
     * Deletes a record in a transaction and tries to insert the same 
     * from a different transaction. Once second transaction goes on wait
     * first transaction is committed or rolled back based on third
     * param (boolean commit).
     * 
     * @param isolation1 isolation level for 1st thread
     * @param isolation2 isolation level for 2nd thread
     * @param commit whether or not to commit
     */
    private void executeThreads (int isolation1, int isolation2, 
            boolean commit) throws Exception {
        Connection con1 = openDefaultConnection();
        con1.setTransactionIsolation(isolation1);
        Connection con2 = openDefaultConnection();
        try {
            con2.setTransactionIsolation(isolation2);
            DBOperations dbo1 = new DBOperations (con1, 5);
            DBOperations dbo2 = new DBOperations (con2, 5);
            dbo1.delete();
            Thread t = new Thread (dbo2);
            t.start();
            //wait for 2 sec should be enough for dbo2 so on wait
            t.sleep(2000);
            if (commit) {
                dbo1.rollback();
                t.join();
                assertSQLState("isolation levels: " + isolation1
                        + " " + isolation2, "23505", dbo2.getException());
            }
            else {
                dbo1.commit();
                t.join();
                assertNull("isolation levels: " + isolation1
                        + " " + isolation2, dbo2.getException());
            }
            assertNull("unexpected failure: " + isolation1
                        + " " + isolation2, dbo2.getUnexpectedException());
        }
        finally {
            con1.commit();
            con2.commit();
            con1.close();
            con2.close();
        }
        
    }
    
    /**
     * Test inserting a duplicate record while original is deleted in
     * a transaction and later committed.
     */
    public void testLockingWithcommit () throws Exception {
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                executeThreads((int) Math.pow(2,i),
                        (int) Math.pow (2,j), true);
            }
        }        
    }
    
    /**
     * Test inserting a duplicate record while original is deleted in
     * a transaction and later rolled back.
     */
    public void testLockingWithRollback () throws Exception {
        for (int i = 0; i < 4; i++) {
            for (int j = 0; j < 4; j++) {
                executeThreads((int) Math.pow (2,i),
                        (int) Math.pow (2,j), false);
            }
        }
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("UniqueConstraintTest");
        suite.addTest(TestConfiguration.embeddedSuite(UniqueConstraintMultiThreadedTest.class));
        return suite;
    }
    
    protected void setUp() throws Exception {
        Statement stmt = createStatement();
        stmt.execute("create table tab1 (i integer)");
        stmt.executeUpdate("alter table tab1 add constraint con1 unique (i)");
        PreparedStatement ps = prepareStatement("insert into tab1 " +
                "values (?)");
        for (int i = 0; i < 10; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();
        }
        commit();
    }

    protected void tearDown() throws java.lang.Exception {
        dropTable("tab1");
        super.tearDown();
    }
}
