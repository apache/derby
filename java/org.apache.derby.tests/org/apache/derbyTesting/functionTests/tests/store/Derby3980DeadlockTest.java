/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.Derby3980DeadlockTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.store;


import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.Properties;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.SecurityManagerSetup;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test to test two threads doing select then delete of a row
 * using REPEATABLE_READ isolation level.  We should get a 
 * deadlock (SQLState 40001);
 *
 */
public class Derby3980DeadlockTest extends BaseJDBCTestCase {
    private final int THREAD_COUNT = 2;
    private final LinkedList<Throwable> listExceptions =
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
            new LinkedList<Throwable>();
    private final Object syncObject = new Object();
    private int startedCount = 0;
    private static final String fprefix = "javacore";
    private static final String POLICY_FILE_NAME =
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
        "org/apache/derbyTesting/functionTests/tests/store/Derby3980DeadlockTest.policy";
    
    public Derby3980DeadlockTest(String name) {
        super(name);
    }

    
    public void test3980Deadlock() {
        Thread [] t = new Thread[THREAD_COUNT];
        createThreads(t);
        waitForThreads(t);
        checkExceptions();        
    }
    
    
    /**
     * Check we have one deadlock exception.
     */
    private void checkExceptions() {        
        //Due to timing, you might see ERROR 40XL1: A lock could not be obtained
        //instead of ERROR 40001 (DERBY-3980)
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        for (Throwable t : listExceptions) {
            if (t instanceof SQLException) {
                assertSQLState("40001", (SQLException) t);
            } else {
                fail("Unexpected exception", t);
            }
        }
        assertEquals("Expected 1 exception, got" + listExceptions.size(),
                1,listExceptions.size());
    }
    /**
     * Decorate a test with SecurityManagerSetup, clientServersuite, and
     * SupportFilesSetup.
     * 
     * @return the decorated test
     */
    private static Test decorateTest() {
        Test test = TestConfiguration.clientServerSuite(Derby3980DeadlockTest.class);
        Properties diagProperties = new Properties();
        diagProperties.setProperty("derby.stream.error.extendedDiagSeverityLevel", "30000");
        diagProperties.setProperty("derby.infolog.append", "true");
        test = new SystemPropertyTestSetup(test, diagProperties, true);
     
        // Install a security manager using the initial policy file.
//IC see: https://issues.apache.org/jira/browse/DERBY-6162
        return new SecurityManagerSetup(test, POLICY_FILE_NAME);
    }
    
    private void waitForThreads(Thread[] t) {
        for (int i = 0; i < THREAD_COUNT; i++)
        {   
            try {
                t[i].join();
            } catch (InterruptedException e){
               fail(
                        "FAIL - InterruptedException  thrown waiting for the threads");
            }
        }
        
    }

    private void createThreads(Thread[] t) {
        for (int i = 0; i < THREAD_COUNT; i++)
        {   
            t[i] = new Thread(new Runnable() {
                public void run() {threadWorker(); }

                private void threadWorker() {
                    Connection threadConnection = null;
                    
                    try {
                        synchronized (syncObject) {
                            
                            /* If a connection hasn't been opened for this thread, open one */
                            if (threadConnection == null){
                                threadConnection = openDefaultConnection();
                            }
                            
                            /* A new thread started, so we increment the counter */
                            startedCount++;
                            
                            /* Wake all the threads to run the check below */
                            syncObject.notifyAll();
                            
                            while (startedCount < THREAD_COUNT) {
                                syncObject.wait();
                            }
                        }          
                        Statement stmt = threadConnection.createStatement();
                    
                    threadConnection.setAutoCommit(false);
                    /* set isolation level to repeatable read */
                    threadConnection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                    
                    ResultSet rs = stmt.executeQuery("select * from t where i = 456");
                    while (rs.next());
                    
                    //stmt.executeUpdate("update t set i = 456 where i = 456")
                    stmt.executeUpdate("delete from t  where i = 456");
                    threadConnection.commit();
                    } catch (Exception e) {
                        synchronized(syncObject){
                            listExceptions.add(e);
                        }
                    }

                    
                }},"Thread"+i);
            t[i].start();
        }
        
    }

    public static Test suite() {
    Test suite = decorateTest();
 
    return new  CleanDatabaseTestSetup(
                DatabasePropertyTestSetup.setLockTimeouts(suite, 5, 10)) {
         /**
          * Creates the table used in the test cases.
          * 
          */
         protected void decorateSQL(Statement s) throws SQLException {
            s.executeUpdate("CREATE TABLE T (I INT)");
            s.executeUpdate("INSERT INTO T VALUES(456)");
         }
      
     };
	
    }

    /**
     * Use tearDown to cleanup some of the diagnostic information files
     */
    protected void tearDown() throws Exception {
        String dsh = BaseTestCase.getSystemProperty("user.dir");
        try {
          File basedir = new File(dsh);
          String[] list = BaseTestCase.getFilesWith(basedir, fprefix);
          removeFiles(list);
        } catch (IllegalArgumentException e) {
            fail("open directory");
        }   
        
        super.tearDown();
    }
}

