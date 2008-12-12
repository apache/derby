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


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test to test two threads doing select then delete of a row
 * using REPEATABLE_READ isolation level.  We should get a 
 * deadlock (SQLState 40001);
 *
 */
public class Derby3980DeadlockTest extends BaseJDBCTestCase {
    private final int THREAD_COUNT = 2;
    private LinkedList  listExceptions = new LinkedList();
    private Object syncObject = new Object();
    private int startedCount = 0;
    
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
        for( Iterator i = listExceptions.iterator(); i.hasNext(); ) {
            SQLException e = (SQLException) i.next();
            assertSQLState("40001",e);
        }
        assertEquals("Expected 1 exception, got" + listExceptions.size(),
                1,listExceptions.size());
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
                    
                    //stmt.executeUpdate("update t set i = 456 where i = 456");
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

	Test suite = TestConfiguration.embeddedSuite(Derby3980DeadlockTest.class);
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

}

