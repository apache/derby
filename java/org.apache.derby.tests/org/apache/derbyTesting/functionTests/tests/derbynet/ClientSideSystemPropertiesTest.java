/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.derbynet.ClientSideSystemProperties
 
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

package org.apache.derbyTesting.functionTests.tests.derbynet;

import java.io.File;
import java.security.AccessController;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;

import junit.framework.Test;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.J2EEDataSource;
import org.apache.derbyTesting.junit.JDBCDataSource;
import org.apache.derbyTesting.junit.SystemPropertyTestSetup;
import org.apache.derbyTesting.junit.TestConfiguration;

/** The test of the jvm properties for enabling client side tracing(DERBY-1275)
  */
public class ClientSideSystemPropertiesTest extends BaseJDBCTestCase { 
	/** Establish a connection and that should start client side tracing
     *  because we have set the system properties to enable client side
     *  tracing. */
    public void testConnection() throws Exception {
        Connection conn = openDefaultConnection();
        conn.setAutoCommit(false);
        checkTraceFileIsPresent();
        conn.rollback();
        conn.close();
    }

    public void testClientDataSourceConnection() throws Exception {
       Connection conn = JDBCDataSource.getDataSource().getConnection();
       conn.setAutoCommit(false);
       checkTraceFileIsPresent();
       conn.rollback();
       conn.close();
    }
    
    public void testClientCPDataSourceConnection() throws Exception {
        PooledConnection pconn = J2EEDataSource.getConnectionPoolDataSource().
                getPooledConnection(); 
        Connection conn = pconn.getConnection();
        conn.setAutoCommit(false);
        checkTraceFileIsPresent();
        conn.rollback();
        conn.close();
        pconn.close();
     }

    public void testClientXADataSourceConnection() throws Exception {
        XAConnection xaconn = J2EEDataSource.getXADataSource().
                getXAConnection();
        Connection conn = xaconn.getConnection();
        conn.setAutoCommit(false);
        checkTraceFileIsPresent();
        conn.close();
        xaconn.close();
     }

    
    private void checkTraceFileIsPresent() {
        //Make sure the connection above created a trace file. This check is 
        //made in the privilege block below by looking inside the 
        //trace Directory and making sure the file count is greater than 0.
        AccessController.doPrivileged
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
		    (new java.security.PrivilegedAction<Void>(){
		    	public Void run(){
		    		File dir = new File(getSystemProperty("derby.client.traceDirectory"));
		    		int fileCounter = 0;
    	            File[] list = dir.listFiles();
    	            File tempFile;
    	            for (;fileCounter<list.length; fileCounter++)
    	            	tempFile = list[fileCounter];
    	            junit.framework.Assert.assertTrue(fileCounter>0);
    	            return null;
    		    }
    		}	 
    	    );
    }
    
    /** If the trace Directory doesn't exist then create one. If there is one
     *  already there, then delete everything under it. */
    protected void setUp() throws Exception
    {
    	AccessController.doPrivileged(
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
    			new java.security.PrivilegedAction<Void>(){
    				public Void run(){
    					File dir = new File(getSystemProperty("derby.client.traceDirectory"));
    					if (dir.exists() == false) //create the trace Directory
    						junit.framework.Assert.assertTrue(dir.mkdir() || dir.mkdirs());
    					else {//cleanup the trace Directory which already exists
    						int fileCounter = 0;
    						File[] list = dir.listFiles();
    						File tempFile;
    						for (;fileCounter<list.length; fileCounter++) {
    							tempFile = list[fileCounter];
    							assertTrue(tempFile.delete());
        					}
		        }
	            return null;
		    }
		}	 
	    );
    }
    
    /** Delete the trace Directory so that the test environment is clean for the
     *  next test run. */
    protected void tearDown() throws Exception
    {
        super.tearDown();
        
        removeDirectory(getSystemProperty("derby.client.traceDirectory"));
    }
    
    /* ------------------- end helper methods  -------------------------- */
    public ClientSideSystemPropertiesTest(String name) {
        super(name);
    }

    /*
     * Set the system properties related to client side tracing.
     */
    public static Test suite() {
        //Create the traceDirectory required by the tests in this class
    	Properties traceRelatedProperties = new Properties();
        traceRelatedProperties.setProperty("derby.client.traceLevel", "64");
        traceRelatedProperties.setProperty("derby.client.traceDirectory", "TraceDir");
        Test suite = TestConfiguration.clientServerSuite(ClientSideSystemPropertiesTest.class);
        return new SystemPropertyTestSetup(suite, traceRelatedProperties); 
    }
    
}
