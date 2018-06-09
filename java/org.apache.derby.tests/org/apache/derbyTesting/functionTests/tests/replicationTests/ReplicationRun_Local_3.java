/*
 
Derby - Class org.apache.derbyTesting.functionTests.tests.replicationTests.ReplicationRun_Local_3
 
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
package org.apache.derbyTesting.functionTests.tests.replicationTests;

import java.sql.Connection;
import java.sql.SQLException;
import org.apache.derby.client.ClientDataSourceInterface;
import org.apache.derbyTesting.junit.JDBC;


/**
 * Defining startSlave, stopmaster, stopSlave and
 * failOver methods returning SQLException for
 * negative testing (ReplicationRun_Local_3 set of tests).
 * 
 */

public class ReplicationRun_Local_3 extends ReplicationRun
{
    
    /**
     * Creates a new instance of ReplicationRun_Local
     * @param testcaseName Identifying the test.
     */
    public ReplicationRun_Local_3(String testcaseName)
    {
        super(testcaseName);
    }
    
    //////////////////////////////////////////////////////////////
    ////
    //// The replication test framework (testReplication()):
    //// a) "clean" replication run starting master and slave servers,
    ////     preparing master and slave databases,
    ////     starting and stopping replication and doing
    ////     failover for a "normal"/"failure free" replication
    ////     test run.
    ////
    //////////////////////////////////////////////////////////////
    

    SQLException _failOver(String serverHost, int serverPort, String dbPath) 
            throws Exception
    {
        util.DEBUG("BEGIN _failOver"); 
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds =  (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName(dbPath);
        ds.setServerName(serverHost);
        ds.setPortNumber(serverPort);
        ds.setConnectionAttributes("failover=true"
                +useEncryption(false));
        try {
            Connection conn = ds.getConnection(); // 
            conn.close();
            util.DEBUG("END   _failOver. Got Connection");
            return null; // If successfull - could only happen on slave after a master stopMaster.
        } catch (SQLException se) {
            util.DEBUG("END   _failOver. " + se.getSQLState());
            return se;
        }
        
    }

    SQLException _startSlaveTrueAndCreateTrue(String serverHost, 
            int serverPort,
            String dbPath) 
        throws Exception
    {
        util.DEBUG("_startSlaveTrueAndCreateTrue");
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName(dbPath);
        ds.setServerName(serverHost);
        ds.setPortNumber(serverPort);
        ds.setConnectionAttributes("startSlave=true;create=true"
                +useEncryption(true));
        try {
            Connection conn = ds.getConnection(); // XRE10 - REPLICATION_CONFLICTING_ATTRIBUTES
            conn.close();
            return null; // Should never get here.
        } catch (SQLException se) {
            return se;
        }
    }

    SQLException _stopMaster(String masterServerHost, int masterServerPort, String dbPath) 
            throws Exception
    {
        util.DEBUG("_stopMaster");
        ClientDataSourceInterface ds;

        Class<?> clazz;
        if (JDBC.vmSupportsJNDI()) {
            clazz = Class.forName("org.apache.derby.jdbc.ClientDataSource");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        } else {
            clazz = Class.forName("org.apache.derby.jdbc.BasicClientDataSource40");
            ds = (ClientDataSourceInterface) clazz.getConstructor().newInstance();
        }

        ds.setDatabaseName(dbPath);
        ds.setServerName(masterServerHost);
        ds.setPortNumber(masterServerPort);
        ds.setConnectionAttributes("stopMaster=true"
                +useEncryption(false));
        try {
            Connection conn = ds.getConnection(); // 
            conn.close();
            return null; // If successfull.
        } catch (SQLException se) {
            util.DEBUG(se.getErrorCode()+" "+se.getSQLState()+" "+se.getMessage());
            return se;
        }       
    }
}
