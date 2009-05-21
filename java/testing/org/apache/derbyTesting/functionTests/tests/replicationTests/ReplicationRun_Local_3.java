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
import org.apache.derby.jdbc.ClientDataSource;


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
    {
        util.DEBUG("BEGIN _failOver"); 
        ClientDataSource ds = new org.apache.derby.jdbc.ClientDataSource();
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
        throws SQLException 
    {
        util.DEBUG("_startSlaveTrueAndCreateTrue");
        ClientDataSource ds = new org.apache.derby.jdbc.ClientDataSource();
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
    {
        util.DEBUG("_stopMaster");
        ClientDataSource ds = new org.apache.derby.jdbc.ClientDataSource();
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

    SQLException _stopSlave(String slaveServerHost, int slaveServerPort,
            String dbPath) 
        throws SQLException
    {
        util.DEBUG("_stopSlave");
        ClientDataSource ds = new org.apache.derby.jdbc.ClientDataSource();
        ds.setDatabaseName(dbPath);
        ds.setServerName(slaveServerHost);
        ds.setPortNumber(slaveServerPort);
        ds.setConnectionAttributes("stopSlave=true"
                +useEncryption(false));
        try {
            Connection conn = ds.getConnection();
            conn.close();
            return null; // If successfull.
        } catch (SQLException se) {
            return se;
        }       
    }
    
}
