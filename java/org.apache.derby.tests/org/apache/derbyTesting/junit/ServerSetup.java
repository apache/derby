/*
 *
 * Derby - Class org.apache.derbyTesting.junit.ServerSetup
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
package org.apache.derbyTesting.junit;

import junit.framework.Test;

/**
 * Change to a client server configuration based upon the
 * current configuration at setup time. Previous configuration
 * is restored at tearDown time. This only changes the
 * configuration, it does not start any network server.
 *
 */
public final class ServerSetup extends ChangeConfigurationSetup {

    private final String host;
    private final int port;
    private JDBCClient client;
    private String dbPath;
    
    public ServerSetup(Test test, String host, int port) {
        super(test);
        this.host = host;
        this.port = port;
    }

    public ServerSetup(Test test, String host, int port, String dbPath) {
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
        super(test);
        this.host = host;
        this.port = port;
        this.dbPath = dbPath;
    }

    TestConfiguration getNewConfiguration(TestConfiguration old) {
               
        return new TestConfiguration(old,
            (client == null) ? JDBCClient.DERBYNETCLIENT : client, host, port);
    }

    TestConfiguration getNewConfiguration(TestConfiguration old, String dbPath) {
               
        return new TestConfiguration(old,
            (client == null) ? JDBCClient.DERBYNETCLIENT : client, host, port, 
            dbPath);
    }

    /**
     * Specify a JDBCClient to use in place of the default DERBYNETCLIENT.
     */
    void setJDBCClient(JDBCClient newClient)
    {
        this.client = newClient;
    }
    /**
     * Specify a dbPath to use in place of the default.
     */
//IC see: https://issues.apache.org/jira/browse/DERBY-4417
    void setDbPath(String dbPath)
    {
        this.dbPath = dbPath;
    }
}
