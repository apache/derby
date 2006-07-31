/*
 *
 * Derby - Class JDBCClient
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

/**
 * Type-safe enumerator of valid JDBC clients.
 * Each JDBC client definition consists of the client name, the name of the
 * JDBC driver class, the name of a DataSource class and the base JDBC url.
 */
public final class JDBCClient {

    /**
     * The embedded JDBC client.
     */
    static final JDBCClient EMBEDDED = new JDBCClient(
            "Embedded", 
            "org.apache.derby.jdbc.EmbeddedDriver", 
            "org.apache.derby.jdbc.EmbeddedDataSource", 
            "jdbc:derby:");
    
    /**
     * The Derby network client.
     */
    static final JDBCClient DERBYNETCLIENT= new JDBCClient(
            "DerbyNetClient",
            "org.apache.derby.jdbc.ClientDriver",
            "org.apache.derby.jdbc.ClientDataSource",
            "jdbc:derby://");
    
    /**
     * The DB2 Universal JDBC network client.
     * AKA: JCC or DerbyNet.
     * (the "old net" client for Derby).
     */
    static final JDBCClient DB2CLIENT= new JDBCClient(
            "DerbyNet",
            "com.ibm.db2.jcc.DB2Driver",
            null,
            "jdbc:derby:net://");
    
    /**
     * Is this the embdded client.
    */
    public boolean isEmbedded()
    {
    	return getName().equals(EMBEDDED.getName());
    }
    /**
     * Is this Derby's network client.
     * @return
     */
    public boolean isDerbyNetClient()
    {
    	return getName().equals(DERBYNETCLIENT.getName());
    }
    /**
     * Is this DB2's Universal JDBC 
     * @return
     */
    public boolean isDB2Client()
    {
    	return getName().equals(DB2CLIENT.getName());
    }
    
    /**
     * Get the name of the client
     */
    public String getName()
    {
    	return frameWork;
    }
    
    /**
     * Get JDBC driver class name.
     * 
     * @return class name for JDBC driver.
     */
    public String getJDBCDriverName() {
        return driverClassName;
    }

    /**
     * Get DataSource class name.
     * 
     * @return class name for DataSource implementation.
     */
    public String getDataSourceClassName() {
        return dsClassName;
    }

    /**
     * Return the base JDBC url.
     * The JDBC base url specifies the protocol and possibly the subprotcol
     * in the JDBC connection string.
     * 
     * @return JDBC base url.
     */
    public String getUrlBase() {
        return urlBase;
    }
    
    /**
     * Return string representation of this object.
     * 
     * @return string representation of this object.
     */
    public String toString() {
        return frameWork;
    }
    
    /**
     * Create a JDBC client definition.
     */
    private JDBCClient(String frameWork, String driverClassName,
                       String dataSourceClassName, String urlBase) {
        this.frameWork          = frameWork;
        this.driverClassName    = driverClassName;
        this.dsClassName        = dataSourceClassName;
        this.urlBase            = urlBase;
    }
    
    private final String frameWork;
    private final String driverClassName;
    private final String dsClassName;
    private final String urlBase;
    
}
