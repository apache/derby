/*
 *
 * Derby - Class JDBCClient
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

import junit.framework.Assert;

/**
 * Type-safe enumerator of valid JDBC clients.
 * Each JDBC client definition consists of the client name, the name of the
 * JDBC driver class, the name of a DataSource class and the base JDBC url.
 */
public final class JDBCClient {

    /**
     * The embedded JDBC client.
     */
    public static final JDBCClient EMBEDDED_30= new JDBCClient(
            "Embedded_30", 
            "org.apache.derby.jdbc.EmbeddedDriver", 
            "org.apache.derby.jdbc.EmbeddedDataSource", 
            "org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource",
            "org.apache.derby.jdbc.EmbeddedXADataSource",
            "jdbc:derby:");
    
    /**
     * The embedded JDBC client for JDBC 4.0.
     */
    static final JDBCClient EMBEDDED_40 = new JDBCClient(
            "Embedded_40", 
            "org.apache.derby.jdbc.EmbeddedDriver", 

            JDBC.vmSupportsJNDI() ?
            "org.apache.derby.jdbc.EmbeddedDataSource40":
            "org.apache.derby.jdbc.BasicEmbeddedDataSource40",

            JDBC.vmSupportsJNDI() ?
            "org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource40":
            "org.apache.derby.jdbc.BasicEmbeddedConnectionPoolDataSource40",

            JDBC.vmSupportsJNDI() ?
            "org.apache.derby.jdbc.EmbeddedXADataSource40":
            "org.apache.derby.jdbc.BasicEmbeddedXADataSource40",

            "jdbc:derby:");
    
    /**
     * Return the default embedded client for this JVM.
     */
    static JDBCClient getDefaultEmbedded()
    {
        if (JDBC.vmSupportsJDBC4())
            return EMBEDDED_40;
        if (JDBC.vmSupportsJDBC3())
            return EMBEDDED_30;
        
        Assert.fail("Unknown JVM environment");
        return null;
    }
    
    /**
     * The Derby network client.
     */
    public static final JDBCClient DERBYNETCLIENT= new JDBCClient(
            "DerbyNetClient",
            "org.apache.derby.jdbc.ClientDriver",

            JDBC.vmSupportsJDBC4() ?
            (JDBC.vmSupportsJNDI() ?
            "org.apache.derby.jdbc.ClientDataSource40" :
            "org.apache.derby.jdbc.BasicClientDataSource40") :
             "org.apache.derby.jdbc.ClientDataSource",

            JDBC.vmSupportsJDBC4() ?
            (JDBC.vmSupportsJNDI() ?
            "org.apache.derby.jdbc.ClientConnectionPoolDataSource40" :
            "org.apache.derby.jdbc.BasicClientConnectionPoolDataSource40") :
            "org.apache.derby.jdbc.ClientConnectionPoolDataSource",

            JDBC.vmSupportsJDBC4() ?
            (JDBC.vmSupportsJNDI() ?
            "org.apache.derby.jdbc.ClientXADataSource40" :
            "org.apache.derby.jdbc.BasicClientXADataSource40") :
            "org.apache.derby.jdbc.ClientXADataSource",

            "jdbc:derby://");
    
    static final JDBCClient DERBYNETCLIENT_30 = new JDBCClient(
            "DerbyNetClient",
            "org.apache.derby.jdbc.ClientDriver",
            "org.apache.derby.jdbc.ClientDataSource",
            "org.apache.derby.jdbc.ClientConnectionPoolDataSource",
            "org.apache.derby.jdbc.ClientXADataSource",
            "jdbc:derby://");

    /**
     * The DB2 Universal JDBC network client.
     * AKA: JCC or DB2 client (was called DerbyNet earlier, the "old net"
     * client for Derby).
     */
    static final JDBCClient DB2CLIENT= new JDBCClient(
            "DB2Client",
            "com.ibm.db2.jcc.DB2Driver",
            null, null, null,
            "jdbc:derby:net://");
    
    /**
     * Is this the embdded client.
    */
    public boolean isEmbedded()
    {
    	return getName().startsWith("Embedded");
    }
    /**
     * Is this Derby's network client.
     */
    public boolean isDerbyNetClient()
    {
    	return getName().equals(DERBYNETCLIENT.getName());
    }
    /**
     * Is this DB2's Universal JDBC 
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
     * Get ConnectionPoolDataSource class name.
     *
     * @return class name for ConnectionPoolDataSource implementation.
     */
    public String getConnectionPoolDataSourceClassName() {
        return poolDsClassName;
    }

    /**
     * Get XADataSource class name.
     *
     * @return class name for XADataSource implementation.
     */
    public String getXADataSourceClassName() {
        return xaDsClassName;
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
                       String dataSourceClassName,
                       String connectionPoolDataSourceClassName,
                       String xaDataSourceClassName,
                       String urlBase) {
        this.frameWork          = frameWork;
        this.driverClassName    = driverClassName;
        this.dsClassName        = dataSourceClassName;
        this.poolDsClassName    = connectionPoolDataSourceClassName;
        this.xaDsClassName      = xaDataSourceClassName;
        this.urlBase            = urlBase;
    }
    
    private final String frameWork;
    private final String driverClassName;
    private final String dsClassName;
    private final String poolDsClassName;
    private final String xaDsClassName;
    private final String urlBase;
    
}
