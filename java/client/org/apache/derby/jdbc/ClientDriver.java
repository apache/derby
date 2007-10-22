/*

   Derby - Class org.apache.derby.jdbc.ClientDriver

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

package org.apache.derby.jdbc;

import java.util.Enumeration;
import java.util.Properties;
import java.sql.SQLException;
import org.apache.derby.client.am.Configuration;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.client.am.Utils;
import org.apache.derby.client.am.Version;
import org.apache.derby.client.am.ClientJDBCObjectFactory;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.net.ClientJDBCObjectFactoryImpl;
import org.apache.derby.shared.common.reference.Attribute;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.reference.MessageId;


public class ClientDriver implements java.sql.Driver {
    private transient int traceFileSuffixIndex_ = 0;

    private final static int DERBY_REMOTE_PROTOCOL = 1;
    
    private static ClientJDBCObjectFactory factoryObject = null;

    static private SQLException exceptionsOnLoadDriver__ = null;
    // Keep track of the registere driver so that we can deregister it if we're a stored proc.
    static private ClientDriver registeredDriver__ = null;

    static {
        // This may possibly hit the race-condition bug of java 1.1.
        // The Configuration static clause should execute before the following line does.
        if (Configuration.exceptionsOnLoadResources != null) {
            exceptionsOnLoadDriver__ =
                    Utils.accumulateSQLException(
                            Configuration.exceptionsOnLoadResources.getSQLException(),
                            exceptionsOnLoadDriver__);
        }
        try {
            registeredDriver__ = new ClientDriver();
            java.sql.DriverManager.registerDriver(registeredDriver__);
        } catch (java.sql.SQLException e) {
            // A null log writer is passed, because jdbc 1 sql exceptions are automatically traced
            exceptionsOnLoadDriver__ =
                    new SqlException(null, 
                        new ClientMessageId(SQLState.JDBC_DRIVER_REGISTER)).getSQLException();
            exceptionsOnLoadDriver__.setNextException(e);
        }
    }

    public ClientDriver() {
    }

    public java.sql.Connection connect(String url,
                                       java.util.Properties properties) throws java.sql.SQLException {
        org.apache.derby.client.net.NetConnection conn;
        try {    
            if (exceptionsOnLoadDriver__ != null) {
                throw exceptionsOnLoadDriver__;
            }

            if (properties == null) {
                properties = new java.util.Properties();
            }

            java.util.StringTokenizer urlTokenizer =
                    new java.util.StringTokenizer(url, "/:= \t\n\r\f", true);

            int protocol = tokenizeProtocol(url, urlTokenizer);
            if (protocol == 0) {
                return null; // unrecognized database URL prefix.
            }

            String slashOrNull = null;
            if (protocol == DERBY_REMOTE_PROTOCOL) {
                try {
                    slashOrNull = urlTokenizer.nextToken(":/");
                } catch (java.util.NoSuchElementException e) {
                    // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
                    throw new SqlException(null, 
                        new ClientMessageId(SQLState.MALFORMED_URL),
                        url, e);
                }
            }
            String server = tokenizeServerName(urlTokenizer, url);    // "/server"
            int port = tokenizeOptionalPortNumber(urlTokenizer, url); // "[:port]/"
            if (port == 0) {
                port = ClientDataSource.propertyDefault_portNumber;
            }

            // database is the database name and attributes.  This will be
            // sent to network server as the databaseName
            String database = tokenizeDatabase(urlTokenizer, url); // "database"
            java.util.Properties augmentedProperties = tokenizeURLProperties(url, properties);
            database = appendDatabaseAttributes(database,augmentedProperties);

            int traceLevel;
            try {
                traceLevel = ClientDataSource.getTraceLevel(augmentedProperties);
            } catch (java.lang.NumberFormatException e) {
                // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
                throw new SqlException(null, 
                    new ClientMessageId(SQLState.TRACELEVEL_FORMAT_INVALID), e);
            }

            // Jdbc 1 connections will write driver trace info on a
            // driver-wide basis using the jdbc 1 driver manager log writer.
            // This log writer may be narrowed to the connection-level
            // This log writer will be passed to the agent constructor.
            org.apache.derby.client.am.LogWriter dncLogWriter =
                    ClientDataSource.computeDncLogWriterForNewConnection(java.sql.DriverManager.getLogWriter(),
                            ClientDataSource.getTraceDirectory(augmentedProperties),
                            ClientDataSource.getTraceFile(augmentedProperties),
                            ClientDataSource.getTraceFileAppend(augmentedProperties),
                            traceLevel,
                            "_driver",
                            traceFileSuffixIndex_++);
            
            
            conn = (org.apache.derby.client.net.NetConnection)getFactory().
                    newNetConnection((org.apache.derby.client.net.NetLogWriter) 
                    dncLogWriter,
                    java.sql.DriverManager.getLoginTimeout(),
                    server,
                    port,
                    database,
                    augmentedProperties);
        } catch(SqlException se) {
            throw se.getSQLException();
        }
        
        if(conn.isConnectionNull())
            return null;
        
        return conn;
    }

    /**
     * Append attributes to the database name except for user/password 
     * which are sent as part of the protocol, and SSL which is used 
     * locally in the client.
     * Other attributes will  be sent to the server with the database name
     * Assumes augmentedProperties is not null
     * 
	 * @param database - Short database name
	 * @param augmentedProperties - Set of properties to append as attributes
	 * @return databaseName + attributes (e.g. mydb;create=true) 
	 */
	private String appendDatabaseAttributes(String database, Properties augmentedProperties) {
	
		StringBuffer longDatabase = new StringBuffer(database);
		for (Enumeration keys = augmentedProperties.propertyNames();
			 keys.hasMoreElements() ;)
		{
			String key = (String) keys.nextElement();
			if (key.equals(Attribute.USERNAME_ATTR) || 
                key.equals(Attribute.PASSWORD_ATTR) ||
                key.equals(Attribute.SSL_ATTR))
				continue;
			longDatabase.append(";" + key + "=" + augmentedProperties.getProperty(key));
		}
		return longDatabase.toString();
	}

	public boolean acceptsURL(String url) throws java.sql.SQLException {
        try
        {
            java.util.StringTokenizer urlTokenizer = 
                    new java.util.StringTokenizer(url, "/:=; \t\n\r\f", true);
            int protocol = tokenizeProtocol(url, urlTokenizer);
            return protocol != 0;
        }
        catch ( SqlException se )
        {
            throw se.getSQLException();
        }
    }

    public java.sql.DriverPropertyInfo[] getPropertyInfo(String url,
                                                         java.util.Properties properties) throws java.sql.SQLException {
        java.sql.DriverPropertyInfo driverPropertyInfo[] = new java.sql.DriverPropertyInfo[2];

        // If there are no properties set already,
        // then create a dummy properties just to make the calls go thru.
        if (properties == null) {
            properties = new java.util.Properties();
        }

        driverPropertyInfo[0] =
                new java.sql.DriverPropertyInfo(Attribute.USERNAME_ATTR,
                        properties.getProperty(Attribute.USERNAME_ATTR, ClientDataSource.propertyDefault_user));

        driverPropertyInfo[1] =
                new java.sql.DriverPropertyInfo(Attribute.PASSWORD_ATTR,
                        properties.getProperty(Attribute.PASSWORD_ATTR));

        driverPropertyInfo[0].description =
            SqlException.getMessageUtil().getTextMessage(
                MessageId.CONN_USERNAME_DESCRIPTION);
        driverPropertyInfo[1].description =
            SqlException.getMessageUtil().getTextMessage(
                MessageId.CONN_PASSWORD_DESCRIPTION);

        driverPropertyInfo[0].required = true;
        driverPropertyInfo[1].required = false; // depending on the security mechanism

        return driverPropertyInfo;
    }

    public int getMajorVersion() {
        return Version.getMajorVersion();
    }

    public int getMinorVersion() {
        return Version.getMinorVersion();
    }

    public boolean jdbcCompliant() {
        return Configuration.jdbcCompliant;
    }

    // ----------------helper methods---------------------------------------------

    // Tokenize one of the following:
    //  "jdbc:derby:"
    // and return 0 if the protcol is unrecognized
    // return DERBY_PROTOCOL for "jdbc:derby"
    private static int tokenizeProtocol(String url, java.util.StringTokenizer urlTokenizer) throws SqlException {
        // Is this condition necessary, StringTokenizer constructor may do this for us
        if (url == null) {
            return 0;
        }

        if (urlTokenizer == null) {
            return 0;
        }

        try {
            String jdbc = urlTokenizer.nextToken(":");
            if (!jdbc.equals("jdbc")) {
                return 0;
            }
            if (!urlTokenizer.nextToken(":").equals(":")) {
                return 0; // Skip over the first colon in jdbc:derby:
            }
            String dbname = urlTokenizer.nextToken(":");
            int protocol = 0;
            if (dbname.equals("derby") && (url.indexOf("derby://") != -1)) {
                // For Derby AS need to check for // since jdbc:derby: is also the
                // embedded prefix
                protocol = DERBY_REMOTE_PROTOCOL;
            } else {
                return 0;
            }

            if (!urlTokenizer.nextToken(":").equals(":")) {
                return 0; // Skip over the second colon in jdbc:derby:
            }

            return protocol;
        } catch (java.util.NoSuchElementException e) {
            return 0;
        }
    }

    // tokenize "/server" from URL jdbc:derby://server:port/
    // returns server name
    private static String tokenizeServerName(java.util.StringTokenizer urlTokenizer,
                                             String url) throws SqlException {
        try {
            if (!urlTokenizer.nextToken("/").equals("/"))
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
            {
                throw new SqlException(null, 
                    new ClientMessageId(SQLState.MALFORMED_URL), url);
            }
            return urlTokenizer.nextToken("/:");
        } catch (java.util.NoSuchElementException e) {
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
                throw new SqlException(null, 
                    new ClientMessageId(SQLState.MALFORMED_URL), url);
        }
    }

    // tokenize "[:portNumber]/" from URL jdbc:derby://server[:port]/
    // returns the portNumber or zero if portNumber is not specified.
    private static int tokenizeOptionalPortNumber(java.util.StringTokenizer urlTokenizer,
                                                  String url) throws SqlException {
        try {
            String firstToken = urlTokenizer.nextToken(":/");
            if (firstToken.equals(":")) {
                String port = urlTokenizer.nextToken("/");
                if (!urlTokenizer.nextToken("/").equals("/")) {
                    // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
                    throw new SqlException(null, 
                        new ClientMessageId(SQLState.MALFORMED_URL), url);
                }
                return Integer.parseInt(port);
            } else if (firstToken.equals("/")) {
                return 0;
            } else {
                // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
                throw new SqlException(null, 
                    new ClientMessageId(SQLState.MALFORMED_URL), url);
            }
        } catch (java.util.NoSuchElementException e) {
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
            throw new SqlException(null, 
                new ClientMessageId(SQLState.MALFORMED_URL), url, e);
        }
    }

    //return database name
    private static String tokenizeDatabase(java.util.StringTokenizer urlTokenizer,
                                           String url) throws SqlException {
        try {
        	// DERBY-618 - database name can contain spaces in the path
            String databaseName = urlTokenizer.nextToken("\t\n\r\f;");
            return databaseName;
        } catch (java.util.NoSuchElementException e) {
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
            throw new SqlException(null, 
                new ClientMessageId(SQLState.MALFORMED_URL), url, e);
        }
    }

    private static java.util.Properties tokenizeURLProperties(String url,
                                                              java.util.Properties properties)
            throws SqlException {
        String attributeString = null;
        int attributeIndex = -1;

        if ((url != null) &&
                ((attributeIndex = url.indexOf(";")) != -1)) {
            attributeString = url.substring(attributeIndex);
        }
        return ClientDataSource.tokenizeAttributes(attributeString, properties);
    }
    
    /**
     *This method returns an Implementation
     *of ClientJDBCObjectFactory depending on
     *VM under use
     *Currently it returns either
     *ClientJDBCObjectFactoryImpl
     *(or)
     *ClientJDBCObjectFactoryImpl40
     */
    
    public static ClientJDBCObjectFactory getFactory() {
        if(factoryObject!=null)
            return factoryObject;
        if(Configuration.supportsJDBC40()) {
            factoryObject = createJDBC40FactoryImpl();
        } else {
            factoryObject = createDefaultFactoryImpl();
        }
        return factoryObject;
    }
    
    /**
     *Returns an instance of the ClientJDBCObjectFactoryImpl class
     */
    private static ClientJDBCObjectFactory createDefaultFactoryImpl() {
        return  new ClientJDBCObjectFactoryImpl();
    }
    
    /**
     *Returns an instance of the ClientJDBCObjectFactoryImpl40 class
     *If a ClassNotFoundException occurs then it returns an
     *instance of ClientJDBCObjectFactoryImpl
     *
     *If a future version of JDBC comes then
     *a similar method would be added say createJDBCXXFactoryImpl
     *in which if  the class is not found then it would
     *return the lower version thus having a sort of cascading effect
     *until it gets a valid instance
     */
    
    private static ClientJDBCObjectFactory createJDBC40FactoryImpl() {
        final String factoryName =
                "org.apache.derby.client.net.ClientJDBCObjectFactoryImpl40";
        try {
            return (ClientJDBCObjectFactory)
            Class.forName(factoryName).newInstance();
        } catch (ClassNotFoundException cnfe) {
            return createDefaultFactoryImpl();
        } catch (InstantiationException ie) {
            return createDefaultFactoryImpl();
        } catch (IllegalAccessException iae) {
            return createDefaultFactoryImpl();
        }
    }
}



