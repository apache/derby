/*
 
   Derby - Class org.apache.derby.jdbc.ClientDataSource40
 
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

import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;
import org.apache.derby.client.am.ClientMessageId;
import org.apache.derby.client.am.SqlException;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This datasource is suitable for a client/server use of Derby,
 * running on the following platforms:
 * <p>
 * <ul>
 *   <li>Java SE 7 (JDBC 4.1) and
 *   <li>full Java SE 8 (JDBC 4.2).
 * </ul>
 * <p>
 * Use BasicClientDataSource40 if your application runs on Java 8
 * Compact Profile 2.
 * <p>
 * Use ClientDataSource if your application runs on the following
 * platforms:
 * <p>
 * <ul>
 *  <li> JDBC 4.0 - Java SE 6
 *  <li> JDBC 3.0 - J2SE 5.0
 * </ul>
 * <p>
 * ClientDataSource40 is a simple data source implementation
 * that can be used for establishing connections in a
 * non-pooling, non-distributed environment.
 * The class ClientConnectionPoolDataSource40 can be used in a connection pooling environment,
 * and the class ClientXADataSource40 can be used in a distributed, and pooling
 * environment.
 *
 * <p>The example below registers a DNC data source object with a JNDI naming service.
 * <pre>
 * org.apache.derby.client.ClientDataSource40 dataSource = new org.apache.derby.client.ClientDataSource40 ();
 * dataSource.setServerName ("my_derby_database_server");
 * dataSource.setDatabaseName ("my_derby_database_name");
 * javax.naming.Context context = new javax.naming.InitialContext();
 * context.bind ("jdbc/my_datasource_name", dataSource);
 * </pre>
 * The first line of code in the example creates a data source object.
 * The next two lines initialize the data source's
 * properties. Then a Java object that references the initial JNDI naming
 * context is created by calling the
 * InitialContext() constructor, which is provided by JNDI.
 * System properties (not shown) are used to tell JNDI the
 * service provider to use. The JNDI name space is hierarchical,
 * similar to the directory structure of many file
 * systems. The data source object is bound to a logical JNDI name
 * by calling Context.bind(). In this case the JNDI name
 * identifies a subcontext, "jdbc", of the root naming context
 * and a logical name, "my_datasource_name", within the jdbc
 * subcontext. This is all of the code required to deploy
 * a data source object within JNDI. This example is provided
 * mainly for illustrative purposes. We expect that developers
 * or system administrators will normally use a GUI tool to
 * deploy a data source object.
 * <p/>
 * Once a data source has been registered with JNDI,
 * it can then be used by a JDBC application, as is shown in the
 * following example.
 * <pre>
 * javax.naming.Context context = new javax.naming.InitialContext ();
 * javax.sql.DataSource dataSource = (javax.sql.DataSource) context.lookup ("jdbc/my_datasource_name");
 * java.sql.Connection connection = dataSource.getConnection ("user", "password");
 * </pre>
 * The first line in the example creates a Java object
 * that references the initial JNDI naming context. Next, the
 * initial naming context is used to do a lookup operation
 * using the logical name of the data source. The
 * Context.lookup() method returns a reference to a Java Object,
 * which is narrowed to a javax.sql.DataSource object. In
 * the last line, the DataSource.getConnection() method
 * is called to produce a database connection.
 * <p/>
 * This simple data source subclass of ClientBaseDataSource maintains
 * it's own private <code>password</code> property.
 * <p/>
 * The specified password, along with the user, is validated by DERBY.
 * This property can be overwritten by specifing
 * the password parameter on the DataSource.getConnection() method call.
 * <p/>
 * This password property is not declared transient, and therefore
 * may be serialized to a file in clear-text, or stored
 * to a JNDI server in clear-text when the data source is saved.
 * Care must taken by the user to prevent security
 * breaches.
 * <p/>
 */
public class ClientDataSource40 extends ClientDataSource
    implements javax.sql.DataSource /* compile-time check for 4.1 extension */
{
   private static final long serialVersionUID = -3936981157692787843L;
    
    public ClientDataSource40() {
        super();
    }   
    
    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    public  Logger getParentLogger()
        throws SQLFeatureNotSupportedException
    {
        throw (SQLFeatureNotSupportedException)
            (
             new SqlException( null, new ClientMessageId(SQLState.NOT_IMPLEMENTED), "getParentLogger" )
             ).getSQLException();
    }
    
}
