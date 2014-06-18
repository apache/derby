/*

   Derby - Class org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource

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

import java.sql.SQLException;
import javax.sql.PooledConnection;


/**
   <P>
    This datasource is suitable for an application using embedded Derby,
    running on full Java SE 6 and higher, corresponding to 4.0 and higher.
   EmbeddedConnectionPoolDataSource is a ConnectionPoolDataSource
   implementation.
    </P>
	
	<P>A ConnectionPoolDataSource is a factory for PooledConnection
	objects. An object that implements this interface will typically be
	registered with a JNDI service.</P>
    
	<P>
	EmbeddedConnectionPoolDataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.</P>
    
	<UL>
    <LI>JDBC 4.0 - Java SE 6</LI>
    <LI>JDBC 4.1 - Java SE 7</LI>
    <LI>JDBC 4.2 - full Java SE 8</LI>
	</UL>

    <P>
    Use BasicEmbeddedConnectionPoolDataSource40 if your application runs on Java 8
    Compact Profile 2.
    </P>

	<P>EmbeddedConnectionPoolDataSource is serializable and referenceable.</P>

	<P>See EmbeddedDataSource for DataSource properties.</P>

 */
public class EmbeddedConnectionPoolDataSource extends EmbeddedDataSource
    implements EmbeddedConnectionPoolDataSourceInterface
{

	private static final long serialVersionUID = 7852784308039674160L;

	/**
		No-arg constructor.
	 */
	public EmbeddedConnectionPoolDataSource() {
		super();
	}

	/*
	 * ConnectionPoolDataSource methods
	 */

	/**
		Attempt to establish a database connection.

		@return a Connection to the database

		@exception SQLException if a database-access error occurs.
	*/
    @Override
	public final PooledConnection getPooledConnection() throws SQLException { 
		return createPooledConnection (getUser(), getPassword(), false);
	}

	/**
		Attempt to establish a database connection.

		@param username the database user on whose behalf the Connection is being made
		@param password the user's password

		@return a Connection to the database

		@exception SQLException if a database-access error occurs.
	*/
    @Override
	public final PooledConnection getPooledConnection(String username, 
												String password)
		 throws SQLException
	{
		return createPooledConnection (username, password, true);
	}
        
    /**
     * Create and return an EmbedPooledConnection from this instance
     * of EmbeddedConnectionPoolDataSource.
     *
     * @param user The user name
     * @param password The password
     * @param requestPassword @{@code false} if original call is from a
     *        no-argument constructor, otherwise {@code true}
     * @return A pooled connection
     * @throws SQLException An error occurred
     */
    private PooledConnection createPooledConnection (String user,
        String password, boolean requestPassword) throws SQLException
    {
        /* This object (EmbeddedConnectionPoolDataSource) is a JDBC 2
         * and JDBC 3 implementation of ConnectionPoolDatSource.  However,
         * it's possible that we are running with a newer driver (esp.
         * JDBC 4) in which case we should return a PooledConnection that
         * implements the newer JDBC interfaces--even if "this" object
         * does not itself satisfy those interfaces.  As an example, if
         * we have a JDK 6 application then even though this specific
         * object doesn't implement JDBC 4 (it only implements JDBC 2
         * and 3), we should still return a PooledConnection object that
         * *does* implement JDBC 4 because that's what a JDK 6 app
         * expects.
         *
         * By calling "findDriver()" here we will get the appropriate
         * driver for the JDK in use (ex. if using JDK 6 then findDriver()
         * will return the JDBC 4 driver).  If we then ask the driver to
         * give us a pooled connection, we will get a connection that
         * corresponds to whatever driver/JDBC implementation is being
         * used--which is what we want.  So for a JDK 6 application we
         * will correctly return a JDBC 4 PooledConnection. DERBY-2488.
         *
         * This type of scenario can occur if an application that was
         * previously running with an older JVM (ex. JDK 1.4/1.5) starts
         * running with a newer JVM (ex. JDK 6), in which case the app
         * is probably still using the "old" data source (ex. is still
         * instantiating EmbeddedConnectionPoolDataSource) instead of
         * the newer one (EmbeddedConnectionPoolDataSource40).
         */
        return findDriver().getNewPooledConnection(
            this, user, password, requestPassword);
    }

}





