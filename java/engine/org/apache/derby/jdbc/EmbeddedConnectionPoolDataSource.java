/*

   Derby - Class org.apache.derby.jdbc.EmbeddedConnectionPoolDataSource

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.jdbc;

import java.sql.SQLException;

/* -- New jdbc 20 extension types --- */
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/** 
	EmbeddedConnectionPoolDataSource is Cloudscape's ConnectionPoolDataSource implementation.
	

	<P>A ConnectionPoolDataSource is a factory for PooledConnection
	objects. An object that implements this interface will typically be
	registered with a JNDI service.
	<P>
	EmbeddedConnectionPoolDataSource automatically supports the correct JDBC specification version
	for the Java Virtual Machine's environment.
	<UL>
	<LI> JDBC 3.0 - Java 2 - JDK 1.4
	<LI> JDBC 2.0 - Java 2 - JDK 1.2,1.3
	</UL>

	<P>EmbeddedConnectionPoolDataSource is serializable and referenceable.

	<P>See EmbeddedDataSource for DataSource properties.

 */
public class EmbeddedConnectionPoolDataSource extends EmbeddedDataSource
		implements	javax.sql.ConnectionPoolDataSource
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
	public final PooledConnection getPooledConnection() throws SQLException { 
		return new EmbedPooledConnection(this, getUser(), getPassword(), false);
	}

	/**
		Attempt to establish a database connection.

		@param user the database user on whose behalf the Connection is being made
		@param password the user's password

		@return a Connection to the database

		@exception SQLException if a database-access error occurs.
	*/
	public final PooledConnection getPooledConnection(String username, 
												String password)
		 throws SQLException
	{
		return new EmbedPooledConnection(this, username, password, true);
	}

}





