/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.jdbc
   (C) Copyright IBM Corp. 2001, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2001_2004;

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





