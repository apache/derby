/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.jdbc
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.jdbc;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.ResultSet;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Interface-ized from EmbedConnectionContext.  Some basic
 * connection attributes that can be obtained from jdbc.
 *
 * @author jamie
 */
public interface ConnectionContext 
{
	public static final String CONTEXT_ID = "JDBC_ConnectionContext";

	/**
		Get a new connection object equivalent to the call
		<PRE>
		DriverManager.getConnection("jdbc:default:connection");
		</PRE>

		@exception SQLException Parent connection has been closed.
	*/
	public Connection getNestedConnection(boolean internal) throws SQLException;

	/**
	 * Get a jdbc ResultSet based on the execution ResultSet.
	 *
	 * @param executionResultSet	a result set as gotten from execution
	 *	
	 * @exception java.sql.SQLException	on error
	 */	
	public java.sql.ResultSet getResultSet
	(
		ResultSet 				executionResultSet
	) throws java.sql.SQLException;
}
