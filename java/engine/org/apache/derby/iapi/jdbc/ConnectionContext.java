/*

   Derby - Class org.apache.derby.iapi.jdbc.ConnectionContext

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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
