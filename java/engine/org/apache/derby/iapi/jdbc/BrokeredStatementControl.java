/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.jdbc
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.jdbc;

import java.sql.*;

/**
	Provides control over a BrokeredStatement, BrokeredPreparedStatement or BrokeredCallableStatement
*/
public interface BrokeredStatementControl
{
	/**
		IBM Copyright &copy notice.
	*/

	String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;

	/**
		Can cursors be held across commits.
	*/
	public void checkHoldCursors(int holdability) throws SQLException;

	/**
		Return the real JDBC statement for the brokered statement
		when this is controlling a Statement.
	*/
	public Statement	getRealStatement() throws SQLException;

	/**
		Return the real JDBC PreparedStatement for the brokered statement
		when this is controlling a PreparedStatement.
	*/
	public PreparedStatement	getRealPreparedStatement() throws SQLException;


	/**
		Return the real JDBC CallableStatement for the brokered statement
		when this is controlling a CallableStatement.
	*/
	public CallableStatement	getRealCallableStatement() throws SQLException;

	/**
		Optionally wrap a returned ResultSet in another ResultSet.
	*/
	public ResultSet	wrapResultSet(ResultSet rs);
}
