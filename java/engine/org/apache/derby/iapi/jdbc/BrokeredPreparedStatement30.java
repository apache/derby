/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.jdbc
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.jdbc;

import java.sql.*;
import java.net.URL;

/**
	JDBC 3 implementation of PreparedStatement.
*/
public class BrokeredPreparedStatement30 extends BrokeredPreparedStatement {

	private final Object generatedKeys;
	public BrokeredPreparedStatement30(BrokeredStatementControl control, int jdbcLevel, String sql, Object generatedKeys) throws SQLException {
		super(control, jdbcLevel, sql);
		this.generatedKeys = generatedKeys;
	}

	public final void setURL(int i, URL x)
        throws SQLException
    {
        getPreparedStatement().setURL( i, x);
    }
    public final ParameterMetaData getParameterMetaData()
        throws SQLException
    {
        return getPreparedStatement().getParameterMetaData();
    }
	/**
		Create a duplicate PreparedStatement to this, including state, from the passed in Connection.
	*/
	public PreparedStatement createDuplicateStatement(Connection conn, PreparedStatement oldStatement) throws SQLException {

		PreparedStatement newStatement;

		if (generatedKeys == null)
			newStatement = conn.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		else {
			// The prepareStatement() calls that take a generated key value do not take resultSet* type
			// parameters, but since they don't return ResultSets that is OK. There are only for INSERT statements.
			if (generatedKeys instanceof Integer)
				newStatement = conn.prepareStatement(sql, ((Integer) generatedKeys).intValue());
			else if (generatedKeys instanceof int[])
				newStatement = conn.prepareStatement(sql, (int[]) generatedKeys);
			else
				newStatement = conn.prepareStatement(sql, (String[]) generatedKeys);
		}


		setStatementState(oldStatement, newStatement);

		return newStatement;
	}
}
