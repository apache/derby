/*

   Derby - Class org.apache.derby.jdbc.XAStatementControl

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.BrokeredStatementControl;
import org.apache.derby.iapi.jdbc.BrokeredStatement;
import org.apache.derby.iapi.jdbc.BrokeredPreparedStatement;
import org.apache.derby.iapi.jdbc.BrokeredCallableStatement;
import org.apache.derby.impl.jdbc.EmbedConnection20;
import org.apache.derby.impl.jdbc.EmbedStatement;
import org.apache.derby.impl.jdbc.EmbedPreparedStatement;

import java.sql.*;

/**
	The Statement returned by an Connection returned by a XAConnection
	needs to float across the underlying real connections. We do this by implementing
	a wrapper statement.
*/
final class XAStatementControl implements BrokeredStatementControl {

	/**
	*/
	private final EmbedXAConnection	xaConnection;
	private final BrokeredConnection	applicationConnection;
	BrokeredStatement		applicationStatement;
	private EmbedConnection20	realConnection;
	private Statement			realStatement;
	private PreparedStatement	realPreparedStatement;
	private CallableStatement	realCallableStatement;

	private XAStatementControl(EmbedXAConnection xaConnection) {
		this.xaConnection = xaConnection;
		this.realConnection = xaConnection.realConnection;
		this.applicationConnection = xaConnection.currentConnectionHandle;
	}

	XAStatementControl(EmbedXAConnection xaConnection, Statement realStatement) throws SQLException {
		this(xaConnection);
		this.realStatement = realStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this);
	}
	XAStatementControl(EmbedXAConnection xaConnection, PreparedStatement realPreparedStatement, String sql, Object generatedKeys) throws SQLException {
		this(xaConnection);
		this.realPreparedStatement = realPreparedStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this, sql, generatedKeys);
	}
	XAStatementControl(EmbedXAConnection xaConnection, CallableStatement realCallableStatement, String sql) throws SQLException {
		this(xaConnection);
		this.realCallableStatement = realCallableStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this, sql);
	}

	public Statement getRealStatement() throws SQLException {

		// 
		if (applicationConnection == xaConnection.currentConnectionHandle) {

			// Application connection is the same.
			if (realConnection == xaConnection.realConnection)
				return realStatement;

			// If we switched back to a local connection, and the first access is through
			// a non-connection object (e.g. statement then realConnection will be null)
			if (xaConnection.realConnection == null) {
				// force the connection
				xaConnection.getRealConnection();
			}

			// underlying connection has changed.
			// create new Statement
			Statement newStatement = applicationStatement.createDuplicateStatement(xaConnection.realConnection, realStatement);
			((EmbedStatement) realStatement).transferBatch((EmbedStatement) newStatement);

			try {
				realStatement.close();
			} catch (SQLException sqle) {
			}

			realStatement = newStatement;
			realConnection = xaConnection.realConnection;
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realStatement;
	}

	public PreparedStatement getRealPreparedStatement() throws SQLException {
		// 
		if (applicationConnection == xaConnection.currentConnectionHandle) {
			// Application connection is the same.
			if (realConnection == xaConnection.realConnection)
				return realPreparedStatement;

			// If we switched back to a local connection, and the first access is through
			// a non-connection object (e.g. statement then realConnection will be null)
			if (xaConnection.realConnection == null) {
				// force the connection
				xaConnection.getRealConnection();
			}

			// underlying connection has changed.
			// create new PreparedStatement
			PreparedStatement newPreparedStatement =
				((BrokeredPreparedStatement) applicationStatement).createDuplicateStatement(xaConnection.realConnection, realPreparedStatement);


			// ((EmbedStatement) realPreparedStatement).transferBatch((EmbedStatement) newPreparedStatement);
			((EmbedPreparedStatement) realPreparedStatement).transferParameters((EmbedPreparedStatement) newPreparedStatement);

			try {
				realPreparedStatement.close();
			} catch (SQLException sqle) {
			}

			realPreparedStatement = newPreparedStatement;
			realConnection = xaConnection.realConnection;
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realPreparedStatement;
	}

	public CallableStatement getRealCallableStatement() throws SQLException {
		if (applicationConnection == xaConnection.currentConnectionHandle) {
			// Application connection is the same.
			if (realConnection == xaConnection.realConnection)
				return realCallableStatement;

			// If we switched back to a local connection, and the first access is through
			// a non-connection object (e.g. statement then realConnection will be null)
			if (xaConnection.realConnection == null) {
				// force the connection
				xaConnection.getRealConnection();
			}

			// underlying connection has changed.
			// create new PreparedStatement
			CallableStatement newCallableStatement =
				((BrokeredCallableStatement) applicationStatement).createDuplicateStatement(xaConnection.realConnection, realCallableStatement);

			((EmbedStatement) realCallableStatement).transferBatch((EmbedStatement) newCallableStatement);

			try {
				realCallableStatement.close();
			} catch (SQLException sqle) {
			}

			realCallableStatement = newCallableStatement;
			realConnection = xaConnection.realConnection;
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realCallableStatement;
	}

	public ResultSet wrapResultSet(ResultSet rs) {
		return rs;
	}

	/**
		Can cursors be held across commits.
	*/
	public void checkHoldCursors(int holdability) throws SQLException {
		xaConnection.checkHoldCursors(holdability);
	}
}
