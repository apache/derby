/*

   Derby - Class org.apache.derby.impl.jdbc.XAStatementControl

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

package org.apache.derby.impl.jdbc;

import org.apache.derby.iapi.jdbc.BrokeredConnection;
import org.apache.derby.iapi.jdbc.BrokeredStatementControl;
import org.apache.derby.iapi.jdbc.BrokeredStatement;
import org.apache.derby.iapi.jdbc.BrokeredPreparedStatement;
import org.apache.derby.iapi.jdbc.BrokeredCallableStatement;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet;
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
	private EmbedConnection	realConnection;
	private Statement			realStatement;
	private PreparedStatement	realPreparedStatement;
	private CallableStatement	realCallableStatement;

	private XAStatementControl(EmbedXAConnection xaConnection) {
		this.xaConnection = xaConnection;
		this.realConnection = xaConnection.realConnection;
		this.applicationConnection = xaConnection.currentConnectionHandle;
	}

//IC see: https://issues.apache.org/jira/browse/DERBY-1163
	XAStatementControl(EmbedXAConnection xaConnection, 
                                Statement realStatement) throws SQLException {
		this(xaConnection);
		this.realStatement = realStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this);
        
        ((EmbedStatement) realStatement).setApplicationStatement(
                applicationStatement);
	}
//IC see: https://issues.apache.org/jira/browse/DERBY-1163
	XAStatementControl(EmbedXAConnection xaConnection, 
                PreparedStatement realPreparedStatement, 
                String sql, Object generatedKeys) throws SQLException {            
		this(xaConnection);
		this.realPreparedStatement = realPreparedStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this, sql, generatedKeys);
        ((EmbedStatement) realPreparedStatement).setApplicationStatement(
                applicationStatement);
	}
//IC see: https://issues.apache.org/jira/browse/DERBY-1163
	XAStatementControl(EmbedXAConnection xaConnection, 
                CallableStatement realCallableStatement, 
                String sql) throws SQLException {
		this(xaConnection);
		this.realCallableStatement = realCallableStatement;
		this.applicationStatement = applicationConnection.newBrokeredStatement(this, sql);
        ((EmbedStatement) realCallableStatement).setApplicationStatement(
                applicationStatement);
	}

	/**
	 * Close the realStatement within this control. 
	 */
	public void closeRealStatement() throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-4310
//IC see: https://issues.apache.org/jira/browse/DERBY-4155
		realStatement.close();
	}
	
	/**
	 * Close the realCallableStatement within this control. 
	 */
	public void closeRealCallableStatement() throws SQLException {
		realCallableStatement.close();
	}
	
	/**
	 * Close the realPreparedStatement within this control. 
	 */
	public void closeRealPreparedStatement() throws SQLException {
		realPreparedStatement.close();
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
            ((EmbedStatement) realStatement).setApplicationStatement(
                    applicationStatement);
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
            ((EmbedStatement) realPreparedStatement).setApplicationStatement(
                        applicationStatement);
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
            ((EmbedStatement) realCallableStatement).setApplicationStatement(
                    applicationStatement);
		}
		else {
			// application connection is different, therefore the outer application
			// statement is closed, so just return the realStatement. It should be
			// closed by virtue of its application connection being closed.
		}
		return realCallableStatement;
	}

    /**
     * Don't need to wrap the ResultSet but do need to update its
     * application Statement reference to be the one the application
     * used to create the ResultSet.
     */
	public ResultSet wrapResultSet(Statement s, ResultSet rs) {
        if (rs != null)
            ((EmbedResultSet) rs).setApplicationStatement(s);
		return rs;
	}

	/**
		Can cursors be held across commits.
	*/
	public int checkHoldCursors(int holdability) throws SQLException {
		return xaConnection.checkHoldCursors(holdability, true);
 	}
}
