/*

   Derby - Class org.apache.derby.iapi.jdbc.BrokeredConnectionControl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.iapi.jdbc;

import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

/**
	Provides control over a BrokeredConnection
*/
public interface BrokeredConnectionControl
{
	/**
		Return the real JDBC connection for the brokered connection.
	*/
	public EngineConnection	getRealConnection() throws SQLException;

	/**
		Notify the control class that a SQLException was thrown
		during a call on one of the brokered connection's methods.
	*/
	public void notifyException(SQLException sqle);


	/**
		Allow control over setting auto commit mode.
	*/
	public void checkAutoCommit(boolean autoCommit) throws SQLException;

	/**
		Allow control over creating a Savepoint (JDBC 3.0)
	*/
	public void checkSavepoint() throws SQLException;

	/**
		Allow control over calling rollback.
	*/
	public void checkRollback() throws SQLException;

	/**
		Allow control over calling commit.
	*/
	public void checkCommit() throws SQLException;

    /**
     * Check if the brokered connection can be closed.
     *
     * @throws SQLException if it is not allowed to call close on the brokered
     * connection
     */
    public void checkClose() throws SQLException;

	/**
		Can cursors be held across commits.
        @param downgrade true to downgrade the holdability,
        false to throw an exception.
	*/
	public int checkHoldCursors(int holdability, boolean downgrade)
        throws SQLException;

	/**
		Returns true if isolation level has been set using JDBC/SQL.
	*/
	public boolean isIsolationLevelSetUsingSQLorJDBC() throws SQLException;
	/**
		Reset the isolation level flag used to keep state in 
		BrokeredConnection. It will get set to true when isolation level 
		is set using JDBC/SQL. It will get reset to false at the start
		and the end of a global transaction.
	*/
	public void resetIsolationLevelFlag() throws SQLException;

	/**
		Close called on BrokeredConnection. If this call
		returns true then getRealConnection().close() will be called.
	*/
	public boolean closingConnection() throws SQLException;

	/**
		Optionally wrap a Statement with another Statement.
	*/
	public Statement wrapStatement(Statement realStatement) throws SQLException;

	/**
		Optionally wrap a PreparedStatement with another PreparedStatement.
	*/
	public PreparedStatement wrapStatement(PreparedStatement realStatement, String sql, Object generateKeys)  throws SQLException;

	/**
		Optionally wrap a CallableStatement with an CallableStatement.
	*/
	public CallableStatement wrapStatement(CallableStatement realStatement, String sql) throws SQLException;
        
        /**
         * Close called on the associated PreparedStatement object
         * @param statement PreparedStatement object on which the close event 
         * occurred     
         */
        public void onStatementClose(PreparedStatement statement);
        
        /**
         * Error occurred on associated PreparedStatement object
         * @param statement PreparedStatement object on which the 
         * error occured
         * @param sqle      The SQLExeption that caused the error
         */
        public void onStatementErrorOccurred(PreparedStatement statement,SQLException sqle);
        
}
