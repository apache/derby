/*

   Derby - Class org.apache.derby.impl.tools.ij.ijResult

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

package org.apache.derby.impl.tools.ij;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Vector;

/**
 * This is a wrapper for results coming out of the
 * ij parser.
 *
 * @author ames
 *
 */
public interface ijResult {
	boolean isConnection();
	boolean isStatement();
	boolean isResultSet() throws SQLException;
	boolean isUpdateCount() throws SQLException;
	boolean isNextRowOfResultSet();
	boolean isVector();
	boolean isMulti();
	boolean isException();
	boolean hasWarnings() throws SQLException ;

	Connection getConnection();
	Statement getStatement();
	int getUpdateCount() throws SQLException;
	ResultSet getResultSet() throws SQLException;
	ResultSet getNextRowOfResultSet();
	Vector getVector();
	SQLException getException();

	void closeStatement() throws SQLException ;

	/*
		Since they will all need to do warning calls/clears, may as
		well stick it here.
	 */
	SQLWarning getSQLWarnings() throws SQLException ;
	void clearSQLWarnings() throws SQLException ;
}
