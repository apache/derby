/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.tools.ij
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
