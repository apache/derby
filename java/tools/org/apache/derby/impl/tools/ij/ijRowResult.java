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
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * This is an impl for when 1 row of a result set is
 * the intended use of it.  The caller *must not*
 * do a "next" on the result set.  It's up to them
 * to make sure that doesn't happen.
 *
 * @author ames
 */
public class ijRowResult extends ijResultImpl {

	ResultSet rowResult;
	boolean hadRow;

	public ijRowResult(ResultSet r, boolean hadRow) {
		rowResult = r;
		this.hadRow = hadRow;
	}

	public boolean isNextRowOfResultSet() { return true; }

	public ResultSet getNextRowOfResultSet() { return hadRow?rowResult:null; }

	public SQLWarning getSQLWarnings() throws SQLException { return rowResult.getWarnings(); }
	public void clearSQLWarnings() throws SQLException { rowResult.clearWarnings(); }
}
