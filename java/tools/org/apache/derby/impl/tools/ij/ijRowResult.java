/*

   Derby - Class org.apache.derby.impl.tools.ij.ijRowResult

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
