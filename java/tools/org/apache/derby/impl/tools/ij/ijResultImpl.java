/*

   Derby - Class org.apache.derby.impl.tools.ij.ijResultImpl

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

import org.apache.derby.iapi.tools.i18n.LocalizedResource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.Vector;

/**
 * This is an empty impl for reuse of code.
 *
 * @author ames
 */
abstract class ijResultImpl implements ijResult {
	public boolean isConnection() { return false; }
	public boolean isStatement() { return false; }
	public boolean isResultSet() throws SQLException { return false; }
	public boolean isUpdateCount() throws SQLException { return false; }
	public boolean isNextRowOfResultSet() { return false; }
	public boolean isVector() { return false; }
	public boolean isMulti() { return false; }
	public boolean isException() { return false; }
	public boolean hasWarnings() throws SQLException { return getSQLWarnings()!=null; }

	public Connection getConnection() { return null; }
	public Statement getStatement() { return null; }
	public int getUpdateCount() throws SQLException { return -1; }
	public ResultSet getResultSet() throws SQLException { return null; }
	public ResultSet getNextRowOfResultSet() { return null; }
	public Vector getVector() { return null; }
	public SQLException getException() { return null; }

	public void closeStatement() throws SQLException { }

	public abstract SQLWarning getSQLWarnings() throws SQLException;
	public abstract void clearSQLWarnings() throws SQLException;


	public String toString() {
		if (isConnection()) return LocalizedResource.getMessage("IJ_Con0",getConnection().toString());
		if (isStatement()) return LocalizedResource.getMessage("IJ_Stm0",getStatement().toString());
		if (isNextRowOfResultSet()) return LocalizedResource.getMessage("IJ_Row0",getNextRowOfResultSet().toString());
		if (isVector()) return LocalizedResource.getMessage("IJ_Vec0",getVector().toString());
		if (isMulti()) return LocalizedResource.getMessage("IJ_Mul0",getVector().toString());
		if (isException()) return LocalizedResource.getMessage("IJ_Exc0",getException().toString());
		return LocalizedResource.getMessage("IJ_Unkn0",this.getClass().getName());
	}
}
