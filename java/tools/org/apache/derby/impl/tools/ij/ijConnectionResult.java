/*

   Derby - Class org.apache.derby.impl.tools.ij.ijConnectionResult

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
import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * @author ames
 */
class ijConnectionResult extends ijResultImpl {

	Connection conn;

	ijConnectionResult(Connection c) {
		conn = c;
	}

	public boolean isConnection() { return true; }

	public Connection getConnection() { return conn; }

	public SQLWarning getSQLWarnings() throws SQLException { return conn.getWarnings(); }
	public void clearSQLWarnings() throws SQLException { conn.clearWarnings(); }
}
