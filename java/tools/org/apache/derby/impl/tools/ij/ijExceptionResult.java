/*

   Derby - Class org.apache.derby.impl.tools.ij.ijExceptionResult

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

import java.sql.SQLException;
import java.sql.SQLWarning;

/**
 * This is an impl for just returning errors from
 * JDBC statements. Used by Async to capture its result
 * for WaitFor.
 *
 * @author ames
 */
class ijExceptionResult extends ijResultImpl {

	SQLException except;

	ijExceptionResult(SQLException e) {
		except = e;
	}

	public boolean isException() { return true; }
	public SQLException getException() { return except; }

	public SQLWarning getSQLWarnings() { return null; }
	public void clearSQLWarnings() { }
}
