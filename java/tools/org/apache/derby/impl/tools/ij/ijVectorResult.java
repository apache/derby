/*

   Derby - Class org.apache.derby.impl.tools.ij.ijVectorResult

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

import java.util.Vector;
import java.sql.SQLWarning;

/**
 * This is an impl for a simple Vector of strings.
 *
 * @author ames
 */
class ijVectorResult extends ijResultImpl {

	Vector vec;
	SQLWarning warns;

	ijVectorResult(Vector v, SQLWarning w) {
		vec = v;
		warns = w;
	}

	public boolean isVector() { return true; }

	public Vector getVector() { return vec; }

	public SQLWarning getSQLWarnings() { return warns; }
	public void clearSQLWarnings() { warns = null; }
}
