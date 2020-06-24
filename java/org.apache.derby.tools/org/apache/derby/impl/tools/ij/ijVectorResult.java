/*

   Derby - Class org.apache.derby.impl.tools.ij.ijVectorResult

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

package org.apache.derby.impl.tools.ij;

import java.util.Vector;
import java.sql.SQLWarning;

/**
 * This is an impl for a simple Vector of objects.
 *
 */
class ijVectorResult extends ijResultImpl {

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
	Vector<Object> vec;
	SQLWarning warns;

	ijVectorResult(Vector<Object> v, SQLWarning w) {
		vec = v;
		warns = w;
	}

	/**
	 * Initialize a new vector containing only one object.
	 */
	ijVectorResult(Object value, SQLWarning w) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		this(new Vector<Object>(1), w);
		vec.add(value);
	}


	public boolean isVector() { return true; }

	public Vector getVector() { return vec; }

	public SQLWarning getSQLWarnings() { return warns; }
	public void clearSQLWarnings() { warns = null; }
}
