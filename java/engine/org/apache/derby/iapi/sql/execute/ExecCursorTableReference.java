/*

   Derby - Class org.apache.derby.iapi.sql.execute.ExecCursorTableReference

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

/**
 * This is a table name reference that can be retrieved from
 * an active cursor.  
 *
 * @author jamie
 */
public interface ExecCursorTableReference
{
	/**
	 * Return the base name of the table
 	 *
	 * @return the base name
	 */
	String getBaseName();

	/**
	 * Return the exposed name of the table.  Exposed
	 * name is another term for correlation name.  If
	 * there is no correlation, this will return the base
	 * name.
 	 *
	 * @return the base name
	 */
	String getExposedName();


	/**
	 * Return the schema for the table.  
	 *
	 * @return the schema name
	 */
	String getSchemaName();
}
