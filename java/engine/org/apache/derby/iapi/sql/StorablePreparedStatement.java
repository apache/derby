/*

   Derby - Class org.apache.derby.iapi.sql.StorablePreparedStatement

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

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedClass;

/**
 * The Statement interface is an extension of exec prepared statement
 * that has some stored prepared specifics.
 *
 * @author jamie
 */
public interface StorablePreparedStatement extends ExecPreparedStatement
{

	/**
	 * Load up the class from the saved bytes.
	 *
	 *
	 * @exception StandardException on error
	 */
	public void loadGeneratedClass()
		throws StandardException;
}	
