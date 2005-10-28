/*

   Derby - Class org.apache.derby.iapi.sql.execute.TemporaryRowHolder

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * This is a class that is used to temporarily
 * (non-persistently) hold rows that are used in
 * language execution.  It will store them in an
 * array, or a temporary conglomerate, depending
 * on the number of rows.  
 * <p>
 * It is used for deferred DML processing.
 *
 * @author jamie
 */
public interface TemporaryRowHolder
{
	/**
	 * Insert a row
	 *
	 * @param row the row to insert 
	 *
	 * @exception StandardException on error
 	 */
	public void insert(ExecRow inputRow)
		throws StandardException;

	/**
	 * Get a result set for scanning what has been inserted
 	 * so far.
	 *
	 * @return a result set to use
	 */
	public CursorResultSet getResultSet();

	/**
	 * Clean up
	 *
	 * @exception StandardException on error
	 */
	public void close() throws StandardException;


	//returns the conglomerate number it created
	public long getTemporaryConglomId();

	//return the conglom id of the position index it maintains
	public long getPositionIndexConglomId();

	//sets the type of the temporary row holder to unique stream
	public void setRowHolderTypeToUniqueStream();

}
