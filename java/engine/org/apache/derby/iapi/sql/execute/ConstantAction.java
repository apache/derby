/*

   Derby - Class org.apache.derby.iapi.sql.execute.ConstantAction

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.catalog.UUID;

/**
 *	This interface describes actions that are ALWAYS performed for a
 *	Statement at Execution time. For instance, it is used for DDL
 *	statements to describe what they should stuff into the catalogs.
 *	<p>
 *	An object satisfying this interface is put into the PreparedStatement
 *	and run at Execution time.
 *
 *	@author Rick Hillegas
 */

public interface ConstantAction
{

	/* Types of Statistics commands */
	public static final int STATISTICSTIMING = 1;
	public static final int RUNTIMESTATISTICS = 2;

	/**
	 *	Run the ConstantAction.
	 *
	 * @param	activation	The execution environment for this constant action.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException;


	/**
	 * Does this constant action modify the passed in table
	 * uuid?  By modify we mean add or drop things tied to
	 * this table (e.g. index, trigger, constraint).  Things
	 * like views or spses that reference this table don't
	 * count.
	 *
	 * @param tableId the other table id
	 * 
	 * @exception StandardException on error
	 */
	public boolean modifiesTableId(UUID tableId) throws StandardException;

    /**
	  *	Reports whether these constants are up-to-date. This returns true
	  *	for homogenous Cloudscape/Cloudsync. For the Plugin, this may
	  *	return false;
	  *
	  *	@return	true if these constants are up-to-date
	  *			false otherwise
	  */
	public	boolean	upToDate()  throws StandardException;
}
