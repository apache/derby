/*

   Derby - Class org.apache.derby.impl.sql.execute.MiscResultSet

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;


/**
 * This is a wrapper class which invokes the Execution-time logic for
 * Misc statements. The real Execution-time logic lives inside the
 * executeConstantAction() method. Note that when re-using the
 * language result set tree across executions (DERBY-827) it is not
 * possible to store the ConstantAction as a member variable, because
 * a re-prepare of the statement will invalidate the stored
 * ConstantAction. Re-preparing a statement does not create a new
 * Activation unless the GeneratedClass has changed, so the existing
 * result set tree may survive a re-prepare.
 */

class MiscResultSet extends NoRowsResultSetImpl
{
	/**
     * Construct a MiscResultSet
	 *
	 *  @param activation		Describes run-time environment.
     */
    MiscResultSet(Activation activation)
    {
		super(activation);
	}
    
	/**
	 * Opens a MiscResultSet, executes the Activation's
	 * ConstantAction, and then immediately closes the MiscResultSet.
	 *
	 * @exception StandardException Standard Derby error policy.
	 */
	public void open() throws StandardException
	{
		setup();
		activation.getConstantAction().executeConstantAction(activation);
		close();
	}

	public  void    close() throws StandardException    { close( false ); }
    
	// Does not override finish() (no action required)

	/**
	 * No action is required, but not implemented in any base class
	 * @see org.apache.derby.iapi.sql.ResultSet#cleanUp
	 */
	public void	cleanUp() 
	{
	}
}
