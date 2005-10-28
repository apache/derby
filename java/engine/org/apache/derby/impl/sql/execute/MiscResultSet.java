/*

   Derby - Class org.apache.derby.impl.sql.execute.MiscResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.Activation;


/**
 *	This is a wrapper class which invokes the Execution-time logic for
 *	Misc statements. The real Execution-time logic lives inside the
 *	executeConstantAction() method of the Execution constant.
 *
 *	@author jamie
 */

class MiscResultSet extends NoRowsResultSetImpl
{
	private final ConstantAction constantAction;

	/**
     * Construct a MiscResultSet
	 *
	 *  @param activation		Describes run-time environment.
	 *
	 *  @exception StandardException Standard Cloudscape error policy.
     */
    MiscResultSet(Activation activation)
		 throws StandardException
    {
		super(activation);
		constantAction = activation.getConstantAction();
	}
    
	public void open() throws StandardException
	{
		constantAction.executeConstantAction(activation);
		super.close();
	}

	/**
	 * @see org.apache.derby.iapi.sql.ResultSet#cleanUp
	 */
	public void	cleanUp() 
	{
	}
}
