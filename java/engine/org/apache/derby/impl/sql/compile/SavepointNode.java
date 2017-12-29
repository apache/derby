/*

   Derby - Class org.apache.derby.impl.sql.compile.SavepointNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A SavepointNode is the root of a QueryTree that represents a Savepoint (ROLLBACK savepoint, RELASE savepoint and SAVEPOINT)
 * statement.
 */

class SavepointNode extends DDLStatementNode
{
	private String	savepointName; //name of the savepoint
	private int	savepointStatementType; //Type of savepoint statement ie rollback, release or set savepoint

	/**
     * Constructor for a SavepointNode
	 *
     * @param objectName              The name of the savepoint
     * @param savepointStatementType  Type of savepoint statement ie rollback,
     *                                release or set savepoint
     * @param cm                      The context manager
     *
	 * @exception StandardException		Thrown on error
	 */

    SavepointNode(
            String objectName,
            int savepointStatementType,
            ContextManager cm)
		throws StandardException
	{
        super(cm);
        this.savepointName = objectName;
        this.savepointStatementType = savepointStatementType;

		if (SanityManager.DEBUG)
		{
			if (this.savepointStatementType > 3 || this.savepointStatementType < 1)
			{
				SanityManager.THROWASSERT(
				"Unexpected value for savepointStatementType = " + this.savepointStatementType + ". Expected value between 1-3");
			}
		}
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			String tempString = "savepointName: " + "\n" + savepointName + "\n";
			tempString = tempString + "savepointStatementType: " + "\n" + savepointStatementType + "\n";
			return super.toString() +  tempString;
		}
		else
		{
			return "";
		}
	}

    String statementToString()
	{
		if (savepointStatementType == 1)
			return "SAVEPOINT";
		else if (savepointStatementType == 2)
			return "ROLLBACK WORK TO SAVEPOINT";
		else
			return "RELEASE TO SAVEPOINT";
	}

	/**
	 * Returns whether or not this Statement requires a set/clear savepoint
	 * around its execution.  The following statement "types" do not require them:
	 *		Cursor	- unnecessary and won't work in a read only environment
	 *		Xact	- savepoint will get blown away underneath us during commit/rollback
	 *
	 * @return boolean	Whether or not this Statement requires a set/clear savepoint
	 */
    @Override
	public boolean needsSavepoint()
	{
		return false;
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		return(
            getGenericConstantActionFactory().getSavepointConstantAction(
                savepointName,
                savepointStatementType));
	}
}
