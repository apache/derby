/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.error.StandardException;

/**
 * A SavepointNode is the root of a QueryTree that represents a Savepoint (ROLLBACK savepoint, RELASE savepoint and SAVEPOINT)
 * statement.
 */

public class SavepointNode extends DDLStatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;
	private String	savepointName; //name of the savepoint
	private int	savepointStatementType; //Type of savepoint statement ie rollback, release or set savepoint

	/**
	 * Initializer for a SavepointNode
	 *
	 * @param objectName		The name of the savepoint
	 * @param savepointStatementType		Type of savepoint statement ie rollback, release or set savepoint
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void init(
			Object objectName,
			Object savepointStatementType)
		throws StandardException
	{
		initAndCheck(null);	
		this.savepointName = (String) objectName;
		this.savepointStatementType = ((Integer) savepointStatementType).intValue();

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

	public String statementToString()
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
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return(
            getGenericConstantActionFactory().getSavepointConstantAction(
                savepointName,
                savepointStatementType));
	}
}
