/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */


package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A TransactionStatementNode represents any type of Transaction statement: 
 * SET TRANSACTION, COMMIT, and ROLLBACK.
 *
 * @author Ames Carlson
 */

public abstract class TransactionStatementNode extends StatementNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	int activationKind()
	{
		   return StatementNode.NEED_NOTHING_ACTIVATION;
	}
	/**
	 * COMMIT and ROLLBACK are allowed to commit
	 * and rollback, duh.
	 *
	 * @return false 
	 */	
	public boolean isAtomic() 
	{
		return false;
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
}
