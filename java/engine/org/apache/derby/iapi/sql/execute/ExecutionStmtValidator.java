/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * An ExecutionStatementValidator is an object that is
 * handed a ConstantAction and asked whether it is ok for
 * this result set to execute.  When something like
 * a trigger is executing, one of these gets pushed.
 * Before execution, each validator that has been pushed
 * is invoked on the result set that we are about to
 * execution.  It is up to the validator to look at
 * the result set and either complain (throw an exception)
 * or let it through.
 *
 * @author jamie
 */
public interface ExecutionStmtValidator
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * Validate the statement.
	 *
	 * @param constantAction The constant action that we are about to execute.  
	 *
	 * @exception StandardException on error
	 *
	 * @see ConstantAction
	 */
	public void validateStatement(ConstantAction constantAction)
		throws StandardException;
}
