/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.types
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import org.apache.derby.iapi.services.io.StreamStorable;

/*
 * The BitDataValue interface corresponds to a SQL BIT 
 */

public interface BitDataValue extends ConcatableDataValue, StreamStorable
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;
	/**
	 * The SQL concatenation '||' operator.
	 *
	 * @param leftOperand	String on the left hand side of '||'
	 * @param rightOperand	String on the right hand side of '||'
	 * @param result	The result of a previous call to this method,
	 *					null if not called yet.
	 *
	 * @return	A ConcatableDataValue containing the result of the '||'
	 *
	 * @exception StandardException		Thrown on error
	 */
	public BitDataValue concatenate(
				BitDataValue leftOperand,
				BitDataValue rightOperand,
				BitDataValue result)
		throws StandardException;

}
