/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.util.ReuseFactory;


import java.sql.Types;

public final class VarbitConstantNode extends BitConstantNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	 * Initializer for a VarbitConstantNode.
	 *
	 * @param arg1  The TypeId for the type of the node
	 *
	 * - OR -
	 *
	 * @param arg1	A Bit containing the value of the constant
	 *
	 * @exception StandardException
	 */

	public void init(
						Object arg1)
		throws StandardException
	{
		init(
					arg1,
					Boolean.TRUE,
					ReuseFactory.getInteger(0));

	}
}
