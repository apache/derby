/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.TypeId;


import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.util.ReuseFactory;

import java.sql.Types;

public final class LOBConstantNode extends ConstantNode
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2002_2004;
	/**
	 * Initializer for a LOBConstantNode.
	 *
	 * @param arg1	The TypeId for the type of the node, or a string?
	 *
	 * - OR *
	 *
	 * @param arg1	A String containing the value of the constant
	 * @param arg2	The factory to get the TypeId
	 *			and DataTypeServices factories from.
	 *
	 * @exception StandardException
	 */
	public void init(Object arg1)
		throws StandardException
	{
		if (arg1 instanceof TypeId)
		{
			super.init(
						arg1,
						Boolean.TRUE,
						ReuseFactory.getInteger(0));
		}
		else
		{
			String val = (String) arg1;

			super.init(
				TypeId.CHAR_ID,
				(val == null) ? Boolean.TRUE : Boolean.FALSE,
				(val != null) ?
					ReuseFactory.getInteger(val.length()) :
					ReuseFactory.getInteger(0));

			setValue(getDataValueFactory().getCharDataValue(val));
		}
	}

	public void init(
					Object newValue, 
					Object newLength)
		throws StandardException
	{
		String val = (String) newValue;
		int newLen = ((Integer) newLength).intValue();

		super.init(
			 TypeId.CHAR_ID,
			 (val == null) ? Boolean.TRUE : Boolean.FALSE,
			 newLength);

		if (val.length() > newLen)
		{
			throw StandardException.newException(SQLState.LANG_STRING_TRUNCATION, "CHAR", val, String.valueOf(newLen));
		}

		// Blank pad the string if necessesary
		while (val.length() < newLen)
		{
			val = val + ' ';
		}

		setValue(getDataValueFactory().getCharDataValue(val));
	}

	/**
	 * Return the value from this LOBConstantNode
	 *
	 * @return	The value of this LOBConstantNode.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public String	getString() throws StandardException
	{
		return value.getString();
	}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */

	//public int	getLength() throws StandardException
	//{
	//	return value.getLength();
	//}

	/**
	 * Return an Object representing the bind time value of this
	 * expression tree.  If the expression tree does not evaluate to
	 * a constant at bind time then we return null.
	 * This is useful for bind time resolution of VTIs.
	 * RESOLVE: What do we do for primitives?
	 *
	 * @return	An Object representing the bind time value of this expression tree.
	 *			(null if not a bind time constant.)
	 *
	 * @exception StandardException		Thrown on error
	 */
	Object getConstantValueAsObject() throws StandardException 
	{
		return value.getString();
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @return		The compiled Expression, 
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb) throws StandardException
	{
		// The generated java is the expression:
		// "#getString()"
		mb.push(getString());
	}
}
