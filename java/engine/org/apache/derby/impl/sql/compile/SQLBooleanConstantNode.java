/*

   Derby - Class org.apache.derby.impl.sql.compile.SQLBooleanConstantNode

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

package	org.apache.derby.impl.sql.compile;

import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;
import org.apache.derby.iapi.types.BooleanDataValue;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.util.StringUtil;
import java.sql.Types;

public class SQLBooleanConstantNode extends ConstantNode
{
	/**
	 * Initializer for a SQLBooleanConstantNode.
	 *
	 * @param newValue	A String containing the value of the constant: true, false, unknown
	 *
	 * @exception StandardException
	 */

	public void init(
					Object newValue)
		throws StandardException
	{
		String strVal = (String) newValue;
		Boolean val = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT((StringUtil.SQLEqualsIgnoreCase(strVal,"true")) ||
								(StringUtil.SQLEqualsIgnoreCase(strVal,"false")) ||
								(StringUtil.SQLEqualsIgnoreCase(strVal,"unknown")),
								"String \"" + strVal +
								"\" cannot be converted to a SQLBoolean");
		}

		if (StringUtil.SQLEqualsIgnoreCase(strVal,"true"))
			val = Boolean.TRUE;
		else if (StringUtil.SQLEqualsIgnoreCase(strVal,"false"))
			val = Boolean.FALSE;

		/*
		** RESOLVE: The length is fixed at 1, even for nulls.
		** Is that OK?
		*/

		/* Fill in the type information in the parent ValueNode */
		super.init(
			 TypeId.BOOLEAN_ID,
			 Boolean.TRUE,
			 ReuseFactory.getInteger(1));

		if ( val == null )
		{
			setValue(getTypeServices().getNull() );
		}
		else
		{
			setValue(getDataValueFactory().getDataValue(val.booleanValue()));
		}
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		mb.push(value.getBoolean());
	}
}
