/*

   Derby - Class org.apache.derby.impl.sql.compile.VarbitConstantNode

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
