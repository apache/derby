/*

   Derby - Class org.apache.derby.impl.sql.compile.SetTransactionIsolationNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

/**
 * A SetTransactionIsolationNode is the root of a QueryTree that represents a SET
 * TRANSACTION ISOLATION command
 *
 * @author Jerry Brenner
 */

public class SetTransactionIsolationNode extends TransactionStatementNode
{
	private int		isolationLevel;

	/**
	 * Initializer for SetTransactionIsolationNode
	 *
	 * @param isolationLevel		The new isolation level
	 */
	public void init(Object isolationLevel)
	{
		this.isolationLevel = ((Integer) isolationLevel).intValue();
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
			return "isolationLevel: " + isolationLevel + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "SET TRANSACTION ISOLATION";
	}

	/**
	 * generates a the code.
	 *
	 * @param acb the activation class builder for this statement
	 * @param mb	The method for the method to be built
	 * @exception StandardException thrown if generation fails
	 */
	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		acb.pushGetResultSetFactoryExpression(mb);

		acb.pushThisAsActivation(mb);

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getSetTransactionResultSet", ClassName.ResultSet, 1);
	}


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return getGenericConstantActionFactory().getSetTransactionIsolationConstantAction(isolationLevel);
	}
}
