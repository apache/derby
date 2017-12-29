/*

   Derby - Class org.apache.derby.impl.sql.compile.SetTransactionIsolationNode

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
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A SetTransactionIsolationNode is the root of a QueryTree that represents a SET
 * TRANSACTION ISOLATION command
 *
 */

class SetTransactionIsolationNode extends TransactionStatementNode
{
	private int		isolationLevel;

	/**
     * Constructor for SetTransactionIsolationNode
	 *
	 * @param isolationLevel		The new isolation level
     * @param cm                    The context manager
     */
    SetTransactionIsolationNode(int isolationLevel, ContextManager cm)
	{
        super(cm);
        this.isolationLevel = isolationLevel;
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
			return "isolationLevel: " + isolationLevel + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

    String statementToString()
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
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
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
    @Override
    public ConstantAction makeConstantAction() throws StandardException
	{
		return getGenericConstantActionFactory().getSetTransactionIsolationConstantAction(isolationLevel);
	}
}
