/*

   Derby - Class org.apache.derby.impl.sql.compile.MiscellaneousStatementNode

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.compiler.MethodBuilder;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

/**
 * A MiscellaneousStatement represents any type of statement that doesn't
 * fit into the well defined categores: 
 * SET (non-transaction).
 *
 * @author Jerry Brenner
 */

public abstract class MiscellaneousStatementNode extends StatementNode
{

	int activationKind()
	{
		   return StatementNode.NEED_NOTHING_ACTIVATION;
	}

	/**
	 * Generic generate code for all Misc statements
	 * that need activations.
	 *
	 * @param acb	The ActivationClassBuilder for the class being built
	 * @param mb the method  for the execute() method to be built
	 *
	 * @return		A compiled expression returning the RepCreatePublicationResultSet
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generate(ActivationClassBuilder acb,
								MethodBuilder mb)
							throws StandardException
	{
		// The generated java is the expression:
		// return ResultSetFactory.getMiscResultSet(this )

		acb.pushGetResultSetFactoryExpression(mb);

		acb.pushThisAsActivation(mb); // first arg

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getMiscResultSet",
						ClassName.ResultSet, 1);
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
