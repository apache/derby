/*

   Derby - Class org.apache.derby.impl.sql.compile.MaterializeSubqueryNode

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

/**
 * A MaterializeSubqueryNode is used to replace the nodes for a subquery, to facilitate
 * code generation for materialization if possible.  See beetle 4373 for details.
 *
 * @author Tingjian Ge
 */
class MaterializeSubqueryNode extends ResultSetNode
{

	private LocalField lf;

	public MaterializeSubqueryNode(LocalField lf)
	{
		this.lf = lf;
	}

	public void generate(ActivationClassBuilder acb,
						 MethodBuilder mb)
		throws StandardException
	{
		acb.pushThisAsActivation(mb);
		mb.getField(lf);
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "materializeResultSetIfPossible", ClassName.NoPutResultSet, 1);
	}

	void decrementLevel(int decrement)
	{
	}
}
