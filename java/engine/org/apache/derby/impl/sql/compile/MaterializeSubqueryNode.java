/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 2002, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
