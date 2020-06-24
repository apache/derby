/*

   Derby - Class org.apache.derby.impl.sql.compile.CurrentRowLocationNode

//IC see: https://issues.apache.org/jira/browse/DERBY-1377
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

import java.lang.reflect.Modifier;
import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

/**
 * The CurrentRowLocation operator is used by DELETE and UPDATE to get the
 * RowLocation of the current row for the target table.  The bind() operations
 * for DELETE and UPDATE add a column to the target list of the SelectNode
 * that represents the ResultSet to be deleted or updated.
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class CurrentRowLocationNode extends ValueNode
{

    CurrentRowLocationNode(ContextManager cm) {
        super(cm);
    }

	/**
	 * Binding this expression means setting the result DataTypeServices.
	 * In this case, the result type is always the same.
	 *
	 * @param fromList			The FROM list for the statement.  This parameter
	 *							is not used in this case.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
     * @param aggregates        The aggregate list being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    ValueNode bindExpression(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates)
            throws StandardException
	{
		setType(new DataTypeDescriptor(TypeId.getBuiltInTypeId(TypeId.REF_NAME),
						false		/* Not nullable */
					)
				);
		return this;
	}

	/**
	 * CurrentRowLocationNode is used in updates and deletes.  See generate() in
	 * UpdateNode and DeleteNode to get the full overview of generate().  This
	 * class is responsible for generating the method that will return the RowLocation
	 * for the next row to be updated or deleted.
	 *
	 * This routine will generate a method of the form:
	 *
	 *		private SQLRef	fieldx;
	 *
	 *		...
	 *
     *      protected DataValueDescriptor exprx()
	 *				throws StandardException
	 *		{
	 *			return fieldx = <SQLRefConstructor>(
	 *									"result set member".getRowLocation(),
	 *									fieldx);
	 *		}
	 * and return the generated code:
	 *    exprx()
	 *
	 * ("result set member" is a member of the generated class added by UpdateNode or
	 * DeleteNode.)
	 * This exprx function is used within another exprx function,
	 * and so doesn't need a static field or to be public; but
	 * at present, it has both. 
	 *
	 * fieldx is a generated field that is initialized to null when the
	 * activation is constructed.  getSQLRef will re-use fieldx on calls
	 * after the first call, rather than allocate a new SQLRef for each call.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mbex)
									throws StandardException
	{
		/* Generate a new method */
		/* only used within the other exprFuns, so can be private */
		MethodBuilder mb = acb.newGeneratedFun(ClassName.DataValueDescriptor, Modifier.PROTECTED);
		
		/* Allocate an object for re-use to hold the result of the operator */
		LocalField field =
			acb.newFieldDeclaration(Modifier.PRIVATE, ClassName.RefDataValue);


		/* Fill in the body of the method
		 * generates:
		 *    return TypeFactory.getSQLRef(this.ROWLOCATIONSCANRESULTSET.getRowLocation());
		 * and adds it to exprFun
		 */

		mb.pushThis();
		mb.getField((String)null, acb.getRowLocationScanResultSetName(), ClassName.CursorResultSet);
		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getRowLocation", ClassName.RowLocation, 0);


//IC see: https://issues.apache.org/jira/browse/DERBY-2583
		acb.generateDataValue(mb, getTypeCompiler(), 
				getTypeServices().getCollationType(), field);

		/*
		** Store the result of the method call in the field, so we can re-use
		** the object.
		*/
		mb.putField(field);

		/* Stuff the full expression into a return statement and add that to the
		 * body of the new method.
		 */
		mb.methodReturn();

		// complete the method
		mb.complete();

		/* Generate the call to the new method */
		mbex.pushThis();
		mbex.callMethod(VMOpcode.INVOKEVIRTUAL, (String) null, mb.getName(), ClassName.DataValueDescriptor, 0);
	}
	
    boolean isEquivalent(ValueNode o)
	{
		return false;
	}
}
