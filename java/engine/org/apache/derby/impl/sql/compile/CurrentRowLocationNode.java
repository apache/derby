/*

   Derby - Class org.apache.derby.impl.sql.compile.CurrentRowLocationNode

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

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.RefDataValue;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;

import org.apache.derby.iapi.services.compiler.ClassBuilder;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;


import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.lang.reflect.Modifier;
import org.apache.derby.iapi.reference.ClassName;

import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.catalog.TypeDescriptor;

import java.util.Vector;

/**
 * The CurrentRowLocation operator is used by DELETE and UPDATE to get the
 * RowLocation of the current row for the target table.  The bind() operations
 * for DELETE and UPDATE add a column to the target list of the SelectNode
 * that represents the ResultSet to be deleted or updated.
 */

public class CurrentRowLocationNode extends ValueNode
{
	/**
	 * Binding this expression means setting the result DataTypeServices.
	 * In this case, the result type is always the same.
	 *
	 * @param fromList			The FROM list for the statement.  This parameter
	 *							is not used in this case.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */

	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
							Vector aggregateVector)
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
	 *		public DataValueDescriptor exprx()
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
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mbex)
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


		acb.generateDataValue(mb, getTypeCompiler(), field);

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
}
