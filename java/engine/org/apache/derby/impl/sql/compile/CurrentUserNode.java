/*

   Derby - Class org.apache.derby.impl.sql.compile.CurrentUserNode

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.lang.reflect.Modifier;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;


import java.sql.Types;

import java.util.Vector;

/**
 * The CurrentUser operator is for the builtin USER, CURRENT_USER,
 * SESSION_USER, CURRENT SCHEMA AND IDENTITY_VAL_LOCAL() operations.
 *
 * @author jerry
 */
public class CurrentUserNode extends ValueNode 
{

	public static final int USER = 0;
	public static final int CURRENT_USER = 1;
	public static final int SESSION_USER = 2;
	public static final int SYSTEM_USER = 3;
	public static final int SCHEMA = 4;
	public static final int IDENTITY_VAL = 5;

	private int whichType;

	public void init(Object whichType) 
	{
		this.whichType = ((Integer) whichType).intValue();

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(((this.whichType >= 0 && this.whichType <= 2) || 
					this.whichType == 4 || this.whichType == 5),
				"whichType expected to be between 0 and 2 or 4 and 5");
		}
	}

	//
	// QueryTreeNode interface
	//

	/**
	 * Binding this expression means setting the result DataTypeServices.
	 * In this case, the result type is based on the operation requested.
	 *
	 * @param fromList			The FROM list for the statement.  This parameter
	 *							is not used in this case.
	 * @param subqueryList		The subquery list being built as we find 
	 *							SubqueryNodes. Not used in this case.
	 * @param aggregateVector	The aggregate vector being built as we find 
	 *							AggregateNodes. Not used in this case.
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ValueNode bindExpression(FromList fromList, SubqueryList subqueryList,
							Vector	aggregateVector)
					throws StandardException
	{
		String errorString = null;
		switch (whichType)
		{
			case USER: 
				errorString = "USER";
				break;

			case CURRENT_USER:
				errorString = "CURRENT_USER";
				break;

			case SESSION_USER:
				errorString = "SESSION_USER";
				break;

			case SYSTEM_USER:
				errorString = "SYSTEM_USER";
				break;

			case SCHEMA:
				errorString = "CURRENT SCHEMA";
				break;

			case IDENTITY_VAL:
				errorString = "IDENTITY_VAL_LOCAL";
				break;
		}

		if (whichType == SCHEMA)
			checkReliability( errorString, CompilerContext.SCHEMA_ILLEGAL );
		else
			checkReliability( errorString, CompilerContext.USER_ILLEGAL );

		if (whichType == IDENTITY_VAL)
			setType(DataTypeDescriptor.getSQLDataTypeDescriptor("java.math.BigDecimal", 31, 0, true, 31));
		else
			setType(DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, false, 128));

		return this;
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType()
	{
		// CurrentDate, Time, Timestamp are invariant for the life of the query
		return Qualifier.QUERY_INVARIANT;
	}

	/**
	 * CurrentDatetimeOperatorNode is used in expressions.
	 * The expression generated for it invokes a static method
	 * on a special Cloudscape type to get the system time and
	 * wrap it in the right java.sql type, and then wrap it
	 * into the right shape for an arbitrary value, i.e. a column
	 * holder. This is very similar to what constants do.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
									throws StandardException
	{
		mb.pushThis();
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Activation, "getLanguageConnectionContext",
											 ClassName.LanguageConnectionContext, 0);
		if (whichType == IDENTITY_VAL)
			mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getIdentityValue", "java.math.BigDecimal", 0);
		else if (whichType == SCHEMA)
			mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getCurrentSchemaName", "java.lang.String", 0);
		else
			mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getAuthorizationId", "java.lang.String", 0);

		String fieldType = getTypeCompiler().interfaceName();
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, fieldType);

		acb.generateDataValue(mb, getTypeCompiler(), field);
	}

	/*
		print the non-node subfields
	 */
	public String toString() {
		if (SanityManager.DEBUG)
		{
			return super.toString()+"whichType = "+whichType+"\n";
		}
		else
		{
			return "";
		}
	}
}
