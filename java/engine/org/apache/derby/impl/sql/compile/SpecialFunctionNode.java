/*

   Derby - Class org.apache.derby.impl.sql.compile.SpecialFunctionNode

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

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.StringDataValue;

import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.compiler.LocalField;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.impl.sql.compile.ExpressionClassBuilder;

import java.lang.reflect.Modifier;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.sql.compile.C_NodeTypes;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import java.sql.Types;

import java.util.Vector;

/**
     SpecialFunctionNode handles system SQL functions.
	 A function value is either obtained by a method
	 call off the LanguageConnectionContext or Activation.
	 LanguageConnectionContext functions are state related to the connection.
	 Activation functions are those related to the statement execution.

     Each SQL function takes no arguments and returns a SQLvalue.
	 <P>
	 Functions supported:
	 <UL>
	 <LI> USER
	 <LI> CURRENT_USER
	 <LI> CURRENT_ROLE
	 <LI> SESSION_USER
	 <LI> SYSTEM_USER
	 <LI> CURRENT SCHEMA
	 <LI> CURRENT ISOLATION
	 <LI> IDENTITY_VAL_LOCAL

	 </UL>


	<P>

	 This node is used rather than some use of MethodCallNode for
	 runtime performance. MethodCallNode does not provide a fast access
	 to the current language connection or activatation, since it is geared
	 towards user defined routines.


*/
public class SpecialFunctionNode extends ValueNode 
{
	/**
		Name of SQL function
	*/
	String sqlName;

	/**
		Java method name
	*/
	private String methodName;

	/**
		Return type of Java method.
	*/
	private String methodType;

	/**
	*/
	//private boolean isActivationCall;

	/**
	 * Binding this special function means setting the result DataTypeServices.
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
	{		DataTypeDescriptor dtd;
		int nodeType = getNodeType();
		switch (nodeType)
		{
		case C_NodeTypes.USER_NODE:
		case C_NodeTypes.CURRENT_USER_NODE:
		case C_NodeTypes.SESSION_USER_NODE:
		case C_NodeTypes.SYSTEM_USER_NODE:
			switch (nodeType)
			{
				case C_NodeTypes.USER_NODE: sqlName = "USER"; break;
				case C_NodeTypes.CURRENT_USER_NODE: sqlName = "CURRENT_USER"; break;
				case C_NodeTypes.SESSION_USER_NODE: sqlName = "SESSION_USER"; break;
				case C_NodeTypes.SYSTEM_USER_NODE: sqlName = "SYSTEM_USER"; break;
			}
			methodName = "getAuthorizationId";
			methodType = "java.lang.String";
            
			//SQL spec Section 6.4 Syntax Rule 4 says that the collation type 
			//of these functions will be the collation of character set 
			//SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
			//these functions will be UCS_BASIC. The collation derivation will 
			//be implicit. 
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
			break;

		case C_NodeTypes.CURRENT_SCHEMA_NODE:
			sqlName = "CURRENT SCHEMA";
			methodName = "getCurrentSchemaName";
			methodType = "java.lang.String";
			
			//This is a Derby specific function but its collation type will
			//be based on the same rules as for SESSION_USER/CURRENT_USER etc. 
			//ie there collation type will be UCS_BASIC. The collation 
			//derivation will be implicit. 
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
			break;

		case C_NodeTypes.CURRENT_ROLE_NODE:
			sqlName = "CURRENT_ROLE";
			methodName = "getCurrentRoleId";
			methodType = "java.lang.String";
			dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				Types.VARCHAR, false, 128);
			//SQL spec Section 6.4 Syntax Rule 4 says that the collation type
			//of these functions will be the collation of character set
			//SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
			//these functions will be UCS_BASIC. The collation derivation will
			//be implicit. (set by default)
			break;

		case C_NodeTypes.IDENTITY_VAL_NODE:
			sqlName = "IDENTITY_VAL_LOCAL";
			methodName = "getIdentityValue";
			methodType = "java.lang.Long";
			dtd = DataTypeDescriptor.getSQLDataTypeDescriptor("java.math.BigDecimal", 31, 0, true, 31);
			break;

		case C_NodeTypes.CURRENT_ISOLATION_NODE:
			sqlName = "CURRENT ISOLATION";
			methodName = "getCurrentIsolationLevelStr";
			methodType = "java.lang.String";
			dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 2);
			//This is a Derby specific function but it's collation type will
			//be based on the same rules as for SESSION_USER/CURRENT_USER etc. 
			//ie there collation type will be UCS_BASIC. The collation 
			//derivation will be implicit. (set by default).
			break;
		default:
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Invalid type for SpecialFunctionNode " + nodeType);
			}
			dtd = null;
			break;
		}

		checkReliability(sqlName, CompilerContext.USER_ILLEGAL );
		setType(dtd);

		return this;
	}

	/**
	 * Return the variant type for the underlying expression.
	   All supported special functions are QUERY_INVARIANT

	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType()
	{
		return Qualifier.QUERY_INVARIANT;
	}

	/**
		Generate an expression that returns a DataValueDescriptor and
		calls a method off the language connection or the activation.
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
		int argCount = 0;

		if (methodName.equals("getCurrentRoleId")) {
			acb.pushThisAsActivation(mb);
			argCount++;
		}

		mb.callMethod(VMOpcode.INVOKEINTERFACE,
					  (String) null, methodName, methodType, argCount);

		String fieldType = getTypeCompiler().interfaceName();
		LocalField field = acb.newFieldDeclaration(Modifier.PRIVATE, fieldType);

		acb.generateDataValue(mb, getTypeCompiler(), 
				getTypeServices().getCollationType(), field);
	}

	/*
		print the non-node subfields
	 */
	public String toString() {
		if (SanityManager.DEBUG)
		{
			return super.toString()+ sqlName;
		}
		else
		{
			return "";
		}
	}
        
	protected boolean isEquivalent(ValueNode o)
	{
		if (isSameNodeType(o))
		{
			SpecialFunctionNode other = (SpecialFunctionNode)o;
			return methodName.equals(other.methodName);
		}
		return false;
	}
}
