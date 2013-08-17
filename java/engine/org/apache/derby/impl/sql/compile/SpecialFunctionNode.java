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

import java.lang.reflect.Modifier;
import java.sql.Types;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.LocalField;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.DataTypeDescriptor;

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
     to the current language connection or activation, since it is geared
	 towards user defined routines.


*/
class SpecialFunctionNode extends ValueNode
{
	/**
		Name of SQL function
	*/
	String sqlName;

    // Allowed kinds
    final static int K_IDENTITY_VAL = 0;
    final static int K_CURRENT_ISOLATION = 1;
    final static int K_CURRENT_SCHEMA = 2;
    final static int K_USER = 3;
    final static int K_CURRENT_USER = 4;
    final static int K_SESSION_USER = 5;
    final static int K_SYSTEM_USER = 6; // currently not in use
    final static int K_CURRENT_ROLE = 7;

    /**
     * This class is used to hold logically different objects for
     * space efficiency. {@code kind} represents the logical object
     * type. See also {@link ValueNode#isSameNodeKind}.
     */
    final int kind;

	/**
		Java method name
	*/
	private String methodName;

	/**
		Return type of Java method.
	*/
	private String methodType;

    SpecialFunctionNode(int kind, ContextManager cm) {
        super(cm);
        this.kind = kind;

        if (SanityManager.DEBUG) {
            if (kind == K_SYSTEM_USER) {
                SanityManager.THROWASSERT("SYSTEM_USER not expected");
            }
        }
    }
	/**
	 * Binding this special function means setting the result DataTypeServices.
	 * In this case, the result type is based on the operation requested.
	 *
	 * @param fromList			The FROM list for the statement.  This parameter
	 *							is not used in this case.
	 * @param subqueryList		The subquery list being built as we find 
	 *							SubqueryNodes. Not used in this case.
     * @param aggregates        The aggregate list being built as we find
	 *							AggregateNodes. Not used in this case.
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error
	 */
    @Override
    ValueNode bindExpression(FromList fromList,
                             SubqueryList subqueryList,
                             List<AggregateNode> aggregates)
            throws StandardException
    {
        DataTypeDescriptor dtd;

        switch (kind) {
        case K_USER:
        case K_CURRENT_USER:
        case K_SYSTEM_USER:
            switch (kind) {
                case K_USER:
                    sqlName = "USER";
                    break;
                case K_CURRENT_USER:
                    sqlName = "CURRENT_USER";
                    break;
                case K_SYSTEM_USER:
                    sqlName = "SYSTEM_USER";
                    break;
			}
            methodName = "getCurrentUserId";
			methodType = "java.lang.String";
            
            // SQL spec Section 6.4 Syntax Rule 4 says that the collation type
            // of these functions will be the collation of character set
            // SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
            // these functions will be UCS_BASIC. The collation derivation will
			//be implicit. 
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
			break;

        case K_SESSION_USER:
            methodName = "getSessionUserId";
            methodType = "java.lang.String";
            sqlName = "SESSION_USER";
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
            break;

        case K_CURRENT_SCHEMA:
			sqlName = "CURRENT SCHEMA";
			methodName = "getCurrentSchemaName";
			methodType = "java.lang.String";
			
			//This is a Derby specific function but its collation type will
			//be based on the same rules as for SESSION_USER/CURRENT_USER etc. 
			//ie there collation type will be UCS_BASIC. The collation 
			//derivation will be implicit. 
            dtd = DataDictionary.TYPE_SYSTEM_IDENTIFIER;
			break;

        case K_CURRENT_ROLE:
			sqlName = "CURRENT_ROLE";
			methodName = "getCurrentRoleIdDelimited";
			methodType = "java.lang.String";
			dtd = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
				// size: 2+(2*128) start and end text quote plus max # of
				// escapes
				Types.VARCHAR, true, 2+(2*128)); 
			//SQL spec Section 6.4 Syntax Rule 4 says that the collation type
			//of these functions will be the collation of character set
			//SQL_IDENTIFIER. In Derby's case, that will mean, the collation of
			//these functions will be UCS_BASIC. The collation derivation will
			//be implicit. (set by default)
			break;

        case K_IDENTITY_VAL:
			sqlName = "IDENTITY_VAL_LOCAL";
			methodName = "getIdentityValue";
			methodType = "java.lang.Long";
			dtd = DataTypeDescriptor.getSQLDataTypeDescriptor("java.math.BigDecimal", 31, 0, true, 31);
			break;

        case K_CURRENT_ISOLATION:
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
            if (SanityManager.DEBUG) {
                SanityManager.THROWASSERT(
                        "Invalid type for SpecialFunctionNode " + kind);
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
    @Override
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
    @Override
    void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
									throws StandardException
	{
		mb.pushThis();
		mb.callMethod(VMOpcode.INVOKEINTERFACE, ClassName.Activation, "getLanguageConnectionContext",
											 ClassName.LanguageConnectionContext, 0);
		int argCount = 0;

		if (methodName.equals("getCurrentRoleIdDelimited") ||
                methodName.equals("getCurrentSchemaName") ||
                methodName.equals("getCurrentUserId")) {

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
    @Override
	public String toString() {
		if (SanityManager.DEBUG)
		{
			return "sqlName: " + sqlName + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

    @Override
    boolean isSameNodeKind(ValueNode o) {
        return super.isSameNodeKind(o) &&
                ((SpecialFunctionNode)o).kind == this.kind;
    }
        
    boolean isEquivalent(ValueNode o)
	{
        if (isSameNodeKind(o)) {
			SpecialFunctionNode other = (SpecialFunctionNode)o;
			return methodName.equals(other.methodName);
		}

		return false;
	}
}
