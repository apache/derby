/*

   Derby - Class org.apache.derby.impl.sql.compile.SetRoleNode

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

package     org.apache.derby.impl.sql.compile;

import java.util.List;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.StatementType;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A SetRoleNode is the root of a QueryTree that represents a SET ROLE
 * statement.
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class SetRoleNode extends MiscellaneousStatementNode
{
    private String      name;
    private int         type;

    /**
     *
     * @param roleName The name of the new role, null if NONE specified
     * @param type Type of role name could be USER or dynamic parameter
     * @param cm Context manager
     * @throws StandardException
     */
    SetRoleNode(
            String roleName,
            int type,
            ContextManager cm) throws StandardException {
        super(cm);
        this.name = roleName;
        this.type = type;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */
    @Override
    public String toString()
    {
        if (SanityManager.DEBUG) {
            return super.toString() +
                (type == StatementType.SET_ROLE_DYNAMIC ?
                 "roleName: ?\n" :
                 "rolename: " + name + "\n");
        } else {
            return "";
        }
    }

    public String statementToString()
    {
        return "SET ROLE";
    }

    /**
     * Create the Constant information that will drive the guts of
     * Execution.
     *
     * @exception StandardException         Thrown on failure
     */
    @Override
    public ConstantAction   makeConstantAction() throws StandardException
    {
        return getGenericConstantActionFactory().
			getSetRoleConstantAction(name, type);
    }
    /**
     * Override: Generate code, need to push parameters
     *
     * @param acb   The ActivationClassBuilder for the class being built
     * @param mb the method  for the execute() method to be built
     *
     * @exception StandardException         Thrown on error
     */
    @Override
    void generate(ActivationClassBuilder acb, MethodBuilder mb)
            throws StandardException
    {
        //generate the parameters for the DYNAMIC SET ROLE
        if (type == StatementType.SET_ROLE_DYNAMIC) {
            generateParameterValueSet(acb);
        }
        // The generated java is the expression:
        // return ResultSetFactory.getMiscResultSet(this )

        acb.pushGetResultSetFactoryExpression(mb);

        acb.pushThisAsActivation(mb); // first arg

        mb.callMethod(VMOpcode.INVOKEINTERFACE, (String)null,
					  "getMiscResultSet", ClassName.ResultSet, 1);
    }
    /**
     * Generate the code to create the ParameterValueSet, if necessary,
     * when constructing the activation.  Also generate the code to call
     * a method that will throw an exception if we try to execute without
     * all the parameters being set.
     *
     * @param acb   The ActivationClassBuilder for the class we're building
     *
     * @exception StandardException         Thrown on error
     */

    private void generateParameterValueSet(ActivationClassBuilder acb)
        throws StandardException
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
        List<ParameterNode>
            parameterList = getCompilerContext().getParameterList();
        // parameter list size should be 1
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(parameterList != null &&
								 parameterList.size() == 1);
        }
        ParameterNode.generateParameterValueSet (acb, 1, parameterList);
    }

    /**
     * Override: Returns the type of activation this class
     * generates.
     *
     * @return  NEED_PARAM_ACTIVATION or
     *          NEED_NOTHING_ACTIVATION depending on params
     *
     */
    @Override
    int activationKind()
    {
        /*
        ** We need parameters only for those that have parameters.
        */
        if (type == StatementType.SET_ROLE_DYNAMIC) {
            return StatementNode.NEED_PARAM_ACTIVATION;
        } else {
            return StatementNode.NEED_NOTHING_ACTIVATION;
        }
    }


	/**
	 * Override to allow committing of reading SYSROLES,
	 * cf. SetRoleConstantAction's call to userCommit to retain idle
	 * state. If atomic, that commit will fail.
	 *
	 * @return false
	 */
    @Override
	public boolean isAtomic()
	{
		return false;
	}


}
