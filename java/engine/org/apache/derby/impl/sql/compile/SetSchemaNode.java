/*

   Derby - Class org.apache.derby.impl.sql.compile.SetSchemaNode

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

import org.apache.derby.iapi.reference.ClassName;
import org.apache.derby.iapi.services.classfile.VMOpcode;

import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.compiler.MethodBuilder;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;

import java.util.Vector;


/**
 * A SetSchemaNode is the root of a QueryTree that 
 * represents a SET SCHEMA statement.  It isn't
 * replicated, but it generates a ConstantAction
 * because it is basically easier than generating
 * the code from scratch.
 *
 * @author jamie
 */

public class SetSchemaNode extends MiscellaneousStatementNode
{
	private String 	name;
	private int 	type;
	
	/**
	 * Initializer for a SetSchemaNode
	 *
	 * @param schemaName	The name of the new schema
	 * @param type			Type of schema name could be USER or dynamic parameter
	 *
	 */
	public void init(Object schemaName, Object type)
	{
		this.name = (String) schemaName;
		if (type != null)
			this.type = ((Integer)type).intValue();
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() + 
				(type == StatementType.SET_SCHEMA_USER ? "schemaName: \nUSER\n" :
				(type == StatementType.SET_SCHEMA_DYNAMIC ? "schemaName: \n?\n" : 
					"schemaName: " + "\n" + name + "\n"));
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "SET SCHEMA";
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getSetSchemaConstantAction(name, type);		
	}
	/**
	 * Generate code, need to push parameters
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
		//generate the parameters for the DYNAMIC SET SCHEMA
		if (type == StatementType.SET_SCHEMA_DYNAMIC)
			generateParameterValueSet(acb);

		// The generated java is the expression:
		// return ResultSetFactory.getMiscResultSet(this )

		acb.pushGetResultSetFactoryExpression(mb);

		acb.pushThisAsActivation(mb); // first arg

		mb.callMethod(VMOpcode.INVOKEINTERFACE, (String) null, "getMiscResultSet",
						ClassName.ResultSet, 1);

		if (type == StatementType.SET_SCHEMA_DYNAMIC)
			generateParameterHolders(acb);
	}
	/**
	 * Generate the code to create the ParameterValueSet, if necessary,
	 * when constructing the activation.  Also generate the code to call
	 * a method that will throw an exception if we try to execute without
	 * all the parameters being set.
	 * 
	 * @param acb	The ActivationClassBuilder for the class we're building
	 *
	 * @return	Nothing
	 */

	void generateParameterValueSet(ActivationClassBuilder acb)
		throws StandardException
	{
		Vector parameterList = getCompilerContext().getParameterList();
		// parameter list size should be 1
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(parameterList != null && parameterList.size() == 1);
			
		ParameterNode.generateParameterValueSet ( acb, 1, parameterList);
	}

	/*
	** When all other generation is done for the statement,
	** we need to ensure all of the parameters have been touched.
	*
	*	@param	acb				ActivationClassBuilder
	*
	*/
	void generateParameterHolders(ActivationClassBuilder acb) 
		throws StandardException
	{
		Vector pList = getCompilerContext().getParameterList();
		// we should have already failed if pList doesn't have at least 1 parameter

		ParameterNode.generateParameterHolders( acb,  pList);
			
	}
	/**
	 * Returns the type of activation this class
	 * generates.
	 * 
	 * @return  NEED_PARAM_ACTIVATION or
	 *			NEED_NOTHING_ACTIVATION depending on params
	 *
	 */
	int activationKind()
	{
		Vector parameterList = getCompilerContext().getParameterList();
		/*
		** We need parameters 
		** only for those that have parameters.
		*/
		if (type == StatementType.SET_SCHEMA_DYNAMIC)
		{
			return StatementNode.NEED_PARAM_ACTIVATION;
		}
		else
		{
			return StatementNode.NEED_NOTHING_ACTIVATION;
		}
	}
}
