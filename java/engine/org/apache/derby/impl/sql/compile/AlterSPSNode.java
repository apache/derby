/*

   Derby - Class org.apache.derby.impl.sql.compile.AlterSPSNode

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

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A AlterSPSNode represents a DDL statement that alters a 
 * stored prepared statement.  Currently, this is only
 * ALTER STATEMENT <name> RECOMPILE [ ALL | INVALID ]
 *
 * @author jamie
 */
public class AlterSPSNode extends DDLStatementNode
{
	private ResultSetNode		usingClause;
	private String				usingText;
	private boolean				invalidOnly;
	
	/**
	 * Initializer for a AlterSPSNode
	 *
	 * @param objectName	The name of the statement
	 *		to alter.  If null do all statements.
	 * @param usingClause		The using clause
	 * @param usingText			The text of the using clause
	 * @param invalidOnly		only recompile invalid spses

	 * @exception StandardException		Thrown on error
	 */
	public void init(Object 		objectName,
							Object	usingClause,
							Object			usingText,
							Object			invalidOnly)
		throws StandardException
	{
		initAndCheck(objectName);

		this.usingClause = (ResultSetNode) usingClause;
		this.usingText = (String) usingText;
		this.invalidOnly = ((Boolean) invalidOnly).booleanValue();
	}

	/**
	 * Bind this alterSPS.  All we do is bind the using
	 * if there is one.
	 * 
	 * @return The bound query tree
	 *
	 * @exception StandardException on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		/*
		** Bind the using clause to find any
		** problem.
		*/
		if (usingClause != null)
		{
			usingClause.bind();
		}

		return this;
	}
	
	public String statementToString()
	{
		return "ALTER STATEMENT";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return (getObjectName() == null)	?
				getGenericConstantActionFactory().getAlterSPSConstantAction(
							(SchemaDescriptor)null, 	
							(String)null,
							usingText,
							invalidOnly
							) :
				getGenericConstantActionFactory().getAlterSPSConstantAction(
							getSchemaDescriptor(),
							getRelativeName(),
							usingText,
							invalidOnly);
	}
}
