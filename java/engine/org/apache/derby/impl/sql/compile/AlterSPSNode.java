/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.compile
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
