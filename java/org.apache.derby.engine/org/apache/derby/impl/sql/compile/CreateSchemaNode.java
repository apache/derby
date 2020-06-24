/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateSchemaNode

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

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.shared.common.sanity.SanityManager;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.execute.ConstantAction;

/**
 * A CreateSchemaNode is the root of a QueryTree that 
 * represents a CREATE SCHEMA statement.
 *
 */

//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
class CreateSchemaNode extends DDLStatementNode
{
	private String 	name;
	private String	aid;
	
	/**
     * Constructor for a CreateSchemaNode
	 *
	 * @param schemaName	The name of the new schema
	 * @param aid		 	The authorization id
     * @param cm            The context manager
	 *
	 * @exception StandardException		Thrown on error
	 */
    CreateSchemaNode(
            String schemaName,
            String aid,
            ContextManager cm) throws StandardException
	{
		/*
		** DDLStatementNode expects tables, null out
		** objectName explicitly to clarify that we
		** can't hang with schema.object specifiers.
		*/
        super(null, cm);
        this.name = schemaName;
        this.aid = aid;
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */
    @Override
	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				"schemaName: " + "\n" + name + "\n" +
				"authorizationId: " + "\n" + aid + "\n";
		}
		else
		{
			return "";
		}
	}

	/**
	 * Bind this createSchemaNode. Main work is to create a StatementPermission
	 * object to require CREATE_SCHEMA_PRIV at execution time.
	 */
    @Override
	public void bindStatement() throws StandardException
	{
		CompilerContext cc = getCompilerContext();
//IC see: https://issues.apache.org/jira/browse/DERBY-1330
		if (isPrivilegeCollectionRequired())
			cc.addRequiredSchemaPriv(name, aid, Authorizer.CREATE_SCHEMA_PRIV);

	}
	
//IC see: https://issues.apache.org/jira/browse/DERBY-673
//IC see: https://issues.apache.org/jira/browse/DERBY-5973
    String statementToString()
	{
		return "CREATE SCHEMA";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
    @Override
    public ConstantAction makeConstantAction()
	{
		return	getGenericConstantActionFactory().getCreateSchemaConstantAction(name, aid);
	}
}
