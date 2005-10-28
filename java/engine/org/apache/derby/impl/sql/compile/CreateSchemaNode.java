/*

   Derby - Class org.apache.derby.impl.sql.compile.CreateSchemaNode

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


import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.impl.sql.execute.ColumnInfo;

import java.util.Properties;

/**
 * A CreateSchemaNode is the root of a QueryTree that 
 * represents a CREATE SCHEMA statement.
 *
 * @author jamie
 */

public class CreateSchemaNode extends DDLStatementNode
{
	private String 	name;
	private String	aid;
	
	/**
	 * Initializer for a CreateSchemaNode
	 *
	 * @param schemaName	The name of the new schema
	 * @param aid		 	The authorization id
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
			Object	schemaName,
			Object	aid)
		throws StandardException
	{
		/*
		** DDLStatementNode expects tables, null out
		** objectName explicitly to clarify that we
		** can't hang with schema.object specifiers.
		*/
		initAndCheck(null);	
	
		this.name = (String) schemaName;
		this.aid = (String) aid;
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
				"schemaName: " + "\n" + name + "\n" +
				"authorizationId: " + "\n" + aid + "\n";
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "CREATE SCHEMA";
	}

	// We inherit the generate() method from DDLStatementNode.

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction()
	{
		return	getGenericConstantActionFactory().getCreateSchemaConstantAction(name, aid);
	}
}
