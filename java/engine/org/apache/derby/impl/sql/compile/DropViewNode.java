/*

   Derby - Class org.apache.derby.impl.sql.compile.DropViewNode

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

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A DropViewNode is the root of a QueryTree that represents a DROP VIEW
 * statement.
 *
 * @author Jerry Brenner
 */

public class DropViewNode extends DropStatementNode
{

	/**
	 * Initializer for a DropViewNode
	 *
	 * @param objectName	The name of the object being dropped
	 *
	 */

	public void init(Object dropObjectName)
		throws StandardException
	{
		initAndCheck(dropObjectName);
	}

	public String statementToString()
	{
		return "DROP VIEW";
	}

	// inherit generate() method from DDLStatementNode


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropViewConstantAction( getFullName(),
											 getRelativeName(),
											 getSchemaDescriptor());
	}
}
