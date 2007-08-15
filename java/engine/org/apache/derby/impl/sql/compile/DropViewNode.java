/*

   Derby - Class org.apache.derby.impl.sql.compile.DropViewNode

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

import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A DropViewNode is the root of a QueryTree that represents a DROP VIEW
 * statement.
 *
 */

public class DropViewNode extends DDLStatementNode
{

	/**
	 * Initializer for a DropViewNode
	 *
	 * @param dropObjectName	The name of the object being dropped
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

 	/**
 	 *  Bind the drop view node
 	 *
 	 *
 	 * @exception StandardException		Thrown on error
 	 */
	
	public void bindStatement() throws StandardException
	{
		DataDictionary dd = getDataDictionary();
		CompilerContext cc = getCompilerContext();
				
		TableDescriptor td = dd.getTableDescriptor(getRelativeName(), 
					getSchemaDescriptor(),
                    getLanguageConnectionContext().getTransactionCompile());
	
		/* 
		 * Statement is dependent on the TableDescriptor 
		 * If td is null, let execution throw the error like
		 * it is before.
		 */
		if (td != null)
		{
			cc.createDependency(td);
		}
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
