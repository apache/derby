/*

   Derby - Class org.apache.derby.impl.sql.compile.DropSPSNode

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
import org.apache.derby.iapi.services.compiler.JavaFactory;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.SPSDescriptor;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A DropSPSNode is the root of a QueryTree that represents a DROP STATEMENT
 * statement.
 *
 * @author Jamie
 */

public class DropSPSNode extends DropStatementNode
{
	public String statementToString()
	{
		return "DROP STATEMENT";
	}

	// inherit generate() method from DDLStatementNode
	/**
	 * Bind this DropSPSNode.  
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		super.bind();
		
		DataDictionary			dd = getDataDictionary();
		SchemaDescriptor sd = getSchemaDescriptor();

		SPSDescriptor spsd = null;
		
		if (sd.getUUID() != null)
			spsd = dd.getSPSDescriptor(getRelativeName(), sd);

		if (spsd == null)
		{
			throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, "DROP STATEMENT", getFullName());
		}
		
		if (spsd.getType() == spsd.SPS_TYPE_TRIGGER)
		{
			throw StandardException.newException(SQLState.LANG_CANNOT_DROP_TRIGGER_S_P_S, getFullName());
		}

		return this;
	}

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropSPSConstantAction(
										 	getSchemaDescriptor(),
											getRelativeName());
	}
}
