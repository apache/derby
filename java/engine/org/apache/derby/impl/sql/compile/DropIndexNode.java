/*

   Derby - Class org.apache.derby.impl.sql.compile.DropIndexNode

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.compile.CompilerContext;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;

import org.apache.derby.iapi.error.StandardException;

/**
 * A DropIndexNode is the root of a QueryTree that represents a DROP INDEX
 * statement.
 *
 * @author Jeff Lichtman
 */

public class DropIndexNode extends DropStatementNode
{
	ConglomerateDescriptor	cd;
	TableDescriptor			td;

	public String statementToString()
	{
		return "DROP INDEX";
	}

	/**
	 * Bind this DropIndexNode.  This means looking up the index,
	 * verifying it exists and getting the conglomerate number.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */
	public QueryTreeNode bind() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		DataDictionary			dd = getDataDictionary();
		SchemaDescriptor		sd;

		sd = getSchemaDescriptor();

		if (sd.getUUID() != null) 
			cd = dd.getConglomerateDescriptor(getRelativeName(), sd, false);

		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, getFullName());
		}

		/* Get the table descriptor */
		td = getTableDescriptor(cd.getTableID());

		/* Drop index is not allowed on an index backing a constraint -
		 * user must drop the constraint, which will drop the index.
		 * Drop constraint drops the constraint before the index,
		 * so it's okay to drop a backing index if we can't find its
		 * ConstraintDescriptor.
		 */
		if (cd.isConstraint())
		{
			ConstraintDescriptor conDesc;
			String constraintName;

			conDesc = dd.getConstraintDescriptor(td, cd.getUUID());
			if (conDesc != null)
			{
				constraintName = conDesc.getConstraintName();
				throw StandardException.newException(SQLState.LANG_CANT_DROP_BACKING_INDEX, 
										getFullName(), constraintName);
			}
		}

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(td);
		cc.createDependency(cd);

		return this;
	}

	// inherit generate() method from DDLStatementNode

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropIndexConstantAction( getFullName(),
											 getRelativeName(),
											 getRelativeName(),
											 getSchemaDescriptor().getSchemaName(),
											 td.getUUID(),
											 td.getHeapConglomerateId());
	}
}
