/*

   Derby - Class org.apache.derby.impl.sql.compile.DropTableNode

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

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.BaseActivation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.compile.CompilerContext;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * A DropTableNode is the root of a QueryTree that represents a DROP TABLE
 * statement.
 *
 * @author Jerry Brenner
 */

public class DropTableNode extends DropStatementNode
{
	private long		conglomerateNumber;
	private int			dropBehavior;
	private	TableDescriptor	td;

	/**
	 * Intializer for a DropTableNode
	 *
	 * @param objectName	The name of the object being dropped
	 * @param dropBehavior		Drop behavior (RESTRICT | CASCADE)
	 *
	 */

	public void init(Object dropObjectName, Object dropBehavior)
		throws StandardException
	{
		initAndCheck(dropObjectName);
		this.dropBehavior = ((Integer) dropBehavior).intValue();
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
				"conglomerateNumber: " + conglomerateNumber + "\n" +
				"td: " + ((td == null) ? "null" : td.toString()) + "\n" +
				"dropBehavior: " + "\n" + dropBehavior + "\n";
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "DROP TABLE";
	}

	/**
	 * Bind this LockTableNode.  This means looking up the table,
	 * verifying it exists and getting the heap conglomerate number.
	 *
	 * @return	The bound query tree
	 *
	 * @exception StandardException		Thrown on error
	 */

	public QueryTreeNode bind() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();

		td = getTableDescriptor();

		conglomerateNumber = td.getHeapConglomerateId();

		/* Get the base conglomerate descriptor */
		ConglomerateDescriptor cd = td.getConglomerateDescriptor(conglomerateNumber);

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(td);
		cc.createDependency(cd);

		return this;
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If table being dropped is in SESSION schema, then return true. 
		return isSessionSchema(td.getSchemaDescriptor());
	}

	// inherit generate() method from DDLStatementNode


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropTableConstantAction( getFullName(),
											 getRelativeName(),
											 getSchemaDescriptor(),
											 conglomerateNumber,
											 td.getUUID(),
											 dropBehavior);
	}
}
