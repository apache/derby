/*

   Derby - Class org.apache.derby.impl.sql.compile.FKConstraintDefinitionNode

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

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.impl.sql.compile.ActivationClassBuilder;
import org.apache.derby.impl.sql.execute.ConstraintInfo;

import org.apache.derby.iapi.util.JBitSet;
import org.apache.derby.iapi.util.ReuseFactory;
import org.apache.derby.iapi.sql.dictionary.DDUtils;

/**
 * A FKConstraintDefintionNode represents table constraint definitions.
 *
 */

public final class FKConstraintDefinitionNode extends ConstraintDefinitionNode
{
	TableName 			refTableName;
	ResultColumnList	refRcl;
	SchemaDescriptor	refTableSd;
	int                 refActionDeleteRule;  // referential action on  delete
	int                 refActionUpdateRule;  // referential action on update
	public void init(
						Object 			constraintName, 
						Object 			refTableName, 
						Object			fkRcl,
						Object			refRcl,
						Object          refActions)
	{
		super.init(
				constraintName,
				ReuseFactory.getInteger(DataDictionary.FOREIGNKEY_CONSTRAINT),
				fkRcl, 
				null,
				null,
				null);
		this.refRcl = (ResultColumnList) refRcl;
		this.refTableName = (TableName) refTableName;

		this.refActionDeleteRule = ((int[]) refActions)[0];
		this.refActionUpdateRule = ((int[]) refActions)[1];
	}

	/**
	 * Bind this constraint definition.  Figure out some
	 * information about the table we are binding against.
	 *
	 * @param dd DataDictionary
	 * 
	 * @exception StandardException on error
	 */
	protected void bind(DDLStatementNode ddlNode, DataDictionary dd)	throws StandardException
	{
		super.bind(ddlNode, dd);

		refTableSd = getSchemaDescriptor(refTableName.getSchemaName());

		if (refTableSd.isSystemSchema())
		{
			throw StandardException.newException(SQLState.LANG_NO_FK_ON_SYSTEM_SCHEMA);
		}

		// check the referenced table, unless this is a self-referencing constraint
		if (refTableName.equals(ddlNode.getObjectName()))
			return;

		// error when the referenced table does not exist
		TableDescriptor td = getTableDescriptor(refTableName.getTableName(), refTableSd);
		if (td == null)
			throw StandardException.newException(SQLState.LANG_INVALID_FK_NO_REF_TAB, 
												getConstraintMoniker(), 
												refTableName.getTableName());

		// Verify if REFERENCES_PRIV is granted to columns referenced
		getCompilerContext().pushCurrentPrivType(getPrivType());

		// Indicate that this statement has a dependency on the
		// table which is referenced by this foreign key:
		getCompilerContext().createDependency(td);

		// If references clause doesn't have columnlist, get primary key info
		if (refRcl.size()==0 && (td.getPrimaryKey() != null))
		{
			// Get the primary key columns
			int[] refCols = td.getPrimaryKey().getReferencedColumns();
			for (int i=0; i<refCols.length; i++)
			{
				ColumnDescriptor cd = td.getColumnDescriptor(refCols[i]);
				// Set tableDescriptor for this column descriptor. Needed for adding required table
				// access permission. Column descriptors may not have this set already.
				cd.setTableDescriptor(td);
				if (isPrivilegeCollectionRequired())
					getCompilerContext().addRequiredColumnPriv(cd);
			}

		}
		else
		{
			for (int i=0; i<refRcl.size(); i++)
			{
				ResultColumn rc = (ResultColumn) refRcl.elementAt(i);
				ColumnDescriptor cd = td.getColumnDescriptor(rc.getName());
				if (cd != null)
				{
					// Set tableDescriptor for this column descriptor. Needed for adding required table
					// access permission. Column descriptors may not have this set already.
					cd.setTableDescriptor(td);
					if (isPrivilegeCollectionRequired())
						getCompilerContext().addRequiredColumnPriv(cd);
				}
			}
		}
		getCompilerContext().popCurrentPrivType();
	}

	public ConstraintInfo getReferencedConstraintInfo()
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(refTableSd != null, 
					"You must call bind() before calling getConstraintInfo");
		}
		return new ConstraintInfo(refTableName.getTableName(), refTableSd,
								  refRcl.getColumnNames(), refActionDeleteRule,
								  refActionUpdateRule);
	}

	public	TableName	getRefTableName() { return refTableName; }

	int getPrivType()
	{
		return Authorizer.REFERENCES_PRIV;
	}
}
