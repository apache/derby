/*

   Derby - Class org.apache.derby.impl.sql.compile.FKConstraintDefinitionNode

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

import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
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
 * @author jamie
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
	 * @param DataDictionary the dd
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
		if (!refTableName.equals(ddlNode.getObjectName())) {

			// clear error when the referenced table does not exist
			if (getTableDescriptor(refTableName.getTableName(), refTableSd) == null)
				throw StandardException.newException(SQLState.LANG_INVALID_FK_NO_REF_TAB, 
												getConstraintMoniker(), 
												refTableName.getTableName());
			
			// now check any other limitations
			ddlNode.getTableDescriptor(refTableName);
		}
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

}













