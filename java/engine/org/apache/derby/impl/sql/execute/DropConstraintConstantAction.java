/*

   Derby - Class org.apache.derby.impl.sql.execute.DropConstraintConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.FormatableHashtable;

import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.types.DataValueFactory;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	drop constraint at Execution time.
 *
 *	@version 0.1
 *	@author Jerry Brenner
 */

class DropConstraintConstantAction extends ConstraintConstantAction
{

	private boolean cascade;		// default false
	private String constraintSchemaName;
    private int verifyType;

	// CONSTRUCTORS

	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintSchemaName		the schema that constraint lives in.
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param tableSchemaName				the schema that table lives in.
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param behavior			the drop behavior (e.g. StatementType.DROP_CASCADE)
	 */
	DropConstraintConstantAction(
		               String				constraintName,
					   String				constraintSchemaName,
		               String				tableName,
					   UUID					tableId,
					   String				tableSchemaName,
					   IndexConstantAction indexAction,
					   int					behavior,
                       int                  verifyType)
	{
		super(constraintName, DataDictionary.DROP_CONSTRAINT, tableName, 
			  tableId, tableSchemaName, indexAction);

		cascade = (behavior == StatementType.DROP_CASCADE);
		this.constraintSchemaName = constraintSchemaName;
        this.verifyType = verifyType;
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		if (constraintName == null)
			return "DROP PRIMARY KEY";

		String ss = constraintSchemaName == null ? schemaName : constraintSchemaName;
		return "DROP CONSTRAINT " + ss + "." + constraintName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConstraintDescriptor		conDesc = null;
		TableDescriptor				td;
		UUID							indexId = null;
		String						indexUUIDString;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();


		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		dd.startWriting(lcc);

		td = dd.getTableDescriptor(tableId);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

		/* Table gets locked in AlterTableConstantAction */

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/

		SchemaDescriptor tdSd = td.getSchemaDescriptor();
		SchemaDescriptor constraintSd = 
			constraintSchemaName == null ? tdSd : dd.getSchemaDescriptor(constraintSchemaName, tc, true);


		/* Get the constraint descriptor for the index, along
		 * with an exclusive row lock on the row in sys.sysconstraints
		 * in order to ensure that no one else compiles against the
		 * index.
		 */
		if (constraintName == null)  // this means "alter table drop primary key"
			conDesc = dd.getConstraintDescriptors(td).getPrimaryKey();
		else
			conDesc = dd.getConstraintDescriptorByName(td, constraintSd, constraintName, true);

		// Error if constraint doesn't exist
		if (conDesc == null)
		{
			String errorName = constraintName == null ? "PRIMARY KEY" :
								(constraintSd.getSchemaName() + "."+ constraintName);

			throw StandardException.newException(SQLState.LANG_DROP_NON_EXISTENT_CONSTRAINT, 
						errorName,
						td.getQualifiedName());
		}
        switch( verifyType)
        {
        case DataDictionary.UNIQUE_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "UNIQUE");
            break;

        case DataDictionary.CHECK_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "CHECK");
            break;

        case DataDictionary.FOREIGNKEY_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "FOREIGN KEY");
            break;
        }

		boolean cascadeOnRefKey = (cascade && 
						conDesc instanceof ReferencedKeyConstraintDescriptor);
		if (!cascadeOnRefKey)
		{
			dm.invalidateFor(conDesc, DependencyManager.DROP_CONSTRAINT, lcc);
		}

		/*
		** If we had a primary/unique key and it is drop cascade,	
		** drop all the referencing keys now.  We MUST do this AFTER
		** dropping the referenced key because otherwise we would
		** be repeatedly changing the reference count of the referenced
		** key and generating unnecessary I/O.
		*/
		dropConstraintAndIndex(dm, td, dd, conDesc, tc, activation, !cascadeOnRefKey);

		if (cascadeOnRefKey) 
		{
			ForeignKeyConstraintDescriptor fkcd;
			ReferencedKeyConstraintDescriptor cd;
			ConstraintDescriptorList cdl;

			cd = (ReferencedKeyConstraintDescriptor)conDesc;
			cdl = cd.getForeignKeyConstraints(ReferencedKeyConstraintDescriptor.ALL);
			int cdlSize = cdl.size();

			for(int index = 0; index < cdlSize; index++)
			{
				fkcd = (ForeignKeyConstraintDescriptor) cdl.elementAt(index);
				dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);
				dropConstraintAndIndex(dm, fkcd.getTableDescriptor(), dd, fkcd,
								tc, activation, true);
			}
	
			/*
			** We told dropConstraintAndIndex not to
			** remove our dependencies, so send an invalidate,
			** and drop the dependencies.
			*/
			dm.invalidateFor(conDesc, DependencyManager.DROP_CONSTRAINT, lcc);
			dm.clearDependencies(lcc, conDesc);
		}
	}

	/*
	 * Drop the constraint.  Clears dependencies, drops 
	 * the backing index and removes the constraint
	 * from the list on the table descriptor.  Does NOT
	 * do an dm.invalidateFor()
	 */
	protected static void dropConstraintAndIndex(DependencyManager	dm,
								TableDescriptor 		td,
								DataDictionary 			dd,
								ConstraintDescriptor 	conDesc,
								TransactionController 	tc,
								Activation				activation,
								boolean					clearDependencies)
		throws StandardException
	{
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tc != null, "tc is null");
			SanityManager.ASSERT(td != null, "td is null");
		}

		if (clearDependencies)
		{
			dm.clearDependencies(activation.getLanguageConnectionContext(), conDesc);
		}

		/* Drop the constraint.
		 * NOTE: This must occur before dropping any backing index, since
		 * a user is not allowed to drop a backing index without dropping
		 * the constraint.
		 */
		dd.dropConstraintDescriptor(td, conDesc, tc);

		/* Drop the index, if there's one for this constraint.
		 * NOTE: There will always be an indexAction. We don't
		 * force the constraint to exist at bind time, so we always
		 * generate one.
		 */
		if (conDesc.hasBackingIndex())
		{
			ConglomerateDescriptor[] conglomDescs;

			// it may have duplicates, and we drop a backing index
			// Bug 4307
			// We need to get the conglomerate descriptors from the 
			// dd in case we dropped other constraints in a cascade operation. 
			conglomDescs = dd.getConglomerateDescriptors(
							conDesc.getConglomerateId());

			if (conglomDescs.length != 0)
			{
				for (int i = 0; i < conglomDescs.length; i++)
				{
					if (conglomDescs[i].isConstraint())
					{
						DropIndexConstantAction.dropIndex(dm, dd, tc,
													conglomDescs[i], td, activation);
						break;
					}
				}
			}
		}

		td.removeConstraintDescriptor(conDesc);
	}
}
