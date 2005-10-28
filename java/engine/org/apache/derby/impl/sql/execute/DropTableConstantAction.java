/*

   Derby - Class org.apache.derby.impl.sql.execute.DropTableConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.GenericDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.TriggerDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

import java.util.Enumeration;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP TABLE Statement at Execution time.
 *
 *	@author Rick Hillegas	Extracted code from DropTableResultSet.
 */

class DropTableConstantAction extends DDLSingleTableConstantAction
{

	private final long				conglomerateNumber;
	private final String				fullTableName;
	private final String				tableName;
	private final SchemaDescriptor	sd;
	private final boolean 	cascade; 		

	// CONSTRUCTORS


	/**
	 *	Make the ConstantAction for a DROP TABLE statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that table lives in.
	 *  @param  conglomerateNubmer	Conglomerate number for heap
	 *  @param  tableId				UUID for table
	 *  @param  behavior			drop behavior: RESTRICT, CASCADE or default
	 *
	 */
	DropTableConstantAction(
								String				fullTableName,
								String				tableName,
								SchemaDescriptor	sd,
								long				conglomerateNumber,
								UUID				tableId,
								int					behavior)
	{
		super(tableId);
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;
		this.conglomerateNumber = conglomerateNumber;
		this.cascade = (behavior == StatementType.DROP_CASCADE);

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
		}
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP TABLE " + fullTableName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP TABLE.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TableDescriptor td;
		UUID tableID;
		ConglomerateDescriptor[] cds;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		if ((sd != null) && sd.getSchemaName().equals(SchemaDescriptor.STD_DECLARED_GLOBAL_TEMPORARY_TABLES_SCHEMA_NAME)) {
			td = lcc.getTableDescriptorForDeclaredGlobalTempTable(tableName); //check if this is a temp table before checking data dictionary

			if (td == null) //td null here means it is not a temporary table. Look for table in physical SESSION schema
				td = dd.getTableDescriptor(tableName, sd);

			if (td == null) //td null means tableName is not a temp table and it is not a physical table in SESSION schema
			{
				throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
			}

			if (td.getTableType() ==  TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE) {
				dm.invalidateFor(td, DependencyManager.DROP_TABLE, lcc);
				tc.dropConglomerate(td.getHeapConglomerateId());
				lcc.dropDeclaredGlobalTempTable(tableName);
				return;
			}
    }

		/* Lock the table before we access the data dictionary
		 * to prevent deadlocks.
		 *
		 * Note that for DROP TABLE replayed at Targets during REFRESH,
		 * the conglomerateNumber will be 0. That's ok. During REFRESH,
		 * we don't need to lock the conglomerate.
		 */
		if ( conglomerateNumber != 0 ) { lockTableForDDL(tc, conglomerateNumber, true); }

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

		/* Get the table descriptor. */
		td = dd.getTableDescriptor(tableId);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		/* Get an exclusive table lock on the table. */
		long heapId = td.getHeapConglomerateId();
		lockTableForDDL(tc, heapId, true);

		/* Drop the triggers */
		GenericDescriptorList tdl = dd.getTriggerDescriptors(td);
		Enumeration descs = tdl.elements();
		while (descs.hasMoreElements())
		{
			TriggerDescriptor trd = (TriggerDescriptor) descs.nextElement();
			DropTriggerConstantAction.dropTriggerDescriptor(lcc, dm, dd, tc, trd, activation);	
		}

		/* Drop all defaults */
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		int					 cdlSize = cdl.size();
		
		for (int index = 0; index < cdlSize; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);

			// If column has a default we drop the default and
			// any dependencies
			if (cd.getDefaultInfo() != null)
			{
				DefaultDescriptor defaultDesc = cd.getDefaultDescriptor(dd);
				dm.clearDependencies(lcc, defaultDesc);
			}
		}

		/* Drop the columns */
		dd.dropAllColumnDescriptors(tableId, tc);

		/* Drop the constraints */
		dropAllConstraintDescriptors(td, activation);

		/*
		** Drop all the conglomerates.  Drop the heap last, because the
		** store needs it for locking the indexes when they are dropped.
		*/
		cds = td.getConglomerateDescriptors();

		long[] dropped = new long[cds.length - 1];
		int numDropped = 0;
		for (int index = 0; index < cds.length; index++)
		{
			ConglomerateDescriptor cd = cds[index];

			/* if it's for an index, since similar indexes share one
			 * conglomerate, we only drop the conglomerate once
			 */
			if (cd.getConglomerateNumber() != heapId)
			{
				long thisConglom = cd.getConglomerateNumber();

				int i;
				for (i = 0; i < numDropped; i++)
				{
					if (dropped[i] == thisConglom)
						break;
				}
				if (i == numDropped)	// not dropped
				{
					dropped[numDropped++] = thisConglom;
					tc.dropConglomerate(thisConglom);
					dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);
				}
			}
		}

		/* Prepare all dependents to invalidate.  (This is there chance
		 * to say that they can't be invalidated.  For example, an open
		 * cursor referencing a table/view that the user is attempting to
		 * drop.) If no one objects, then invalidate any dependent objects.
		 * We check for invalidation before we drop the table descriptor
		 * since the table descriptor may be looked up as part of
		 * decoding tuples in SYSDEPENDS.
		 */

		dm.invalidateFor(td, cascade ?
						DependencyManager.DROP_TABLE_CASCADE : DependencyManager.DROP_TABLE,
						lcc);

		/* Drop the table */
		dd.dropTableDescriptor(td, sd, tc);

		/* Drop the conglomerate descriptors */
		dd.dropAllConglomerateDescriptors(td, tc);

		/* Drop the store element at last, to prevent dangling reference
		 * for open cursor, beetle 4393.
		 */
		tc.dropConglomerate(heapId);

	}

	private void dropAllConstraintDescriptors(TableDescriptor td, Activation activation)
		throws StandardException
	{
		ConstraintDescriptor				cd;
		ConstraintDescriptorList 			cdl;
		ConstraintDescriptor 				fkcd;
		ConstraintDescriptorList 			fkcdl;
		LanguageConnectionContext			lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		cdl = dd.getConstraintDescriptors(td);
		
		/*
		** First go, don't drop unique or primary keys.
		** This will ensure that self-referential constraints
		** will work ok, even if not cascading.
	 	*/
		/* The current element will be deleted underneath
		 * the loop, so we only increment the counter when
		 * skipping an element. (HACK!)
		 */
		for(int index = 0; index < cdl.size(); )
		{
			cd = cdl.elementAt(index);
			if (cd instanceof ReferencedKeyConstraintDescriptor)
			{
				index++;
				continue;
			}

			dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
			DropConstraintConstantAction.dropConstraintAndIndex(dm, td, dd, cd, 
						tc, activation, true);
		}

		/*
	 	** Referenced keys (unique or pk) constraints only
		*/
		/* The current element will be deleted underneath
		 * the loop. (HACK!)
		 */
		while (cdl.size() > 0)
		{
			cd = cdl.elementAt(0);
			if (SanityManager.DEBUG)
			{
				if (!(cd instanceof ReferencedKeyConstraintDescriptor))
				{
					SanityManager.THROWASSERT("Constraint descriptor not an instance of " +
					"ReferencedKeyConstraintDescriptor as expected.  Is a "+ cd.getClass().getName());
				}
			}

			/*
			** Drop the referenced constraint (after we got
			** the primary keys) now.  Do this prior to
			** droping the referenced keys to avoid performing
			** a lot of extra work updating the referencedcount
			** field of sys.sysconstraints.
			**
			** Pass in false to dropConstraintsAndIndex so it
			** doesn't clear dependencies, we'll do that ourselves.
			*/
			DropConstraintConstantAction.dropConstraintAndIndex(dm, td, dd, cd, 
						tc, activation, false);

			/*
			** If we are going to cascade, get all the
			** referencing foreign keys and zap them first.
			*/
			if (cascade)
			{
				/*
				** Go to the system tables to get the foreign keys
				** to be safe
				*/

				fkcdl = dd.getForeignKeys(cd.getUUID());

				/*
				** For each FK that references this key, drop
				** it.
				*/
				for(int inner = 0; inner < fkcdl.size(); inner++)
				{
					fkcd = (ConstraintDescriptor) fkcdl.elementAt(inner);
					dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);
					DropConstraintConstantAction.dropConstraintAndIndex(
							dm, fkcd.getTableDescriptor(), dd, fkcd,
							tc, activation, true);
					activation.addWarning(
						StandardException.newWarning(SQLState.LANG_CONSTRAINT_DROPPED,
 							fkcd.getConstraintName(),
							fkcd.getTableDescriptor().getName()));
				}
			}

			/*
			** Now that we got rid of the fks (if we were cascading), it is 
			** ok to do an invalidate for.
			*/
			dm.invalidateFor(cd, DependencyManager.DROP_CONSTRAINT, lcc);
			dm.clearDependencies(lcc, cd);
		}
	}

}
