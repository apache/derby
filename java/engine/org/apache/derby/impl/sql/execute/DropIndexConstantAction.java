/*

   Derby - Class org.apache.derby.impl.sql.execute.DropIndexConstantAction

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

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.depend.Dependency;
import org.apache.derby.iapi.sql.depend.Dependent;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

import java.util.Enumeration;


/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP INDEX Statement at Execution time.
 *
 *	@author Jeff Lichtman	Cribbed from DropTableConstantAction
 */

class DropIndexConstantAction extends IndexConstantAction
{

	private String				fullIndexName;
	private long				tableConglomerateId;


	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP INDEX statement.
	 *
	 *
	 *	@param	fullIndexName		Fully qualified index name
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName			Schema that index lives in.
	 *  @param  tableId				UUID for table
	 *  @param  tableConglomerateId	heap Conglomerate Id for table
	 *
	 */
	DropIndexConstantAction(
								String				fullIndexName,
								String				indexName,
								String				tableName,
								String				schemaName,
								UUID				tableId,
								long				tableConglomerateId)
	{
		super(tableId, indexName, tableName, schemaName);
		this.fullIndexName = fullIndexName;
		this.tableConglomerateId = tableConglomerateId;
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "DROP INDEX " + fullIndexName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP INDEX.
	 *
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction(Activation activation)
						throws StandardException
	{
		TableDescriptor td;
		ConglomerateDescriptor cd;

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

		// need to lock heap in exclusive mode first.  Because we can't first
		// shared lock the row in SYSCONGLOMERATES and later exclusively lock
		// it, this is potential deadlock (track 879).  Also td need to be
		// gotten after we get the lock, a concurrent thread could be modifying
		// table shape (track 3804, 3825)

		// older version (or target) has to get td first, potential deadlock
		if (tableConglomerateId == 0)
		{
			td = dd.getTableDescriptor(tableId);
			if (td == null)
			{
				throw StandardException.newException(
					SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
			}
			tableConglomerateId = td.getHeapConglomerateId();
		}
		lockTableForDDL(tc, tableConglomerateId, true);

		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true) ;

		/* Get the conglomerate descriptor for the index, along
		 * with an exclusive row lock on the row in sys.sysconglomerates
		 * in order to ensure that no one else compiles against the
		 * index.
		 */
		cd = dd.getConglomerateDescriptor(indexName, sd, true);

		if (cd == null)
		{
			throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION, fullIndexName);
		}

		/* Prepare all dependents to invalidate.  (This is there chance
		 * to say that they can't be invalidated.)
		 * We check for invalidation before we drop the conglomerate descriptor
		 * since the conglomerate descriptor may be looked up as part of
		 * decoding tuples in SYSDEPENDS.
		 */
		dropIndex(dm, dd, tc, cd, td, activation);
	}

	public static void dropIndex(DependencyManager 	dm,
							DataDictionary			dd,
							TransactionController	tc,
							ConglomerateDescriptor	cd,
							TableDescriptor			td,
							Activation				act)
		throws StandardException
	{	
		LanguageConnectionContext lcc = act.getLanguageConnectionContext();
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(tc != null, "tc is null");
			SanityManager.ASSERT(cd != null, "cd is null");
		}

		// only drop the conglomerate if no similar index but with different
		// name. Get from dd in case we drop other dup indexes with a cascade operation

		if (dd.getConglomerateDescriptors(cd.getConglomerateNumber()).length == 1)
		{
			/* Drop statistics */
			dd.dropStatisticsDescriptors(td.getUUID(), cd.getUUID(), tc);

			/* Drop the conglomerate */
			tc.dropConglomerate(cd.getConglomerateNumber());
		}

		// invalidate any prepared statements that
		// depended on the index (including this one)
		dm.invalidateFor(cd, DependencyManager.DROP_INDEX, lcc);

		/* Drop the conglomerate descriptor */
		dd.dropConglomerateDescriptor(cd, tc);

		/* 
		** Remove the conglomerate descriptor from the list hanging off of the
		** table descriptor
		*/
		td.removeConglomerateDescriptor(cd);
	}

}
