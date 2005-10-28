/*

   Derby - Class org.apache.derby.impl.sql.execute.DropViewConstantAction

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

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.catalog.UUID;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	DROP VIEW Statement at Execution time.
 *
 *	@author Jerry Brenner.
 */

class DropViewConstantAction extends DDLConstantAction
{

	private String				fullTableName;
	private String				tableName;
	private SchemaDescriptor	sd;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a DROP VIEW statement.
	 *
	 *
	 *	@param	fullTableName		Fully qualified table name
	 *	@param	tableName			Table name.
	 *	@param	sd					Schema that view lives in.
	 *
	 */
	DropViewConstantAction(
								String				fullTableName,
								String				tableName,
								SchemaDescriptor	sd )
	{
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;

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
		return "DROP VIEW " + fullTableName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP VIEW.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TableDescriptor td;
		ViewDescriptor vd;

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

		/* Get the table descriptor.  We're responsible for raising
		 * the error if it isn't found 
		 */
		td = dd.getTableDescriptor(tableName, sd);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		/* Verify that TableDescriptor represents a view */
		if (td.getTableType() != TableDescriptor.VIEW_TYPE)
		{
			throw StandardException.newException(SQLState.LANG_DROP_VIEW_ON_NON_VIEW, fullTableName);
		}

		vd = dd.getViewDescriptor(td);

		vd.dropViewWork(dd, dm, lcc, tc, sd, td, false);
	}
}
