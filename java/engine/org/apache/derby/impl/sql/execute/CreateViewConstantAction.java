/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateViewConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ViewDescriptor;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	CREATE VIEW Statement at Execution time.
 *  A view is represented as:
 *  <UL>
 *  <LI> TableDescriptor with the name of the view and type VIEW_TYPE
 *  <LI> Set of ColumnDescriptor's for the column names and types
 *  <LI> ViewDescriptor describing the SQL query that makes up the view.
 *  </UL>
 *  Dependencies are created as:
 *  <UL>
 *  <LI> ViewDescriptor depends on the Providers that its compiled
 *  query depends on.
 *  <LI> ViewDescriptor depends on the privileges required to execute the view.
 *  </UL>
 *  Note there are no dependencies created between the ViewDescriptor, TableDecriptor
 *  and the ColumnDescriptor's.
 *
 */

class CreateViewConstantAction extends DDLConstantAction
{
	
	private final String					tableName;
	private final String					schemaName;
	private final String					viewText;
	private final int						tableType;
	private final int						checkOption;
	private final ColumnInfo[]			columnInfo;
	private final ProviderInfo[]			providerInfo;
	private final UUID					compSchemaId;
	
	// CONSTRUCTORS
	/**
	 *	Make the ConstantAction for a CREATE VIEW statement.
	 *
	 *  @param schemaName			name for the schema that view lives in.
	 *  @param tableName	Name of view.
	 *  @param tableType	Type of table (ie. TableDescriptor.VIEW_TYPE).
	 *	@param viewText		Text of query expression for view definition
	 *  @param checkOption	Check option type
	 *  @param columnInfo	Information on all the columns in the table.
	 *  @param providerInfo Information on all the Providers
	 *  @param compSchemaId 	Compilation Schema Id
	 */
	CreateViewConstantAction(
								String			schemaName,
								String			tableName,
								int				tableType,
								String			viewText,
								int				checkOption,
								ColumnInfo[]	columnInfo,
								ProviderInfo[]  providerInfo,
								UUID			compSchemaId)
	{
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.tableType = tableType;
		this.viewText = viewText;
		this.checkOption = checkOption;
		this.columnInfo = columnInfo;
		this.providerInfo = providerInfo;
		this.compSchemaId = compSchemaId;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(schemaName != null, "Schema name is null");
		}
	}

	// OBJECT METHODS

	public	String	toString()
	{
		return constructToString("CREATE VIEW ", tableName);
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE VIEW.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		TableDescriptor 			td;
		UUID 						toid;
		ColumnDescriptor			columnDescriptor;
		ViewDescriptor				vd;

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

		SchemaDescriptor sd = DDLConstantAction.getSchemaDescriptorForCreate(dd, activation, schemaName);

		/* Create a new table descriptor.
		 * (Pass in row locking, even though meaningless for views.)
		 */
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		td = ddg.newTableDescriptor(tableName,
									sd,
									tableType,
									TableDescriptor.ROW_LOCK_GRANULARITY);

		dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);
		toid = td.getUUID();

		// for each column, stuff system.column
		ColumnDescriptor[] cdlArray = new ColumnDescriptor[columnInfo.length];
		int index = 1;
		for (int ix = 0; ix < columnInfo.length; ix++)
		{
			columnDescriptor = new ColumnDescriptor(
				                   columnInfo[ix].name,
								   index++,
								   columnInfo[ix].dataType,
								   columnInfo[ix].defaultValue,
								   columnInfo[ix].defaultInfo,
								   td,
								   (UUID) null,
								   columnInfo[ix].autoincStart,
								   columnInfo[ix].autoincInc,
								   columnInfo[ix].autoincCycle
							   );
			cdlArray[ix] = columnDescriptor;
		}

		dd.addDescriptorArray(cdlArray, td,
							  DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		// add columns to the column descriptor list.
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		for (int i = 0; i < cdlArray.length; i++)
			cdl.add(cdlArray[i]);

		/* Get and add a view descriptor */
		vd = ddg.newViewDescriptor(toid, tableName, viewText, 
									checkOption, 
									(compSchemaId == null) ?
										lcc.getDefaultSchema().getUUID() :
										compSchemaId);

		for (int ix = 0; ix < providerInfo.length; ix++)
		{
			/* We should always be able to find the Provider */
				Provider provider = (Provider) providerInfo[ix].
										getDependableFinder().
											getDependable(dd,
												providerInfo[ix].getObjectId());
				dm.addDependency(vd, provider, lcc.getContextManager());
		}
		//store view's dependency on various privileges in the dependeny system
		storeViewTriggerDependenciesOnPrivileges(activation, vd);

		dd.addDescriptor(vd, sd, DataDictionary.SYSVIEWS_CATALOG_NUM, true, tc);
	}
}
