/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateTableConstantAction

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
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.DefaultDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;

import org.apache.derby.iapi.sql.depend.Provider;
import org.apache.derby.iapi.sql.depend.ProviderInfo;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.catalog.DependableFinder;
import org.apache.derby.catalog.UUID;

import org.apache.derby.catalog.types.DefaultInfoImpl;

import java.util.Properties;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	CREATE TABLE Statement at Execution time.
 *
 */

class CreateTableConstantAction extends DDLConstantAction
{

	private char					lockGranularity;
	private boolean					onCommitDeleteRows; //If true, on commit delete rows else on commit preserve rows of temporary table.
	private boolean					onRollbackDeleteRows; //If true, on rollback delete rows from temp table if it was logically modified in that UOW. true is the only supported value
	private String					tableName;
	private String					schemaName;
	private int						tableType;
	private ColumnInfo[]			columnInfo;
	private CreateConstraintConstantAction[]	constraintActions;
	private Properties				properties;
	
	/**
	 *	Make the ConstantAction for a CREATE TABLE statement.
	 *
	 *  @param schemaName	name for the schema that table lives in.
	 *  @param tableName	Name of table.
	 *  @param tableType	Type of table (e.g., BASE, global temporary table).
	 *  @param columnInfo	Information on all the columns in the table.
	 *		 (REMIND tableDescriptor ignored)
	 *  @param constraintActions	CreateConstraintConstantAction[] for constraints
	 *  @param properties	Optional table properties
	 * @param lockGranularity	The lock granularity.
	 * @param onCommitDeleteRows	If true, on commit delete rows else on commit preserve rows of temporary table.
	 * @param onRollbackDeleteRows	If true, on rollback, delete rows from temp tables which were logically modified. true is the only supported value
	 */
	CreateTableConstantAction(
								String			schemaName,
								String			tableName,
								int				tableType,
								ColumnInfo[]	columnInfo,
								CreateConstraintConstantAction[] constraintActions,
								Properties		properties,
								char			lockGranularity,
								boolean			onCommitDeleteRows,
								boolean			onRollbackDeleteRows)
	{
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.tableType = tableType;
		this.columnInfo = columnInfo;
		this.constraintActions = constraintActions;
		this.properties = properties;
		this.lockGranularity = lockGranularity;
		this.onCommitDeleteRows = onCommitDeleteRows;
		this.onRollbackDeleteRows = onRollbackDeleteRows;

		if (SanityManager.DEBUG)
		{
			if (tableType == TableDescriptor.BASE_TABLE_TYPE && lockGranularity != TableDescriptor.TABLE_LOCK_GRANULARITY &&
				lockGranularity != TableDescriptor.ROW_LOCK_GRANULARITY)
			{
				SanityManager.THROWASSERT(
					"Unexpected value for lockGranularity = " + lockGranularity);
			}
			if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE && onRollbackDeleteRows == false)
			{
				SanityManager.THROWASSERT(
					"Unexpected value for onRollbackDeleteRows = " + onRollbackDeleteRows);
			}
			SanityManager.ASSERT(schemaName != null, "SchemaName is null");
		}
	}

	// OBJECT METHODS

	public	String	toString()
	{
		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			return constructToString("DECLARE GLOBAL TEMPORARY TABLE ", tableName);
		else
			return constructToString("CREATE TABLE ", tableName);
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE TABLE.
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
		SchemaDescriptor			schemaDescriptor;
		ColumnDescriptor			columnDescriptor;
		ExecRow						template;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/* Mark the activation as being for create table */
		activation.setForCreateTable();

        // setup for create conglomerate call:
        //   o create row template to tell the store what type of rows this
        //     table holds.
        //   o create array of collation id's to tell collation id of each
        //     column in table.
		template            = RowUtil.getEmptyValueRow(columnInfo.length, lcc);
        int[] collation_ids = new int[columnInfo.length];

		for (int ix = 0; ix < columnInfo.length; ix++)
		{
            ColumnInfo  col_info = columnInfo[ix];

            // Get a template value for each column

			if (col_info.defaultValue != null)
            {
                /* If there is a default value, use it, otherwise use null */
				template.setColumn(ix + 1, col_info.defaultValue);
            }
			else
            {
				template.setColumn(ix + 1, col_info.dataType.getNull());
            }

            // get collation info for each column.

            collation_ids[ix] = col_info.dataType.getCollationType();
		}


		/* create the conglomerate to hold the table's rows
		 * RESOLVE - If we ever have a conglomerate creator
		 * that lets us specify the conglomerate number then
		 * we will need to handle it here.
		 */
		long conglomId = tc.createConglomerate(
				"heap", // we're requesting a heap conglomerate
				template.getRowArray(), // row template
				null, //column sort order - not required for heap
                collation_ids,
				properties, // properties
				tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE ?
                    (TransactionController.IS_TEMPORARY | 
                     TransactionController.IS_KEPT) : 
                        TransactionController.IS_DEFAULT);

		/*
		** Inform the data dictionary that we are about to write to it.
		** There are several calls to data dictionary "get" methods here
		** that might be done in "read" mode in the data dictionary, but
		** it seemed safer to do this whole operation in "write" mode.
		**
		** We tell the data dictionary we're done writing at the end of
		** the transaction.
		*/
		if ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE )
			dd.startWriting(lcc);

		SchemaDescriptor sd;
		if (tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE)
			sd = dd.getSchemaDescriptor(schemaName, tc, true);
		else
			sd = DDLConstantAction.getSchemaDescriptorForCreate(dd, activation, schemaName);

		//
		// Create a new table descriptor.
		// 
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		if ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE )
		{
			td = ddg.newTableDescriptor(tableName, sd, tableType, lockGranularity);
			dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM, false, tc);
		} else
		{
			td = ddg.newTableDescriptor(tableName, sd, tableType, onCommitDeleteRows, onRollbackDeleteRows);
			td.setUUID(dd.getUUIDFactory().createUUID());
		}
		toid = td.getUUID();

		// Save the TableDescriptor off in the Activation
		activation.setDDLTableDescriptor(td);

		/* NOTE: We must write the columns out to the system
		 * tables before any of the conglomerates, including
		 * the heap, since we read the columns before the
		 * conglomerates when building a TableDescriptor.
		 * This will hopefully reduce the probability of
		 * a deadlock involving those system tables.
		 */
		
		// for each column, stuff system.column
		int index = 1;

		ColumnDescriptor[] cdlArray = new ColumnDescriptor[columnInfo.length];
		for (int ix = 0; ix < columnInfo.length; ix++)
		{
			UUID defaultUUID = columnInfo[ix].newDefaultUUID;

			/* Generate a UUID for the default, if one exists
			 * and there is no default id yet.
			 */
			if (columnInfo[ix].defaultInfo != null &&
				defaultUUID == null)
			{
				defaultUUID = dd.getUUIDFactory().createUUID();
			}

			if (columnInfo[ix].autoincInc != 0)//dealing with autoinc column
            {
                columnDescriptor = new ColumnDescriptor
                    (
                     columnInfo[ix].name,
                     index++,
                     columnInfo[ix].dataType,
                     columnInfo[ix].defaultValue,
                     columnInfo[ix].defaultInfo,
                     td,
                     defaultUUID,
                     columnInfo[ix].autoincStart,
                     columnInfo[ix].autoincInc,
                     columnInfo[ix].autoinc_create_or_modify_Start_Increment,
                     columnInfo[ix].autoincCycle
                     );

                //
                // If we're at level 10.11 or higher, we create a sequence generator
                // for the auto-increment column. See derby-6542.
                //
                if ( dd.checkVersion( DataDictionary.DD_VERSION_DERBY_10_11, null ) )
                {
                    CreateSequenceConstantAction    csca = makeCSCA
                        ( columnInfo[ ix ], TableDescriptor.makeSequenceName( toid ) );
                    csca.executeConstantAction( activation );
                }
            }
            else
            {
				columnDescriptor = new ColumnDescriptor
                    (
                     columnInfo[ix].name,
                     index++,
                     columnInfo[ix].dataType,
                     columnInfo[ix].defaultValue,
                     columnInfo[ix].defaultInfo,
                     td,
                     defaultUUID,
                     columnInfo[ix].autoincStart,
                     columnInfo[ix].autoincInc,
                     columnInfo[ix].autoincCycle
                     );
            }

			cdlArray[ix] = columnDescriptor;
		}

		if ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE )
		{
			dd.addDescriptorArray(cdlArray, td,
							  DataDictionary.SYSCOLUMNS_CATALOG_NUM,
							  false, tc);
		}

		// now add the column descriptors to the table.
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		for (int i = 0; i < cdlArray.length; i++)
			cdl.add(cdlArray[i]);
				 
		//
		// Create a conglomerate desciptor with the conglomId filled in and
		// add it.
		//
		// RESOLVE: Get information from the conglomerate descriptor which
		//          was provided. 
		//
		ConglomerateDescriptor cgd =
			ddg.newConglomerateDescriptor(conglomId, null, false, null, false, null, toid,
										  sd.getUUID());
		if ( tableType != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE )
		{
			dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM,
						 false, tc);
		}

		// add the newly added conglomerate to the table descriptor
		ConglomerateDescriptorList conglomList = td.getConglomerateDescriptorList();
		conglomList.add(cgd);

		/* Create any constraints */
		if (constraintActions != null)
		{
			/*
			** Do everything but FK constraints first,
			** then FK constraints on 2nd pass.
			*/
			for (int conIndex = 0; conIndex < constraintActions.length; conIndex++)
			{
				// skip fks
				if (!constraintActions[conIndex].isForeignKeyConstraint())
				{
					constraintActions[conIndex].executeConstantAction(activation);
				}
			}

			for (int conIndex = 0; conIndex < constraintActions.length; conIndex++)
			{
				// only foreign keys
				if (constraintActions[conIndex].isForeignKeyConstraint())
				{
					constraintActions[conIndex].executeConstantAction(activation);
				}
			}
		}

        //
        // Add dependencies. These can arise if a generated column depends
        // on a user created function.
        //
		for (int ix = 0; ix < columnInfo.length; ix++)
		{
            addColumnDependencies( lcc, dd, td, columnInfo[ ix ] );
        }

        //
        // The table itself can depend on the user defined types of its columns.
        //
        adjustUDTDependencies( lcc, dd, td, columnInfo, false );
        
		if ( tableType == TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE )
		{
			lcc.addDeclaredGlobalTempTable(td);
		}

		// Indicate that the CREATE TABLE statement itself depends on the
		// table it is creating. Normally such statement dependencies are
		// added during compilation, but here we have a bootstrapping issue
		// because the table doesn't exist until the CREATE TABLE statement
		// has been executed, so we had to defer the creation of this
		// dependency until now. (DERBY-4479)
		dd.getDependencyManager().addDependency(
			activation.getPreparedStatement(), td, lcc.getContextManager());

	}

    /** Create a sequence generator for an identity column */
    public  static CreateSequenceConstantAction    makeCSCA
        ( ColumnInfo info, String sequenceName )
        throws StandardException
    {
        DataTypeDescriptor  dtd = info.dataType;
        long[]      bounds = dtd.getNumericBounds();
        long    minValue = bounds[ DataTypeDescriptor.MIN_VALUE_IDX ];
        long    maxValue = bounds[ DataTypeDescriptor.MAX_VALUE_IDX ];

	boolean cycling=false;

	if(info.autoincCycle)
		cycling=true;
	
        return new CreateSequenceConstantAction
            (
             SchemaDescriptor.STD_SYSTEM_SCHEMA_NAME,
             sequenceName,
             dtd,
             info.autoincStart,
             info.autoincInc,
             maxValue,
             minValue,
             cycling
             );
    }

}
