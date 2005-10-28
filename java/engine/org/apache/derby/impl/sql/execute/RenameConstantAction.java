/*

   Derby - Class org.apache.derby.impl.sql.execute.RenameConstantAction

   Copyright 2001, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.StatementType;

import org.apache.derby.iapi.sql.execute.ConstantAction;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.catalog.UUID;

import org.apache.derby.iapi.services.io.FormatableBitSet;


/**
 * This class  describes actions that are ALWAYS performed for a
 * RENAME TABLE/COLUMN/INDEX Statement at Execution time.
 *
 * @author Mamta Satoor.
 */

class RenameConstantAction extends DDLSingleTableConstantAction
{


	private String fullTableName;
	private String tableName;
	private String newTableName;
	private String oldObjectName;
	private String newObjectName;
	private UUID schemaId;
	private SchemaDescriptor sd;
	/* You can rename using either alter table or rename command to
	 * rename a table/column. An index can only be renamed with rename
	 * command. usedAlterTable flag is used to keep that information.
	 */
	private boolean usedAlterTable;
	/* renamingWhat will be set to 1 if user is renaming a table.
	 * Will be set to 2 if user is renaming a column and will be
	 * set to 3 if user is renaming an index
	 */
	private int renamingWhat;

	// CONSTRUCTORS

	/**
	 * Make the ConstantAction for a RENAME TABLE/COLUMN/INDEX statement.
	 *
	 * @param fullTableName Fully qualified table name
	 * @param tableName Table name.
	 * @param oldObjectName This is either the name of column/index in case
	 *		of rename column/index. For rename table, this is null.
	 * @param newObjectName This is new name for table/column/index
	 * @param	sd Schema that table lives in.
	 * @param tableId UUID for table
	 * @param usedAlterTable True-Used Alter Table, False-Used Rename.
	 *		For rename index, this will always be false because
	 *		there is no alter table command to rename index
	 * @param renamingWhat Rename a 1 - table, 2 - column, 3 - index
	 *
	 */
	public	RenameConstantAction
	(
				   String fullTableName,
				   String tableName,
				   String oldObjectName,
				   String newObjectName,
				   SchemaDescriptor sd,
				   UUID tableId,
				   boolean usedAlterTable,
				   int renamingWhat)
	{
		super(tableId);
		this.fullTableName = fullTableName;
		this.tableName = tableName;
		this.sd = sd;
		this.usedAlterTable = usedAlterTable;
		this.renamingWhat = renamingWhat;

		switch (this.renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				this.newTableName = newObjectName;
				this.oldObjectName = null;
				this.newObjectName=newObjectName;
				break;

			case StatementType.RENAME_COLUMN:
			case StatementType.RENAME_INDEX:
				this.oldObjectName = oldObjectName;
				this.newObjectName = newObjectName;
				break;

			default:
				if (SanityManager.DEBUG) {
					SanityManager.THROWASSERT(
					"Unexpected rename action in RenameConstantAction");
				}
		}
		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(sd != null, "SchemaDescriptor is null");
		}
	}

	// OBJECT METHODS

	public	String	toString()
	{
		String renameString;
		if (usedAlterTable)
			renameString = "ALTER TABLE ";
		else
			renameString = "RENAME ";

		switch (this.renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				if(usedAlterTable)
					renameString = renameString + fullTableName + " RENAME TO " + newTableName;
				else
					renameString = renameString + " TABLE " + fullTableName + " TO " + newTableName;
				break;

			case StatementType.RENAME_COLUMN:
				if(usedAlterTable)
					renameString = renameString + fullTableName + " RENAME " + oldObjectName + " TO " + newObjectName;
				else
					renameString = renameString + " COLUMN " + fullTableName + "." + oldObjectName + " TO " + newObjectName;
				break;

			case StatementType.RENAME_INDEX:
				renameString = renameString + " INDEX " + oldObjectName + " TO " + newObjectName;
				break;

			default:
				if (SanityManager.DEBUG) {
						SanityManager.THROWASSERT(
					"Unexpected rename action in RenameConstantAction");
				}
				break;
		}

		return renameString;
	}

	// INTERFACE METHODS


	/**
	 * The guts of the Execution-time logic for RENAME TABLE/COLUMN/INDEX.
	 *
	 * @see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException Thrown on failure
	 */
	public void executeConstantAction
	(
				   Activation activation)
		throws StandardException
	{
		TableDescriptor td;
		UUID tableID;

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
			throw StandardException.newException(
				SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		if (sd == null)
		{
			sd = getAndCheckSchemaDescriptor(dd, schemaId, "RENAME TABLE");
		}

		long heapId = td.getHeapConglomerateId();

		/* need to lock table, beetle 4271
		 */
		lockTableForDDL(tc, heapId, true);

		/* need to get td again, in case it's changed before lock acquired
		 */
		td = dd.getTableDescriptor(tableId);
		if (td == null)
		{
			throw StandardException.newException(
				SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, fullTableName);
		}

		switch (renamingWhat)
		{
			case StatementType.RENAME_TABLE:
				execGutsRenameTable(td, activation);
				break;

			case StatementType.RENAME_COLUMN:
				execGutsRenameColumn(td, activation);
				break;

			case StatementType.RENAME_INDEX:
				execGutsRenameIndex(td, activation);
				break;

			default:
				if (SanityManager.DEBUG) {
					SanityManager.THROWASSERT(
							"Unexpected rename action in RenameConstantAction");
				}
				break;
		}
	}

	//do necessary work for rename table at execute time.
	private void execGutsRenameTable
	(
				   TableDescriptor td, Activation activation)
		throws StandardException
	{
		ConstraintDescriptorList constraintDescriptorList;
		ConstraintDescriptor constraintDescriptor;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();
		dm.invalidateFor(td, DependencyManager.RENAME, lcc);

		/* look for foreign key dependency on the table. If found any,
		use dependency manager to pass the rename action to the
		dependents. */
		constraintDescriptorList = dd.getConstraintDescriptors(td);
		for(int index=0; index<constraintDescriptorList.size(); index++)
		{
			constraintDescriptor = constraintDescriptorList.elementAt(index);
			if (constraintDescriptor instanceof ReferencedKeyConstraintDescriptor)
				dm.invalidateFor(constraintDescriptor, DependencyManager.RENAME, lcc);
		}

		// Drop the table
		dd.dropTableDescriptor(td, sd, tc);
		// Change the table name of the table descriptor
		td.setTableName(newTableName);
		// add the table descriptor with new name
		dd.addDescriptor(td, sd, DataDictionary.SYSTABLES_CATALOG_NUM,
						 false, tc);
	}

	//do necessary work for rename column at execute time.
	private void execGutsRenameColumn
	(
				   TableDescriptor td, Activation activation)
		throws StandardException
	{
		ColumnDescriptor columnDescriptor = null;
		int columnPosition = 0;
		ConstraintDescriptorList constraintDescriptorList;
		ConstraintDescriptor constraintDescriptor;
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/* get the column descriptor for column to be renamed and
		 * using it's position in the table, set the referenced
		 * column map of the table indicating which column is being
		 * renamed. Dependency Manager uses this to find out the
		 * dependents on the column.
		 */
		columnDescriptor = td.getColumnDescriptor(oldObjectName);
		columnPosition = columnDescriptor.getPosition();
		FormatableBitSet toRename = new FormatableBitSet(td.getColumnDescriptorList().size() + 1);
		toRename.set(columnPosition);
		td.setReferencedColumnMap(toRename);
    
		dm.invalidateFor(td, DependencyManager.RENAME, lcc);

		//look for foreign key dependency on the column.
		constraintDescriptorList = dd.getConstraintDescriptors(td);
		for(int index=0; index<constraintDescriptorList.size(); index++)
		{
			constraintDescriptor = constraintDescriptorList.elementAt(index);
			int[] referencedColumns = constraintDescriptor.getReferencedColumns();
			int numRefCols = referencedColumns.length;
			for (int j = 0; j < numRefCols; j++)
			{
				if ((referencedColumns[j] == columnPosition) &&
					(constraintDescriptor instanceof ReferencedKeyConstraintDescriptor))
					dm.invalidateFor(constraintDescriptor, DependencyManager.RENAME, lcc);
			}
		}

		// Drop the column
		dd.dropColumnDescriptor(td.getUUID(), oldObjectName, tc);
		columnDescriptor.setColumnName(newObjectName);
		dd.addDescriptor(columnDescriptor, td,
						 DataDictionary.SYSCOLUMNS_CATALOG_NUM, false, tc);

		//Need to do following to reload the cache so that table
		//descriptor now has new column name
		td = dd.getTableDescriptor(td.getObjectID());
	}

	//do necessary work for rename index at execute time.
	private void execGutsRenameIndex
	(
				   TableDescriptor td, Activation activation)
		throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();
		//for indexes, we only invalidate sps, rest we ignore(ie views)
		dm.invalidateFor(td, DependencyManager.RENAME_INDEX, lcc);

		ConglomerateDescriptor conglomerateDescriptor =
			dd.getConglomerateDescriptor(oldObjectName, sd, true);

		if (conglomerateDescriptor == null)
			throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION,
			oldObjectName);

		/* Drop the index descriptor */
		dd.dropConglomerateDescriptor(conglomerateDescriptor, tc);
		// Change the index name of the index descriptor
		conglomerateDescriptor.setConglomerateName(newObjectName);
		// add the index descriptor with new name
		dd.addDescriptor(conglomerateDescriptor, sd,
						 DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);
	}

	/* Following is used for error handling by repSourceCompilerUtilities
   * in it's method checkIfRenameDependency */
	public	String	getTableName()	{ return tableName; }

}
