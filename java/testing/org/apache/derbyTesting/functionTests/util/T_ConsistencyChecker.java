/*

   Derby - Class org.apache.derbyTesting.functionTests.util.T_ConsistencyChecker

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util;


import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * This class has methods for corrupting a database. 
 * IT MUST NOT BE DISTRIBUTED WITH THE PRODUCT.
 *
 * NOTE: The entry points to this class are all static,
 * for easy access via the query language.  Each of the
 * static methods instantiates an object from the class
 * and calls methods off of that object.  This allows
 * the sharing of code across the various static methods.
 */
public class T_ConsistencyChecker
{
	private	DataDictionary				dd;
	private	TransactionController		tc;
	private	LanguageConnectionContext	lcc;
	private	DataValueFactory			dvf;
	private	ExecutionContext			ec;
	private String						indexName;
	private String						schemaName;
	private String						tableName;
	private ConglomerateDescriptor		id;
	private SchemaDescriptor			sd;
	private	TableDescriptor				td;

	T_ConsistencyChecker(String schemaName, String tableName, String indexName)
		throws StandardException
	{
		this.schemaName = schemaName;
		this.tableName = tableName;
		this.indexName = indexName;
	}

	/**
	 * Delete the first row from the heap, without
	 * deleting it from the indexes on the table.
	 *
	 * @param schemaName	The schema name.
	 * @param tableName		The table name.
	 * 
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static void deleteFirstHeapRow(String schemaName, String tableName)
		throws StandardException
	{
		T_ConsistencyChecker t_cc = new T_ConsistencyChecker(schemaName, tableName, null);
		t_cc.getContexts();
		t_cc.getDescriptors();

		/* Open a scan on the heap */
		ScanController heapScan = t_cc.openUnqualifiedHeapScan();

		// Move to the 1st row in the heap
		heapScan.next();

		// Delete the 1st row in the heap
		heapScan.delete();

		heapScan.close();
	}

	/**
	 * Get the first row from the heap and insert it into
	 * the heap again, without
	 * inserting it from the indexes on the table.
	 *
	 * @param schemaName	The schema name.
	 * @param tableName		The table name.
	 * 
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static void reinsertFirstHeapRow(String schemaName, String tableName)
		throws StandardException
	{
		T_ConsistencyChecker t_cc = new T_ConsistencyChecker(schemaName, tableName, null);
		t_cc.getContexts();
		t_cc.getDescriptors();

		/* Open a scan on the heap */
		ScanController heapScan = t_cc.openUnqualifiedHeapScan();

		// Move to the 1st row in the heap
		heapScan.next();

		// Fetch the 1st row
		ExecRow firstRow = t_cc.getHeapRowOfNulls();
		heapScan.fetch(firstRow.getRowArray());
		heapScan.close();

		// Insert another copy of the 1st row into the heap
		ConglomerateController heapCC = t_cc.openHeapCC();
		heapCC.insert(firstRow.getRowArray());
		heapCC.close();
	}

	/**
	 * Set all of the columns in the first row from 
	 * the heap to null, without
	 * updating the indexes on the table.
	 *
	 * @param schemaName	The schema name.
	 * @param tableName		The table name.
	 * 
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static void nullFirstHeapRow(String schemaName, String tableName)
		throws StandardException
	{
		T_ConsistencyChecker t_cc = new T_ConsistencyChecker(schemaName, tableName, null);
		t_cc.getContexts();
		t_cc.getDescriptors();

		/* Open a scan on the heap */
		ScanController heapScan = t_cc.openUnqualifiedHeapScan();

		// Move to the 1st row in the heap
		heapScan.next();

		// Get the RowLocation
		RowLocation baseRL = heapScan.newRowLocationTemplate();
		heapScan.fetchLocation(baseRL);

		// Replace the current row with nulls
		heapScan.replace(
			t_cc.getHeapRowOfNulls().getRowArray(),
			(FormatableBitSet) null);

		heapScan.close();
	}

	/**
	 * Get the first row from the heap and insert it into
	 * the specified index, with a bad row location, without
	 * inserting it into the heap or the other indexes on the table.
	 *
	 * @param schemaName	The schema name.
	 * @param tableName		The table name.
	 * @param indexName		The specified index.
	 * 
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static void insertBadRowLocation(String schemaName, String tableName, String indexName)
		throws StandardException
	{
		T_ConsistencyChecker t_cc = new T_ConsistencyChecker(schemaName, tableName, indexName);
		t_cc.getContexts();
		t_cc.getDescriptors();


		/* Open a scan on the heap */
		ScanController heapScan = t_cc.openUnqualifiedHeapScan();

		// Get the RowLocation
		RowLocation baseRL = heapScan.newRowLocationTemplate();
		RowLocation badRL = heapScan.newRowLocationTemplate();
		heapScan.close();

		/* Open a scan on the index */
		ExecRow		indexRow = t_cc.getIndexTemplateRow(baseRL);
		ScanController indexScan = t_cc.openUnqualifiedIndexScan();

		// Move to the 1st row in the index
		indexScan.next();

		// Fetch the 1st row
		indexScan.fetch(indexRow.getRowArray());
		indexScan.close();

		// Insert another copy of the 1st row into the index with a bad row location
		int keyLength = 
				t_cc.getIndexDescriptor().getIndexDescriptor().baseColumnPositions().length;
		indexRow.setColumn(keyLength + 1, badRL);

		ConglomerateController indexCC = t_cc.openIndexCC();
		indexCC.insert(indexRow.getRowArray());
		indexCC.close();
	}

	/**
	 * Swap the values in the specified columns of the
	 * first row from the heap, without
	 * updating the indexes on the table.
	 *
	 * @param schemaName	The schema name.
	 * @param tableName		The table name.
	 * @param firstColumn	First column #.
	 * @param secondColumn	Second column #.
	 * 
	 * @return Nothing.
	 *
	 * @exception StandardException		Thrown on error
	 */
	public static void swapColumnsInFirstHeapRow(String schemaName, String tableName,
												 int firstColumn, int secondColumn)
		throws StandardException
	{
	}

	/* Get the various contexts */
	private void getContexts()
		throws StandardException
	{
		lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		tc = lcc.getTransactionExecute();

		dd = lcc.getDataDictionary();

		dvf = lcc.getDataValueFactory();

		ec = (ExecutionContext) 
				(ContextService.getContext(ExecutionContext.CONTEXT_ID));
	}

	/* Get the various descriptors */
	private void getDescriptors()
		throws StandardException
	{
		sd = dd.getSchemaDescriptor(schemaName, tc, true);
		td = dd.getTableDescriptor(tableName, sd);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND, schemaName + "." + tableName);
		}

		if (indexName != null)
		{
			id = dd.getConglomerateDescriptor(indexName, sd, true);
			if (id == null)
			{
				throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, indexName);
			}
		}
	}

	/* Get a heap row full of nulls */
	private ExecRow getHeapRowOfNulls()
		throws StandardException
	{
		ConglomerateController	baseCC;
		ExecRow					baseRow;

		/* Open the heap for reading */
		baseCC = tc.openConglomerate(
		            td.getHeapConglomerateId(), false, 0, 
			        TransactionController.MODE_TABLE,
				    TransactionController.ISOLATION_SERIALIZABLE);

		/* Get a row template for the base table */
		baseRow = ec.getExecutionFactory().getValueRow(td.getNumberOfColumns());

		/* Fill the row with nulls of the correct type */
		ColumnDescriptorList cdl = td.getColumnDescriptorList();
		int					 cdlSize = cdl.size();

		for (int index = 0; index < cdlSize; index++)
		{
			ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
			DataTypeDescriptor dts = cd.getType();
			baseRow.setColumn(cd.getPosition(),
									dts.getNull());
		}

		baseCC.close();
		return baseRow;
	}

	/* Open an unqualified scan on the heap for update */
	private ScanController openUnqualifiedHeapScan()
		throws StandardException
	{
		ScanController heapScan;

		heapScan = tc.openScan(td.getHeapConglomerateId(),
								false,	// hold
								TransactionController.OPENMODE_FORUPDATE,		//  forUpdate
								TransactionController.MODE_TABLE,
						        TransactionController.ISOLATION_SERIALIZABLE,
								(FormatableBitSet) null,
								null,	// startKeyValue
								0,		// not used with null start posn.
								null,	// qualifier
								null,	// stopKeyValue
								0);		// not used with null stop posn.

		return heapScan;
	}

	/* Open the heap conglomerate for update */
	private ConglomerateController openHeapCC()
		throws StandardException
	{
		ConglomerateController heapCC;

		heapCC = tc.openConglomerate(
			            td.getHeapConglomerateId(),
                        false,
						TransactionController.OPENMODE_FORUPDATE,		//  forUpdate
				        TransactionController.MODE_TABLE,
					    TransactionController.ISOLATION_SERIALIZABLE);


		return heapCC;
	}

	/* Get a template row for the specified index */
	private ExecRow getIndexTemplateRow(RowLocation baseRL)
		throws StandardException
	{
		int[]					baseColumnPositions;
		int						baseColumns = 0;
		ExecRow					indexScanTemplate;

		baseColumnPositions =
				id.getIndexDescriptor().baseColumnPositions();
		baseColumns = baseColumnPositions.length;

		FormatableBitSet indexColsBitSet = new FormatableBitSet();
		for (int i = 0; i < baseColumns; i++)
		{
			indexColsBitSet.grow(baseColumnPositions[i]);
			indexColsBitSet.set(baseColumnPositions[i] - 1);
		}

		/* Get a row template */
		indexScanTemplate = ec.getExecutionFactory().getValueRow(baseColumns + 1);

		/* Fill the row with nulls of the correct type */
		for (int column = 0; column < baseColumns; column++)
		{
			/* Column positions in the data dictionary are one-based */
			ColumnDescriptor cd = td.getColumnDescriptor(baseColumnPositions[column]);
			DataTypeDescriptor dts = cd.getType();
			indexScanTemplate.setColumn(column + 1,
									dts.getNull());
		}

		/* Set the row location in the last column of the index row */
		indexScanTemplate.setColumn(baseColumns + 1, baseRL);

		return indexScanTemplate;
	}

	/* Open an unqualified scan on the index for update */
	private ScanController openUnqualifiedIndexScan()
		throws StandardException
	{
		ScanController indexScan;

		indexScan = tc.openScan(id.getConglomerateNumber(),
								false,	// hold
								TransactionController.OPENMODE_FORUPDATE,		//  forUpdate
								TransactionController.MODE_TABLE,
						        TransactionController.ISOLATION_SERIALIZABLE,
								(FormatableBitSet) null,
								null,	// startKeyValue
								0,		// not used with null start posn.
								null,	// qualifier
								null,	// stopKeyValue
								0);		// not used with null stop posn.

		return indexScan;
	}

	/* Open the index conglomerate for update */
	private ConglomerateController openIndexCC()
		throws StandardException
	{
		ConglomerateController indexCC;

		indexCC = tc.openConglomerate(
			            id.getConglomerateNumber(),
                        false,
						TransactionController.OPENMODE_FORUPDATE,		//  forUpdate
				        TransactionController.MODE_TABLE,
					    TransactionController.ISOLATION_SERIALIZABLE);


		return indexCC;
	}

	/* Return the ConglomerateDescriptor for the index */
	private ConglomerateDescriptor getIndexDescriptor()
	{
		return id;
	}
}
