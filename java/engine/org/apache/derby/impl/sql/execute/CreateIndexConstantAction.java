/*

   Derby - Class org.apache.derby.impl.sql.execute.CreateIndexConstantAction

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

import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.services.loader.ClassInspector;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;

import org.apache.derby.iapi.sql.execute.ConstantAction;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;

import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptorList;
import org.apache.derby.iapi.sql.dictionary.DataDescriptorGenerator;
import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.IndexRowGenerator;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.TypeId;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.RowLocationRetRowSource;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.SortController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;


import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.StatisticsImpl;

import java.util.Properties;
import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	CREATE TABLE Statement at Execution time.
 *
 *	@author Jeff Lichtman	Cribbed from from CreateTableConstantAction
 */

class CreateIndexConstantAction extends IndexConstantAction
{

	private boolean			unique;
	private String			indexType;
	private long			conglomId;
	private String[]		columnNames;
	private boolean[]		isAscending;
	private boolean			isConstraint;
	private UUID			conglomerateUUID;
	private Properties		properties;

	private ExecRow indexTemplateRow;


	// CONSTRUCTORS
	/**
	 *	Make the ConstantAction for a CREATE INDEX statement.
	 *
	 *  @param unique		True means it will be a unique index
	 *  @param indexType	The type of index (BTREE, for example)
	 *  @param sd			the schema that table (and index) lives in.
	 *  @param indexName	Name of the index
	 *  @param tableName	Name of table the index will be on
	 *  @param tableId		UUID of table
	 *  @param conglomId	Conglomerate ID of the index, if known in advance
	 *  @param columnNames	Names of the columns in the index, in order
	 *	@param isAscending	Array of booleans telling asc/desc on each column
	 *  @param isConstraint	TRUE if index is backing up a constraint, else FALSE
	 *  @param conglomerateUUID	ID of conglomerate
	 *  @param properties	The optional properties list associated with the index.
	 */
	CreateIndexConstantAction(
								boolean			unique,
								String			indexType,
								String			schemaName,
								String			indexName,
								String			tableName,
								UUID			tableId,
								long			conglomId,
								String[]		columnNames,
								boolean[]		isAscending,
								boolean			isConstraint,
								UUID			conglomerateUUID,
								Properties		properties)
	{
		super(tableId, indexName, tableName, schemaName);
		this.unique = unique;
		this.indexType = indexType;
		this.conglomId= conglomId;
		this.columnNames = columnNames;
		this.isAscending = isAscending;
		this.isConstraint = isConstraint;
		this.conglomerateUUID = conglomerateUUID;
		this.properties = properties;
	}

	///////////////////////////////////////////////
	//
	// OBJECT SHADOWS
	//
	///////////////////////////////////////////////

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		return "CREATE INDEX " + indexName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for CREATE INDEX.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		boolean						forCreateTable;
		TableDescriptor 			td;
		UUID 						toid;
		ColumnDescriptor			columnDescriptor;
		int[]						baseColumnPositions;
		IndexRowGenerator			indexRowGenerator = null;
		ExecRow[]					baseRows;
		ExecIndexRow[]				indexRows;
		ExecRow[]					compactBaseRows;
		GroupFetchScanController    scan;
		RowLocationRetRowSource	    rowSource;
		long						sortId;
		int							maxBaseColumnPosition = -1;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		TransactionController tc = lcc.getTransactionExecute();

		/* Remember whether or not we are doing a create table */
		forCreateTable = activation.getForCreateTable();

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

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true) ;


		/* Get the table descriptor. */
		/* See if we can get the TableDescriptor 
		 * from the Activation.  (Will be there
		 * for backing indexes.)
		 */
		td = activation.getDDLTableDescriptor();

		if (td == null)
		{
			/* tableId will be non-null if adding an index to
			 * an existing table (as opposed to creating a
			 * table with a constraint with a backing index).
			 */
			if (tableId != null)
			{
				td = dd.getTableDescriptor(tableId);
			}
			else
			{
				td = dd.getTableDescriptor(tableName, sd);
			}
		}

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_CREATE_INDEX_NO_TABLE, 
						indexName, tableName);
		}

		if (td.getTableType() == TableDescriptor.SYSTEM_TABLE_TYPE)
		{
			throw StandardException.newException(SQLState.LANG_CREATE_SYSTEM_INDEX_ATTEMPTED, 
						indexName, tableName);
		}

		/* Get a shared table lock on the table. We need to lock table before
		 * invalidate dependents, otherwise, we may interfere with the
		 * compilation/re-compilation of DML/DDL.  See beetle 4325 and $WS/
		 * docs/language/SolutionsToConcurrencyIssues.txt (point f).
		 */
		lockTableForDDL(tc, td.getHeapConglomerateId(), false);

		// invalidate any prepared statements that
		// depended on this table (including this one)
		if (! forCreateTable)
		{
			dm.invalidateFor(td, DependencyManager.CREATE_INDEX, lcc);
		}

		// Translate the base column names to column positions
		baseColumnPositions = new int[columnNames.length];
		for (int i = 0; i < columnNames.length; i++)
		{
			// Look up the column in the data dictionary
			columnDescriptor = td.getColumnDescriptor(columnNames[i]);
			if (columnDescriptor == null)
			{
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_FOUND_IN_TABLE, 
															columnNames[i],
															tableName);
			}

			TypeId typeId = columnDescriptor.getType().getTypeId();

			// Don't allow a column to be created on a non-orderable type
			ClassFactory cf = lcc.getLanguageConnectionFactory().getClassFactory();
			boolean isIndexable = typeId.orderable(cf);

			if (isIndexable && typeId.userType()) {
				String userClass = typeId.getCorrespondingJavaTypeName();

				// Don't allow indexes to be created on classes that
				// are loaded from the database. This is because recovery
				// won't be able to see the class and it will need it to
				// run the compare method.
				try {
					if (cf.isApplicationClass(cf.loadApplicationClass(userClass)))
						isIndexable = false;
				} catch (ClassNotFoundException cnfe) {
					// shouldn't happen as we just check the class is orderable
					isIndexable = false;
				}
			}

			if (!isIndexable) {
				throw StandardException.newException(SQLState.LANG_COLUMN_NOT_ORDERABLE_DURING_EXECUTION, 
					typeId.getSQLTypeName());
			}

			// Remember the position in the base table of each column
			baseColumnPositions[i] = columnDescriptor.getPosition();

			if (maxBaseColumnPosition < baseColumnPositions[i])
				maxBaseColumnPosition = baseColumnPositions[i];
		}

		// check if we have similar indices already for this table
		ConglomerateDescriptor[] congDescs = td.getConglomerateDescriptors();
		boolean duplicate = false;

		for (int i = 0; i < congDescs.length; i++)
		{
			ConglomerateDescriptor cd = congDescs[i];
			if ( ! cd.isIndex())
				continue;
			IndexRowGenerator irg = cd.getIndexDescriptor();
			int[] bcps = irg.baseColumnPositions();
			boolean[] ia = irg.isAscending();
			int j = 0;

			/* For an index to be considered a duplicate of already existing index, the
			 * following conditions have to be satisfied:
			 * 1. the set of columns (both key and include columns) and their 
			 *  order in the index is the same as that of an existing index AND 
			 * 2. the ordering attributes are the same AND 
			 * 3. both the previously existing index and the one being created 
			 *  are non-unique OR the previously existing index is unique
			 */

			if ((bcps.length == baseColumnPositions.length) &&
			    (irg.isUnique() || !unique) &&
				indexType.equals(irg.indexType()))
			{
				for (; j < bcps.length; j++)
				{
					if ((bcps[j] != baseColumnPositions[j]) || (ia[j] != isAscending[j]))
						break;
				}
			}

			if (j == baseColumnPositions.length)	// duplicate
			{
				/*
				 * Don't allow users to create a duplicate index. Allow if being done internally
				 * for a constraint
				 */
				if (!isConstraint)
				{
					activation.addWarning(
							StandardException.newWarning(
								SQLState.LANG_INDEX_DUPLICATE,
								cd.getConglomerateName()));

					return;
				}

				conglomId = cd.getConglomerateNumber();
				indexRowGenerator = cd.getIndexDescriptor();
				conglomerateUUID = cd.getUUID();
				duplicate = true;
				break;
			}
		}

		/* If this index already has an essentially same one, we share the
		 * conglomerate with the old one, and just simply add a descriptor
		 * entry into SYSCONGLOMERATES.
		 */
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();
		if (duplicate)
		{
			ConglomerateDescriptor cgd =
				ddg.newConglomerateDescriptor(conglomId, indexName, true,
										  indexRowGenerator, isConstraint,
										  conglomerateUUID, td.getUUID(), sd.getUUID() );
			dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);
			// add newly added conglomerate to the list of conglomerate 
			// descriptors in the td.
			ConglomerateDescriptorList cdl = 
				td.getConglomerateDescriptorList();
			cdl.add(cgd);

			// can't just return yet, need to get member "indexTemplateRow"
			// because create constraint may use it
		}

		// Describe the properties of the index to the store using Properties
		// RESOLVE: The following properties assume a BTREE index.
		Properties	indexProperties;
		
		if (properties != null)
		{
			indexProperties = properties;
		}
		else
		{
			indexProperties = new Properties();
		}

		// Tell it the conglomerate id of the base table
		indexProperties.put("baseConglomerateId",
							Long.toString(td.getHeapConglomerateId()));

		// All indexes are unique because they contain the RowLocation.
		// The number of uniqueness columns must include the RowLocation
		// if the user did not specify a unique index.
		indexProperties.put("nUniqueColumns",
					Integer.toString(unique ? baseColumnPositions.length :
												baseColumnPositions.length + 1)
							);

		// By convention, the row location column is the last column
		indexProperties.put("rowLocationColumn",
							Integer.toString(baseColumnPositions.length));

		// For now, all columns are key fields, including the RowLocation
		indexProperties.put("nKeyFields",
							Integer.toString(baseColumnPositions.length + 1));

		// For now, assume that all index columns are ordered columns
		if (! duplicate)
		{
			indexRowGenerator = new IndexRowGenerator(indexType, unique,
													baseColumnPositions,
													isAscending,
													baseColumnPositions.length);
		}

		/* Now add the rows from the base table to the conglomerate.
		 * We do this by scanning the base table and inserting the
		 * rows into a sorter before inserting from the sorter
		 * into the index.  This gives us better performance
		 * and a more compact index.
		 */

		rowSource = null;
		sortId = 0;
		boolean needToDropSort = false;	// set to true once the sorter is created

		/* bulkFetchSIze will be 16 (for now) unless
		 * we are creating the table in which case it
		 * will be 1.  Too hard to remove scan when
		 * creating index on new table, so minimize
		 * work where we can.
		 */
		int bulkFetchSize = (forCreateTable) ? 1 : 16;	
		int numColumns = td.getNumberOfColumns();
		int approximateRowSize = 0;

		// Create the FormatableBitSet for mapping the partial to full base row
		FormatableBitSet bitSet = new FormatableBitSet(numColumns+1);
		for (int index = 0; index < baseColumnPositions.length; index++)
		{
			bitSet.set(baseColumnPositions[index]);
		}
		FormatableBitSet zeroBasedBitSet = RowUtil.shift(bitSet, 1);

		// Start by opening a full scan on the base table.
		scan = tc.openGroupFetchScan(
                            td.getHeapConglomerateId(),
							false,	// hold
							0,	// open base table read only
                            TransactionController.MODE_TABLE,
                            TransactionController.ISOLATION_SERIALIZABLE,
							zeroBasedBitSet,    // all fields as objects
							(DataValueDescriptor[]) null,	// startKeyValue
							0,		// not used when giving null start posn.
							null,	// qualifier
							(DataValueDescriptor[]) null,	// stopKeyValue
							0);		// not used when giving null stop posn.

		// Create an array to put base row template
		baseRows = new ExecRow[bulkFetchSize];
		indexRows = new ExecIndexRow[bulkFetchSize];
		compactBaseRows = new ExecRow[bulkFetchSize];

		try
		{
			// Create the array of base row template
			for (int i = 0; i < bulkFetchSize; i++)
			{
				// create a base row template
				baseRows[i] = activation.getExecutionFactory().getValueRow(maxBaseColumnPosition);

				// create an index row template
				indexRows[i] = indexRowGenerator.getIndexRowTemplate();

				// create a compact base row template
				compactBaseRows[i] = activation.getExecutionFactory().getValueRow(
													baseColumnPositions.length);
			}

			indexTemplateRow = indexRows[0];

			// Fill the partial row with nulls of the correct type
			ColumnDescriptorList cdl = td.getColumnDescriptorList();
			int					 cdlSize = cdl.size();
			for (int index = 0, numSet = 0; index < cdlSize; index++)
			{
				if (! zeroBasedBitSet.get(index))
				{
					continue;
				}
				numSet++;
				ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
				DataTypeDescriptor dts = cd.getType();


				for (int i = 0; i < bulkFetchSize; i++)
				{
					// Put the column in both the compact and sparse base rows
					baseRows[i].setColumn(index + 1,
								  dts.getNull());
					compactBaseRows[i].setColumn(numSet,
								  baseRows[i].getColumn(index + 1));
				}

				// Calculate the approximate row size for the index row
				approximateRowSize += dts.getTypeId().getApproximateLengthInBytes(dts);
			}

			// Get an array of RowLocation template
			RowLocation rl[] = new RowLocation[bulkFetchSize];
			for (int i = 0; i < bulkFetchSize; i++)
			{
				rl[i] = scan.newRowLocationTemplate();

				// Get an index row based on the base row
				indexRowGenerator.getIndexRow(compactBaseRows[i], rl[i], indexRows[i], bitSet);
			}

			/* now that we got indexTemplateRow, done for duplicate index
			 */
			if (duplicate)
				return;

			/* For non-unique indexes, we order by all columns + the RID.
			 * For unique indexes, we just order by the columns.
			 * We create a unique index observer for unique indexes
			 * so that we can catch duplicate key.
			 * We create a basic sort observer for non-unique indexes
			 * so that we can reuse the wrappers during an external
			 * sort.
			 */
			int numColumnOrderings;
			SortObserver sortObserver = null;
			if (unique)
			{
				numColumnOrderings = baseColumnPositions.length;
				// if the index is a constraint, use constraintname in possible error messagge
				String indexOrConstraintName = indexName;
				if  (conglomerateUUID != null)
				{
					ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateUUID);
					if ((isConstraint) && (cd != null && cd.getUUID() != null && td != null))
					{
						ConstraintDescriptor conDesc = dd.getConstraintDescriptor(td,
                                                                      cd.getUUID());
						indexOrConstraintName = conDesc.getConstraintName();
					}
				}
				sortObserver = new UniqueIndexSortObserver(true, isConstraint, 
														   indexOrConstraintName,
														   indexTemplateRow,
														   true,
														   td.getName());
			}
			else
			{
				numColumnOrderings = baseColumnPositions.length + 1;
				sortObserver = new BasicSortObserver(true, false, 
													 indexTemplateRow,
													 true);
			}
			ColumnOrdering[]	order = new ColumnOrdering[numColumnOrderings];
			for (int i=0; i < numColumnOrderings; i++) 
			{
				order[i] = new IndexColumnOrder(i, unique || i < numColumnOrderings - 1 
													? isAscending[i] : true);
			}

			// create the sorter
			sortId = tc.createSort((Properties)null, 
					indexTemplateRow.getRowArrayClone(),
					order,
					sortObserver,
					false,			// not in order
					scan.getEstimatedRowCount(),
					approximateRowSize	// est row size, -1 means no idea	
					);

			needToDropSort = true;

			// Populate sorter and get the output of the sorter into a row
			// source.  The sorter has the indexed columns only and the columns
			// are in the correct order. 
			rowSource = loadSorter(baseRows, indexRows, tc,
								   scan, sortId, rl);

			conglomId = tc.createAndLoadConglomerate(
					indexType,
					indexTemplateRow.getRowArray(),	// index row template
					order, //colums sort order
					indexProperties,
					TransactionController.IS_DEFAULT, // not temporary
					rowSource,
					(long[]) null);
			
		}
		finally
		{

			/* close the table scan */
			if (scan != null)
				scan.close();

			/* close the sorter row source before throwing exception */
			if (rowSource != null)
				rowSource.closeRowSource();

			/*
			** drop the sort so that intermediate external sort run can be
			** removed from disk
			*/
			if (needToDropSort)
			 	tc.dropSort(sortId);
		}

		ConglomerateController indexController =
			tc.openConglomerate(
                conglomId, false, 0, TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_SERIALIZABLE);

		// Check to make sure that the conglomerate can be used as an index
		if ( ! indexController.isKeyed())
		{
			indexController.close();
			throw StandardException.newException(SQLState.LANG_NON_KEYED_INDEX, indexName,
														   indexType);
		}
		indexController.close();

		//
		// Create a conglomerate descriptor with the conglomId filled in and
		// add it.
		//

		ConglomerateDescriptor cgd =
			ddg.newConglomerateDescriptor(conglomId, indexName, true,
										  indexRowGenerator, isConstraint,
										  conglomerateUUID, td.getUUID(), sd.getUUID() );

		dd.addDescriptor(cgd, sd, DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);

		// add newly added conglomerate to the list of conglomerate descriptors
		// in the td.
		ConglomerateDescriptorList cdl = td.getConglomerateDescriptorList();
		cdl.add(cgd);

		CardinalityCounter cCount = (CardinalityCounter)rowSource;
		long numRows;
		if ((numRows = cCount.getRowCount()) > 0)
		{
			long[] c = cCount.getCardinality();
			for (int i = 0; i < c.length; i++)
			{
				StatisticsDescriptor statDesc = 
					new StatisticsDescriptor(dd, dd.getUUIDFactory().createUUID(),
												cgd.getUUID(), td.getUUID(), "I", new StatisticsImpl(numRows, c[i]),
												i + 1);
				dd.addDescriptor(statDesc, null, 
								 DataDictionary.SYSSTATISTICS_CATALOG_NUM,
								 true, tc);
			}
		}
	}

	// CLASS METHODS
	
	///////////////////////////////////////////////////////////////////////
	//
	//	GETTERs called by CreateConstraint
	//
	///////////////////////////////////////////////////////////////////////
	ExecRow getIndexTemplateRow()
	{
		return indexTemplateRow;
	}

	/**
	 * Do necessary clean up (close down controllers, etc.) before throwing
	 * a statement exception.
	 *
	 * @param scan				ScanController for the heap
	 * @param indexController	ConglomerateController for the index
	 *
	 * @return Nothing.
	 */
	private void statementExceptionCleanup(
					ScanController scan, 
					ConglomerateController indexController)
        throws StandardException
	{
		if (indexController != null)
		{
			indexController.close();
		}
		if (scan != null)
		{
			scan.close();
		}
	}

	/**
	 * Scan the base conglomerate and insert the keys into a sorter,
	 * returning a rowSource on the sorter. 
	 *
	 * @return RowSource on the sorted index keys.
	 *
	 * @exception StandardException					thrown on error
	 */
	private RowLocationRetRowSource loadSorter(ExecRow[] baseRows,
								               ExecIndexRow[] indexRows, 
								               TransactionController tc,
								               GroupFetchScanController scan,
								               long sortId,
								               RowLocation rl[])
		throws StandardException
	{
		SortController		sorter;
		long				rowCount = 0;

		sorter = tc.openSort(sortId);

		try
		{
			// Step through all the rows in the base table
			// prepare an array or rows for bulk fetch
			int bulkFetchSize = baseRows.length;

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(bulkFetchSize == indexRows.length, 
					"number of base rows and index rows does not match");
				SanityManager.ASSERT(bulkFetchSize == rl.length,
					"number of base rows and row locations does not match");
			}

			DataValueDescriptor[][] baseRowArray = new DataValueDescriptor[bulkFetchSize][];

			for (int i = 0; i < bulkFetchSize; i++)
				baseRowArray[i] = baseRows[i].getRowArray();

			// rl[i] and baseRowArray[i] and indexRows[i] are all tied up
			// beneath the surface.  Fetching the base row and row location
			// from the table scan will automagically set up the indexRow
			// fetchNextGroup will return how many rows are actually fetched.
			int bulkFetched = 0;

			while ((bulkFetched = scan.fetchNextGroup(baseRowArray, rl)) > 0)
			{
				for (int i = 0; i < bulkFetched; i++)
				{
					sorter.insert(indexRows[i].getRowArray());
					rowCount++;
				}
			}

			/*
			** We've just done a full scan on the heap, so set the number
			** of rows so the optimizer will have an accurate count.
			*/
			scan.setEstimatedRowCount(rowCount);
		}
		finally
		{
			sorter.close();
		}

		return new CardinalityCounter(tc.openSortRowSource(sortId));
	}
}

