/*

   Derby - Class org.apache.derby.iapi.db.ConsistencyChecker

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

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.error.PublicAPI;

import org.apache.derby.iapi.sql.dictionary.DataDictionaryContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptor;
import org.apache.derby.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.derby.iapi.sql.dictionary.ConglomerateDescriptor;

import org.apache.derby.iapi.sql.depend.DependencyManager;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;


import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.sql.SQLException;

/**
 * The ConsistencyChecker class provides static methods for verifying
 * the consistency of the data stored within a database.
 * 
 *
   <p>This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
   <p>This class can be accessed using the class alias <code> CONSISTENCYCHECKER </code> in SQL-J statements.
 * <p>
 * <I>IBM Corp. reserves the right to change, rename, or
 * remove this class or any of the the methods on the
 * class at any time.</I>
 */
public class ConsistencyChecker
{

	/** no requirement for a constructor */
	private ConsistencyChecker() {
	}

	/**
	 * Check the named table, ensuring that all of its indexes are consistent
	 * with the base table.
	 * Use this
	 *  method only within an SQL-J statement; do not call it directly.
	 * <P>When tables are consistent, the method returns true. Otherwise, the method throws an exception.
	 * <p>To check the consistency of a single table:
	 * <p><code>
	 * VALUES ConsistencyChecker::checkTable(<i>SchemaName</i>, <i>TableName</i>)</code></p>
	 * <P>For example, to check the consistency of the table <i>APP.Flights</i>:
	 * <p><code>
	 * VALUES ConsistencyChecker::checkTable('APP', 'FLIGHTS')</code></p>
	 * <p>To check the consistency of all of the tables in the 'APP' schema,
	 * stopping at the first failure: 
	 *
	 * <P><code>SELECT tablename, ConsistencyChecker::checkTable(<br>
	 * 'APP', tablename)<br>
	 * FROM sys.sysschemas s, sys.systables t
	 * WHERE s.schemaname = 'APP' AND s.schemaid = t.schemaid</code>
	 *
	 * <p> To check the consistency of an entire database, stopping at the first failure:
	 *
	 * <p><code>SELECT schemaname, tablename,<br>
	 * ConsistencyChecker::checkTable(schemaname, tablename)<br>
	 * FROM sys.sysschemas s, sys.systables t<br>
	 * WHERE s.schemaid = t.schemaid</code>
	 *
	 *
	 *
	 * @param schemaName	The schema name of the table.
	 * @param tableName		The name of the table
	 *
	 * @return	true, if the table is consistent, exception thrown if inconsistent
	 *
	 * @exception	SQLException	Thrown if some inconsistency
	 *									is found, or if some unexpected
	 *									exception is thrown..
	 */
	public static boolean checkTable(String schemaName, String tableName)
						throws SQLException
	{
		DataDictionary			dd;
		TableDescriptor			td;
		long					baseRowCount = -1;
		TransactionController	tc;
		ConglomerateDescriptor	heapCD;
		ConglomerateDescriptor	indexCD;
		ExecRow					baseRow;
		ExecRow					indexRow;
		RowLocation				rl = null;
		RowLocation				scanRL = null;
		ScanController			scan = null;
		int[]					baseColumnPositions;
		int						baseColumns = 0;
		DataValueFactory		dvf;
		long					indexRows;
		ConglomerateController	baseCC = null;
		ConglomerateController	indexCC = null;
		ExecutionContext		ec;
		SchemaDescriptor		sd;
		ConstraintDescriptor	constraintDesc;

		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
		tc = lcc.getTransactionExecute();

		try {

            dd = lcc.getDataDictionary();

            dvf = lcc.getDataValueFactory();

            ec = lcc.getExecutionContext() ;

            sd = dd.getSchemaDescriptor(schemaName, tc, true);
            td = dd.getTableDescriptor(tableName, sd);

            if (td == null)
            {
                throw StandardException.newException(
                    SQLState.LANG_TABLE_NOT_FOUND, 
                    schemaName + "." + tableName);
            }

            /* Skip views */
            if (td.getTableType() == TableDescriptor.VIEW_TYPE)
            {
                return true;
            }

			/* Open the heap for reading */
			baseCC = tc.openConglomerate(
			            td.getHeapConglomerateId(), false, 0, 
				        TransactionController.MODE_TABLE,
					    TransactionController.ISOLATION_SERIALIZABLE);

			/* Check the consistency of the heap */
			baseCC.checkConsistency();

			heapCD = td.getConglomerateDescriptor(td.getHeapConglomerateId());

			/* Get a row template for the base table */
			baseRow = ec.getExecutionFactory().getValueRow(td.getNumberOfColumns());

			/* Fill the row with nulls of the correct type */
			ColumnDescriptorList cdl = td.getColumnDescriptorList();
			int					 cdlSize = cdl.size();

			for (int index = 0; index < cdlSize; index++)
			{
				ColumnDescriptor cd = (ColumnDescriptor) cdl.elementAt(index);
				baseRow.setColumn(cd.getPosition(),
										cd.getType().getNull());
			}

			/* Look at all the indexes on the table */
			ConglomerateDescriptor[] cds = td.getConglomerateDescriptors();
			for (int index = 0; index < cds.length; index++)
			{
				indexCD = cds[index];
				/* Skip the heap */
				if ( ! indexCD.isIndex())
					continue;

				/* Check the internal consistency of the index */
				indexCC = 
			        tc.openConglomerate(
				        indexCD.getConglomerateNumber(),
                        false,
					    0,
						TransactionController.MODE_TABLE,
	                    TransactionController.ISOLATION_SERIALIZABLE);

				indexCC.checkConsistency();
				indexCC.close();
				indexCC = null;

				/* if index is for a constraint check that the constraint exists */

				if (indexCD.isConstraint())
				{
					constraintDesc = dd.getConstraintDescriptor(td, indexCD.getUUID());
					if (constraintDesc == null)
					{
						throw StandardException.newException(
										SQLState.LANG_OBJECT_NOT_FOUND,
										"CONSTRAINT for INDEX",
										indexCD.getConglomerateName());
					}
				}

				/*
				** Set the base row count when we get to the first index.
				** We do this here, rather than outside the index loop, so
				** we won't do the work of counting the rows in the base table
				** if there are no indexes to check.
				*/
				if (baseRowCount < 0)
				{
					scan = tc.openScan(heapCD.getConglomerateNumber(),
										false,	// hold
										0,		// not forUpdate
									    TransactionController.MODE_TABLE,
									    TransactionController.ISOLATION_SERIALIZABLE,
                                        RowUtil.EMPTY_ROW_BITSET,
										null,	// startKeyValue
										0,		// not used with null start posn.
										null,	// qualifier
										null,	// stopKeyValue
										0);		// not used with null stop posn.

					/* Also, get the row location template for index rows */
					rl = scan.newRowLocationTemplate();
					scanRL = scan.newRowLocationTemplate();

					for (baseRowCount = 0; scan.next(); baseRowCount++)
						;	/* Empty statement */

					scan.close();
					scan = null;
				}

				baseColumnPositions =
						indexCD.getIndexDescriptor().baseColumnPositions();
				baseColumns = baseColumnPositions.length;

				FormatableBitSet indexColsBitSet = new FormatableBitSet();
				for (int i = 0; i < baseColumns; i++)
				{
					indexColsBitSet.grow(baseColumnPositions[i]);
					indexColsBitSet.set(baseColumnPositions[i] - 1);
				}

				/* Get one row template for the index scan, and one for the fetch */
				indexRow = ec.getExecutionFactory().getValueRow(baseColumns + 1);

				/* Fill the row with nulls of the correct type */
				for (int column = 0; column < baseColumns; column++)
				{
					/* Column positions in the data dictionary are one-based */
 					ColumnDescriptor cd = td.getColumnDescriptor(baseColumnPositions[column]);
					indexRow.setColumn(column + 1,
											cd.getType().getNull());
				}

				/* Set the row location in the last column of the index row */
				indexRow.setColumn(baseColumns + 1, rl);

				/* Do a full scan of the index */
				scan = tc.openScan(indexCD.getConglomerateNumber(),
									false,	// hold
									0,		// not forUpdate
								    TransactionController.MODE_TABLE,
						            TransactionController.ISOLATION_SERIALIZABLE,
									(FormatableBitSet) null,
									null,	// startKeyValue
									0,		// not used with null start posn.
									null,	// qualifier
									null,	// stopKeyValue
									0);		// not used with null stop posn.

				DataValueDescriptor[] baseRowIndexOrder = 
                    new DataValueDescriptor[baseColumns];
				DataValueDescriptor[] baseObjectArray = baseRow.getRowArray();

				for (int i = 0; i < baseColumns; i++)
				{
					baseRowIndexOrder[i] = baseObjectArray[baseColumnPositions[i] - 1];
				}
			
				/* Get the index rows and count them */
				for (indexRows = 0; scan.next(); indexRows++)
				{
					RowLocation	baseRL;

					/* Get the index row */
					scan.fetch(indexRow.getRowArray());

					/*
					** Get the base row using the RowLocation in the index row,
					** which is in the last column.  
					*/
					baseRL = (RowLocation) indexRow.getColumn(baseColumns + 1);

					boolean base_row_exists = 
		                baseCC.fetch(
			                baseRL, baseObjectArray, indexColsBitSet);

					/* Throw exception if fetch() returns false */
					if (! base_row_exists)
					{
						String indexName = indexCD.getConglomerateName();
						throw StandardException.newException(SQLState.LANG_INCONSISTENT_ROW_LOCATION, 
									(schemaName + "." + tableName),
									indexName, 
									baseRL.toString(),
									indexRow.toString());
					}

					/* Compare all the column values */
					for (int column = 0; column < baseColumns; column++)
					{
						DataValueDescriptor indexColumn =
							indexRow.getColumn(column + 1);
						DataValueDescriptor baseColumn =
							baseRowIndexOrder[column];

						/*
						** With this form of compare(), null is considered equal
						** to null.
						*/
						if (indexColumn.compare(baseColumn) != 0)
						{
							ColumnDescriptor cd = 
                                td.getColumnDescriptor(
                                    baseColumnPositions[column]);

                            /*
                            System.out.println(
                                "SQLState.LANG_INDEX_COLUMN_NOT_EQUAL:" +
                                "indexCD.getConglomerateName()" + indexCD.getConglomerateName() +
                                ";td.getSchemaName() = " + td.getSchemaName() +
                                ";td.getName() = " + td.getName() +
                                ";baseRL.toString() = " + baseRL.toString() +
                                ";cd.getColumnName() = " + cd.getColumnName() +
                                ";indexColumn.toString() = " + indexColumn.toString() +
                                ";baseColumn.toString() = " + baseColumn.toString() +
                                ";indexRow.toString() = " + indexRow.toString());
                            */

							throw StandardException.newException(
                                SQLState.LANG_INDEX_COLUMN_NOT_EQUAL, 
                                indexCD.getConglomerateName(),
                                td.getSchemaName(),
                                td.getName(),
                                baseRL.toString(),
                                cd.getColumnName(),
                                indexColumn.toString(),
                                baseColumn.toString(),
                                indexRow.toString());
						}
					}
				}

				/* Clean up after the index scan */
				scan.close();
				scan = null;

				/*
				** The index is supposed to have the same number of rows as the
				** base conglomerate.
				*/
				if (indexRows != baseRowCount)
				{
					throw StandardException.newException(SQLState.LANG_INDEX_ROW_COUNT_MISMATCH, 
										indexCD.getConglomerateName(),
										td.getSchemaName(),
										td.getName(),
										Long.toString(indexRows),
										Long.toString(baseRowCount));
				}
			}
			/* check that all constraints have backing index */
			ConstraintDescriptorList constraintDescList = 
				dd.getConstraintDescriptors(td);
			for (int index = 0; index < constraintDescList.size(); index++)
			{
				constraintDesc = constraintDescList.elementAt(index);
				if (constraintDesc.hasBackingIndex())
				{
					ConglomerateDescriptor conglomDesc;

					conglomDesc = td.getConglomerateDescriptor(
							constraintDesc.getConglomerateId());
					if (conglomDesc == null)
					{
						throw StandardException.newException(
										SQLState.LANG_OBJECT_NOT_FOUND,
										"INDEX for CONSTRAINT",
										constraintDesc.getConstraintName());
					}
				}
			}
			
		}
		catch (StandardException se)
		{
			throw PublicAPI.wrapStandardException(se);
		}
		finally
		{
            try
            {
                /* Clean up before we leave */
                if (baseCC != null)
                {
                    baseCC.close();
                    baseCC = null;
                }
                if (indexCC != null)
                {
                    indexCC.close();
                    indexCC = null;
                }
                if (scan != null)
                {
                    scan.close();
                    scan = null;
                }
            }
            catch (StandardException se)
            {
                throw PublicAPI.wrapStandardException(se);
            }
		}

		return true;
	}
}
