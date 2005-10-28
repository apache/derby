/*

   Derby - Class org.apache.derby.impl.sql.execute.UpdateStatisticsConstantAction

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

import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.StatisticsDescriptor;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.catalog.UUID;
import org.apache.derby.catalog.types.StatisticsImpl;
import org.apache.derby.iapi.sql.depend.DependencyManager;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * This class describes actions that are performed for an 
 * UPDATE STATISTICS Statement at execution time.
 */

class UpdateStatisticsConstantAction extends DDLConstantAction
{
	private UUID tableUUID;
	private UUID[] objectUUID;
	private String objectName;
	private boolean forTable;
	private long[] conglomerateNumber;
	private ExecIndexRow[] indexRow;

	/* RUNTIME state of the system is maintained in these objects.
	 * rowBufferOne simply reuses the index row prepared by
	 * makeConstantAction. rowBufferTwo is a clone (an extra copy) of
	 * objects. rowBufferCurrent just switches between rowBufferOne and
	 * rowBufferTwo. 
	 */
	private DataValueDescriptor[][] rowBufferArray;
	private DataValueDescriptor[] rowBuffer;
	private DataValueDescriptor[] lastUniqueKey;

	private static final int GROUP_FETCH_SIZE = 16;

	public UpdateStatisticsConstantAction() {};

	public UpdateStatisticsConstantAction(boolean forTable,
										  String objectName,
										  UUID tableUUID,
										  UUID[] objectUUID,
										  long[] conglomerateNumber,
										  ExecIndexRow[] indexRow)
	{
		
		this.forTable = forTable;
		this.objectName = objectName;
		this.tableUUID = tableUUID;
		this.objectUUID = objectUUID;
		this.conglomerateNumber = conglomerateNumber;
		this.indexRow = indexRow;
	}

	public String toString()
	{
		return "UPDATE STATISTICS FOR " + (forTable ? "TABLE" : "INDEX") + " " +
			objectName; 
			
	}

	public void executeConstantAction(Activation activation) 
		throws StandardException
	
	{
		GroupFetchScanController gsc = null;
		TransactionController tc = activation.getTransactionController();
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();

		
		dd.startWriting(lcc);

		TableDescriptor td = dd.getTableDescriptor(tableUUID);
		dm.invalidateFor(td, DependencyManager.UPDATE_STATISTICS, lcc);

		for (int indexNumber = 0; indexNumber < conglomerateNumber.length;
			 indexNumber++) 
		{
			if (conglomerateNumber[indexNumber] == -1)
				continue;

			int numCols = indexRow[indexNumber].nColumns() - 1;;
			long[] cardinality = new long[numCols];
			long numRows = 0;
			initializeRowBuffers(indexRow[indexNumber]);

			try
			{
				/* Read uncommited, with record locking. Actually CS store may
				   not hold record locks */
				gsc = 
                    tc.openGroupFetchScan(
                        conglomerateNumber[indexNumber], 
                        false,  // hold
                        0,      // openMode: for read
                        TransactionController.MODE_RECORD, // locking
                        TransactionController.ISOLATION_READ_UNCOMMITTED, //isolation level
                        null,   // scancolumnlist-- want everything.
                        null,   // startkeyvalue-- start from the beginning.
                        0,
                        null,   // qualifiers, none!
                        null,   // stopkeyvalue,
                        0);
		
				boolean firstRow = true;
				int rowsFetched = 0;
				while ((rowsFetched = gsc.fetchNextGroup(rowBufferArray, null)) > 0)
				{
					for (int i = 0; i < rowsFetched; i++)
					{
						int whichPositionChanged = compareWithPrevKey(i, firstRow);
						firstRow = false;
						if (whichPositionChanged >= 0)
						{
							for (int j = whichPositionChanged; j < cardinality.length; j++)
								cardinality[j]++;
						}
						numRows++;
					}

					DataValueDescriptor[] tmp;
					tmp = rowBufferArray[GROUP_FETCH_SIZE - 1];
					rowBufferArray[GROUP_FETCH_SIZE - 1] = lastUniqueKey;
					lastUniqueKey = tmp;
				} // while
			} // try
			finally 
			{
				if (gsc != null)
				{
					gsc.close();
					gsc = null;
				}
			}

		    if (numRows == 0)
			{
				/* if there is no data in the table: no need to write anything
				 * to sys.systatstics.
				 */
				break;			
			}			

			StatisticsDescriptor statDesc;
		
			dd.dropStatisticsDescriptors(tableUUID, objectUUID[indexNumber],
										 tc); 

			for (int i = 0; i < indexRow[indexNumber].nColumns() - 1; i++)
			{
				statDesc = new StatisticsDescriptor(dd, dd.getUUIDFactory().createUUID(),
													   objectUUID[indexNumber],
													   tableUUID,
													   "I",
													   new StatisticsImpl(numRows,
																		  cardinality[i]),
													   i + 1);
				dd.addDescriptor(statDesc, null,
								 DataDictionary.SYSSTATISTICS_CATALOG_NUM,
								 true, tc);
			} // for each leading column (c1) (c1,c2)....

		} // for each index.
	}


	private void initializeRowBuffers(ExecIndexRow ir)
	{

		rowBufferArray = new DataValueDescriptor[GROUP_FETCH_SIZE][];
		lastUniqueKey = ir.getRowArrayClone();
		rowBufferArray[0] = ir.getRowArray(); // 1 gets old objects.
	}

  	private int compareWithPrevKey(int index, boolean firstRow)
  		throws StandardException
  	{
  		if (firstRow)
  			return 0;

  		DataValueDescriptor[] prev = (index == 0) ? lastUniqueKey : rowBufferArray[index - 1];
  		DataValueDescriptor[] curr = rowBufferArray[index];
  		// no point trying to do rowlocation; hence - 1
  		for (int i = 0; i < (prev.length - 1); i++)
  		{
			DataValueDescriptor dvd = (DataValueDescriptor)prev[i];

			if (dvd.isNull())
				return i;		// nulls are counted as unique values.

  			if (prev[i].compare(curr[i]) != 0)
  			{
  				return i;
  			}
  		}

  		return -1;
  	}

	

}
