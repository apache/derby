/*

   Derby - Class org.apache.derby.impl.sql.catalog.IndexScan

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

package org.apache.derby.impl.sql.catalog;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.CatalogRowFactory;
import org.apache.derby.iapi.sql.dictionary.TabInfo;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ScanQualifier;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
  Convience class for scanning an index.
  */
public class IndexScan
{
	//
	//Glorp the caller supplies
	TabInfo ti;
	int indexId;
	ScanQualifier[][] qualifiers;

	//
	//Glorp we set up for the caller.
	ExecutionFactory ef;
	TransactionController tc;
	DataDictionary	dataDictionary;
	ConglomerateController heapCC;
	DataValueFactory dvf;
	private RowLocation	baseRowLocation;
	ExecRow	baseRow;
	CatalogRowFactory rf;
	ExecIndexRow indexRow;
	ScanController sc;
	
	/**
	  Create a scan on an index.

	  @param dataDictionary	the namespace
	  @param ti TabInfo for the system table associated with the index.
	  @param indexId the id for the index (From the CatalogRowFactory).
	  @param keyCols the key columns for the scan.
	  @param qualifiers qualifiers
	  @exception StandardException Ooops
	  */
	public IndexScan
	(
		DataDictionary	dataDictionary,
		TabInfo 		ti,
		int 			indexId,
		DataValueDescriptor[] 	keyCols,
		ScanQualifier[][] qualifiers
    )
		 throws StandardException 
	{
		this.dataDictionary = dataDictionary;
		this.ti = ti;
		this.indexId = indexId;
		this.qualifiers = qualifiers;

		LanguageConnectionContext 	lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
		tc = lcc.getTransactionCompile();
		dvf = lcc.getDataValueFactory();
		
		ExecutionContext ec= lcc.getExecutionContext();
		ef = ec.getExecutionFactory();

		DataValueDescriptor[] rowArray = null;
		if (keyCols != null)
		{
			ExecIndexRow keyRow = ef.getIndexableRow(keyCols.length);
			for (int ix=0;ix<keyCols.length;ix++)
				keyRow.setColumn(ix+1,(DataValueDescriptor)keyCols[ix]);
			rowArray = keyRow.getRowArray();
		}

		rf = ti.getCatalogRowFactory();

		baseRow = rf.makeEmptyRow();
		heapCC = tc.openConglomerate( ti.getHeapConglomerate(),
                                      false,
									  0,
									  TransactionController.MODE_TABLE,
                                      TransactionController.ISOLATION_REPEATABLE_READ);
		indexRow = rf.buildEmptyIndexRow(indexId,
										  heapCC.newRowLocationTemplate());
		sc = tc.openScan(ti.getIndexConglomerate(indexId),
                 false,                           // don't hold open across commit
                 0,                               // for read
                 TransactionController.MODE_TABLE,
                 TransactionController.ISOLATION_REPEATABLE_READ,
                 (FormatableBitSet) null,                  // all fields as objects
                 rowArray,            			  // start position - first row
                 ScanController.GE,               // startSearchOperation
                 qualifiers,                      //scanQualifier,
                 rowArray,                        // stop position - through last row
                 ScanController.GT);              // stopSearchOperation
	} 

	/**
	  Fetch a row from the index scan.

	  @return The row or null. Note that the next call to fetch will
	  replace the columns in the returned row.
	  @exception StandardException Ooops
	  */
	public ExecIndexRow fetch()
		 throws StandardException
	{ 
		if (sc == null) return null;
		if (!sc.next())
		{
			sc.close();
			return null;
		}
		sc.fetch(indexRow.getRowArray());
		return indexRow;
	}

	/**
	  Fetch the base row corresponding to the current index row

	  @return The base row row or null.
	  @exception StandardException Ooops
	  */
	public ExecRow fetchBaseRow()
		 throws StandardException
	{ 
		if (sc == null) return null;

		baseRowLocation = (RowLocation) indexRow.getColumn(indexRow.nColumns());

		boolean base_row_exists = 
            heapCC.fetch(
                baseRowLocation, baseRow.getRowArray(), (FormatableBitSet) null);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(base_row_exists, "base row disappeared.");
        }

		return baseRow;
	}

	/**
	  Close the scan.
	  */
	public void close()
        throws StandardException
	{
		if (sc != null) 
		{
			sc.close();
			sc = null;
		}

		if ( heapCC == null )
		{
			heapCC.close();
			heapCC = null;
		}
	}
}
