/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.services.stream.HeaderPrintWriter;
import org.apache.derby.iapi.services.stream.InfoStreams;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.error.StandardException;


import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

/**
 * Eliminates duplicates while scanning the underlying conglomerate.
 * (Assumes no predicates, for now.)
 *
 * @author jerry
 */
public class DistinctScanResultSet extends HashScanResultSet
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	Enumeration element = null;


    //
    // class interface
    //
    public DistinctScanResultSet(long conglomId, 
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		GeneratedMethod resultRowAllocator, 
		int resultSetNumber,
		int hashKeyItem,
		String tableName,
		String indexName,
		boolean isConstraint,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost,
		GeneratedMethod closeCleanup)
			throws StandardException
    {
		super(conglomId, scoci, activation, resultRowAllocator, resultSetNumber,
			  (GeneratedMethod) null, // startKeyGetter
			  0,					  // startSearchOperator
			  (GeneratedMethod) null, // stopKeyGetter
			  0,					  // stopSearchOperator
			  false,				  // sameStartStopPosition
			  (Qualifier[][]) null,	  // scanQualifiers
			  (Qualifier[][]) null,	  // nextQualifiers
			  DEFAULT_INITIAL_CAPACITY, DEFAULT_LOADFACTOR, DEFAULT_MAX_CAPACITY,
			  hashKeyItem, tableName, indexName, isConstraint, 
			  false,				  // forUpdate
			  colRefItem, lockMode, tableLocked, isolationLevel,
			  false,
			  optimizerEstimatedRowCount, optimizerEstimatedCost, closeCleanup);

		// Tell super class to eliminate duplicates
		eliminateDuplicates = true;
    }

	//
	// ResultSet interface (override methods from HashScanResultSet)
	//

	/**
     * Return the next row (if any) from the scan (if open).
	 *
	 * @exception StandardException thrown on failure to get next row
	 */
	public ExecRow getNextRowCore() throws StandardException
	{
	    ExecRow result = null;
		Object[] columns = null;

		beginTime = getCurrentTimeMillis();
	    if ( isOpen )
	    {
			if (firstNext)
			{
				element = hashtable.elements();
				firstNext = false;
			}

			if (element.hasMoreElements())
			{
				columns = (Object[]) element.nextElement();

				setCompatRow(compactRow, columns);

				rowsSeen++;

				result = compactRow;
			}
			// else done
		}

		currentRow = result;
		setCurrentRow(result);

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}
}
