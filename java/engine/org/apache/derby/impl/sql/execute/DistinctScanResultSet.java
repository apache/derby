/*

   Derby - Class org.apache.derby.impl.sql.execute.DistinctScanResultSet

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

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.sql.Activation;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Enumeration;

/**
 * Eliminates duplicates while scanning the underlying conglomerate.
 * (Assumes no predicates, for now.)
 *
 */
class DistinctScanResultSet extends HashScanResultSet
{

	Enumeration element = null;


    //
    // class interface
    //
    DistinctScanResultSet(long conglomId, 
		StaticCompiledOpenConglomInfo scoci, Activation activation, 
		int resultRowTemplate,
		int resultSetNumber,
		int hashKeyItem,
		String tableName,
		String userSuppliedOptimizerOverrides,
		String indexName,
		boolean isConstraint,
		int colRefItem,
		int lockMode,
		boolean tableLocked,
		int isolationLevel,
		double optimizerEstimatedRowCount,
		double optimizerEstimatedCost)
			throws StandardException
    {
		super(conglomId, scoci, activation, resultRowTemplate, resultSetNumber,
			  (GeneratedMethod) null, // startKeyGetter
			  0,					  // startSearchOperator
			  (GeneratedMethod) null, // stopKeyGetter
			  0,					  // stopSearchOperator
			  false,				  // sameStartStopPosition
			  (Qualifier[][]) null,	  // scanQualifiers
			  (Qualifier[][]) null,	  // nextQualifiers
			  DEFAULT_INITIAL_CAPACITY, DEFAULT_LOADFACTOR, DEFAULT_MAX_CAPACITY,
			  hashKeyItem, tableName, userSuppliedOptimizerOverrides, indexName, isConstraint, 
			  false,				  // forUpdate
			  colRefItem, lockMode, tableLocked, isolationLevel,
			  false,
			  optimizerEstimatedRowCount, optimizerEstimatedCost);

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
                DataValueDescriptor[] columns = (DataValueDescriptor[]) element.nextElement();

				setCompatRow(compactRow, columns);

				rowsSeen++;

				result = compactRow;
			}
			// else done
		}

		setCurrentRow(result);

		nextTime += getElapsedMillis(beginTime);
	    return result;
	}
}
