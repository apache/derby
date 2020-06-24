/*

   Derby - Class org.apache.derby.impl.sql.execute.BasicSortObserver

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

import org.apache.derby.iapi.store.access.SortObserver;

import org.apache.derby.shared.common.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Vector;
import org.apache.derby.shared.common.sanity.SanityManager;

/**
 * This is the most basic sort observer.  It
 * handles distinct sorts and non-distinct sorts.
 *
 */
public class BasicSortObserver implements SortObserver
{
	protected boolean 	doClone;
	protected boolean	distinct;
	private	  boolean	reuseWrappers;
	private	  ExecRow	execRow;
	private	  Vector<DataValueDescriptor[]>	vector;

	/**
	 * Simple constructor
	 *
	 * @param doClone If true, then rows that are retained
	 *		by the sorter will be cloned.  This is needed
	 *		if language is reusing row wrappers.
	 *
	 * @param distinct	If true, toss out duplicates.  
	 *		Otherwise, retain them.
	 *
	 * @param execRow	ExecRow to use as source of clone for store.
	 *
	 * @param reuseWrappers	Whether or not we can reuse the wrappers
	 */
	public BasicSortObserver(boolean doClone, boolean distinct, ExecRow	execRow, boolean reuseWrappers)
	{
		this.doClone = doClone;	
		this.distinct = distinct;
		this.execRow = execRow;
		this.reuseWrappers = reuseWrappers;
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		vector = new Vector<DataValueDescriptor[]>();
	}

	/**
	 * Called prior to inserting a distinct sort
	 * key.  
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining
	 *
	 * @return the row to be inserted by the sorter.  If null,
	 *		then nothing is inserted by the sorter.  Distinct
	 *		sorts will want to return null.
	 *
	 * @exception StandardException never thrown
	 */
	public DataValueDescriptor[] insertNonDuplicateKey(DataValueDescriptor[] insertRow)
		throws StandardException
	{
		return (doClone) ? 
					getClone(insertRow) :
					insertRow;
	}	
	/**
	 * Called prior to inserting a duplicate sort
	 * key.  
	 *
	 * @param insertRow the current row that the sorter
	 * 		is on the verge of retaining.  It is a duplicate
	 * 		of existingRow.
	 *
	 * @param existingRow the row that is already in the
	 * 		the sorter which is a duplicate of insertRow
	 *
	 * @exception StandardException never thrown
	 */
	public DataValueDescriptor[] insertDuplicateKey(DataValueDescriptor[] insertRow, DataValueDescriptor[] existingRow) 
			throws StandardException
	{
		return (distinct) ?
					(DataValueDescriptor[])null :
						(doClone) ? 
							getClone(insertRow) :
							insertRow;

	}

	public void addToFreeList(DataValueDescriptor[] objectArray, int maxFreeListSize)
	{
		if (reuseWrappers && vector.size() < maxFreeListSize)
		{
			vector.addElement(objectArray);
		}
	}

	public DataValueDescriptor[] getArrayClone()
		throws StandardException
	{
		int lastElement = vector.size();

		if (lastElement > 0)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
			DataValueDescriptor[] retval = vector.elementAt(lastElement - 1);
			vector.removeElementAt(lastElement - 1);
			return retval;
		}
		return execRow.getRowArrayClone();
	}


	private DataValueDescriptor[] getClone(DataValueDescriptor[] origArray)
	{
		/* If the free list is not empty, then
		 * get an DataValueDescriptor[] from there and swap
		 * objects between that DataValueDescriptor[] and 
		 * origArray, returning the DataValueDescriptor[]
		 * from the free list.  That will save
		 * on unnecessary cloning.
		 */
/* RESOLVE - We can't enable this code
 * until Bug 2829 is fixed.
 * (Close bug 2828 when enabling the code.
		if (vector.size() > 0)
		{
			DataValueDescriptor[] retval = getArrayClone();
			for (int index = 0; index < retval.length; index++)
			{
				DataValueDescriptor tmp = origArray[index];
				origArray[index] = retval[index];
				retval[index] = tmp;
			}
			return retval;
		}
*/
		DataValueDescriptor[] newArray = new DataValueDescriptor[origArray.length];
		for (int i = 0; i < origArray.length; i++)
		{
            // History: We used to materialize streams when getting a clone
            //          here (i.e. used getClone, not cloneObject). We still
            //          do, as the sorter closes the underlying source result
            //          set very early, which causes store streams to fail
            //          because the container handle is closed.
            // Beetle 4896.
//IC see: https://issues.apache.org/jira/browse/DERBY-4520
            newArray[i] = origArray[i].cloneValue(true);
		}

		return newArray;
	}

    public boolean deferred() {
//IC see: https://issues.apache.org/jira/browse/DERBY-532
//IC see: https://issues.apache.org/jira/browse/DERBY-3330
//IC see: https://issues.apache.org/jira/browse/DERBY-6419
        return false;
    }

    public boolean deferrable() {
        return false;
    }

    public void rememberDuplicate(DataValueDescriptor[] row)
            throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.NOTREACHED();
        }
    }
}
