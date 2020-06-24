/*

   Derby - Class org.apache.derby.impl.store.access.sort.SortBufferScan

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

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

/**

  A sort scan that just reads rows out of a sorter.

**/

public class SortBufferScan extends SortScan
{
	/**
	The sorter we're returning rows from.
	**/
	protected SortBuffer sortBuffer;

	/*
	 * Constructors.
	 */

	SortBufferScan(
    MergeSort           sort, 
    TransactionManager  tran, 
    SortBuffer          sortBuffer,
    boolean             hold)
	{
		super(sort, tran, hold);

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(sortBuffer != null);

		this.sortBuffer = sortBuffer;
	}

	/*
	 * Methods of MergeSortScan
	 */

    /**
    Move to the next position in the scan.
	@see org.apache.derby.iapi.store.access.ScanController#next
    **/
    public boolean next()
		throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                sortBuffer != null, 
                "next() called on scan after scan was closed.");
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-6322
        current = sortBuffer.removeFirst();
        return current != null;
    }

    /**
    Close the scan.
    **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
    {
        if (closeHeldScan || !hold)
        {
            close();
            return(true);
        }
        else
        {
            return(false);
        }

    }

    /**
    Close the scan.
	@see org.apache.derby.iapi.store.access.ScanController#close
    **/
    public void close()
	{
		if (super.sort != null)
		{
			sort.doneScanning(this, sortBuffer);
			sortBuffer = null;
		}
		super.close();
	}

}
