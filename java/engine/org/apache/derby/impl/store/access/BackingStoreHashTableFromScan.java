/*

   Derby - Class org.apache.derby.impl.store.access.BackingStoreHashTableFromScan

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

package org.apache.derby.impl.store.access;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.ScanManager;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowSource;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Properties;

/**

Extend BackingStoreHashtable with the ability to maintain the underlying 
openScan() until the hashtable has been closed.  This is necessary for 
long row access.  Access to long row delays actual objectification until
the columns are accessed, but depends on the underlying table to be still
open when the column is accessed.  

<P>
Transactions are obtained from an AccessFactory.
@see BackingStoreHashtable

**/

class BackingStoreHashTableFromScan extends BackingStoreHashtable
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private ScanManager             open_scan;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public BackingStoreHashTableFromScan(
        TransactionController   tc,
		long                    conglomId,
		int                     open_mode,
        int                     lock_level,
        int                     isolation_level,
		FormatableBitSet                 scanColumnList,
		DataValueDescriptor[]   startKeyValue,
		int                     startSearchOperator,
		Qualifier               qualifier[][],
		DataValueDescriptor[]   stopKeyValue,
		int                     stopSearchOperator,
        long                    max_rowcnt,
        int[]                   key_column_numbers,
        boolean                 remove_duplicates,
        long                    estimated_rowcnt,
        long                    max_inmemory_rowcnt,
        int                     initialCapacity,
        float                   loadFactor,
        boolean                 collect_runtimestats,
		boolean					skipNullKeyColumns)
            throws StandardException
    {

        super(
            tc, 
            (RowSource) null,
            key_column_numbers,
            remove_duplicates,
            estimated_rowcnt,
            max_inmemory_rowcnt,
            initialCapacity,
            loadFactor,
			skipNullKeyColumns);

        open_scan =  (ScanManager)
            tc.openScan(
                conglomId,
                false,
                open_mode,
                lock_level,
                isolation_level,
                scanColumnList,
                startKeyValue,
                startSearchOperator,
                qualifier,
                stopKeyValue,
                stopSearchOperator);

        open_scan.fetchSet(
            max_rowcnt, key_column_numbers, this);

        if (collect_runtimestats)
        {
            Properties prop = new Properties();
            open_scan.getScanInfo().getAllScanInfo(prop);
            this.setAuxillaryRuntimeStats(prop);
            prop = null;
        }
    }


    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Close the BackingStoreHashtable.
     * <p>
     * Perform any necessary cleanup after finishing with the hashtable.  Will
     * deallocate/dereference objects as necessary.  If the table has gone
     * to disk this will drop any on disk files used to support the hash table.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void close() 
		throws StandardException
    {
        open_scan.close();

        super.close();

        return;
    }

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
