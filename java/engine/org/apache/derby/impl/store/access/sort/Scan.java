/*

   Derby - Class org.apache.derby.impl.store.access.sort.Scan

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

package org.apache.derby.impl.store.access.sort;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.i18n.MessageService;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.types.Orderable;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.ScanManager;

import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.ScanInfo;

import org.apache.derby.iapi.store.raw.Page;

import org.apache.derby.iapi.types.DataValueDescriptor;

import java.util.Properties;

/**

	Abstract base class for all sort classes which return rows from the
	sort.  Subclasses must implement fetch, next, and close.

**/

public abstract class Scan implements ScanManager, ScanInfo
{
	/*
	 * Methods of ScanController
	 */

    /**
     * A call to allow client to indicate that current row does not qualify.
     * <p>
     * Indicates to the ScanController that the current row does not
     * qualify for the scan.  If the isolation level of the scan allows, 
     * this may result in the scan releasing the lock on this row.
     * <p>
     * Note that some scan implimentations may not support releasing locks on 
     * non-qualifying rows, or may delay releasing the lock until sometime
     * later in the scan (ie. it may be necessary to keep the lock until 
     * either the scan is repositioned on the next row or page).
     * <p>
     * This call should only be made while the scan is positioned on a current
     * valid row.
     * <p>
     * This call does not make sense for sort scans.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void didNotQualify()
        throws StandardException
    {
    }

    /**
     * Fetch the next N rows from the table.
     * <p>
     * Currently unimplemented for sorts.
     * <p>
     **/
    public int fetchNextGroup(
    DataValueDescriptor[][]     row_array,
    RowLocation[]               rowloc_array)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
     * Insert all rows that qualify for the current scan into the input
     * Hash table.  
     * <p>
     * Currently unimplemented for sorts.
     * <p>
     **/
    public void fetchSet(
    long                    max_rowcnt,
    int[]                   key_column_numbers,
    BackingStoreHashtable   hash_table)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
    Returns true if the current position of the scan still qualifies
    under the set of qualifiers passed to the openScan().
	@see ScanController#doesCurrentPositionQualify
    **/
    public boolean doesCurrentPositionQualify()
		throws StandardException
    {
		return true;
    }

	/**
	Fetch the location of the current position in the scan.
	@see ScanController#fetchLocation
	**/
	public void fetchLocation(RowLocation templateLocation)
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    /**
     * Return ScanInfo object which describes performance of scan.
     * <p>
     * Return ScanInfo object which contains information about the current
     * scan.
     * <p>
     * Currently the ScanInfo does not have any performance data.
     *
     * @see ScanInfo
     *
	 * @return The ScanInfo object which contains info about current scan.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public ScanInfo getScanInfo()
		throws StandardException
    {
        return(this);
    }

    /**
     * Get the total estimated number of rows in the container.
     * <p>
     * The number is a rough estimate and may be grossly off.  In general
     * the server will cache the row count and then occasionally write
     * the count unlogged to a backing store.  If the system happens to 
     * shutdown before the store gets a chance to update the row count it
     * may wander from reality.
     * <p>
     * This call is currently only supported on Heap conglomerates, it
     * will throw an exception if called on btree conglomerates.
     *
	 * @return The total estimated number of rows in the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public long getEstimatedRowCount()
		throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
     * Set the total estimated number of rows in the container.
     * <p>
     * Often, after a scan, the client of RawStore has a much better estimate
     * of the number of rows in the container than what store has.  For 
     * instance if we implement some sort of update statistics command, or
     * just after a create index a complete scan will have been done of the
     * table.  In this case this interface allows the client to set the
     * estimated row count for the container, and store will use that number
     * for all future references.
     * <p>
     * This call is currently only supported on Heap conglomerates, it
     * will throw an exception if called on btree conglomerates.
     *
     * @param count the estimated number of rows in the container.
     *
	 * @return The total estimated number of rows in the conglomerate.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void setEstimatedRowCount(long count)
		throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
    Returns true if the current position of the scan is at a 
    deleted row.
	@see ScanController#isCurrentPositionDeleted
    **/
    public boolean isCurrentPositionDeleted()
		throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
     * Return whether this is a keyed conglomerate.
     * <p>
     *
	 * @return whether this is a keyed conglomerate.
     **/
	public boolean isKeyed()
    {
        return(false);
    }

    /**
     * Return whether this scan is table locked.
     *
	 * @return whether this is table locked.
     **/
    public boolean isTableLocked()
    {
        return(true);
    }

    /**
    Delete the row at the current position of the scan.
	@see ScanController#delete
    **/
    public boolean delete()
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

    /**
    Reposition the current scan.
	@see ScanController#reopenScan
    **/
	public void reopenScan(
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    Qualifier               qualifier[][],
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator)
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
    Reposition the current scan.  This call is semantically the same as if
    the current scan had been closed and a openScan() had been called instead.
    The scan is reopened against the same conglomerate, and the scan
    is reopened with the same "scan column list", "hold" and "forUpdate"
    parameters passed in the original openScan.  

	@exception StandardException Standard exception policy.
    **/
	public void reopenScanByRowLocation(
    RowLocation startRowLocation,
    Qualifier qualifier[][])
        throws StandardException
    {
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
    }

    /**
    Replace the entire row at the current position of the scan.
	@see ScanController#replace
    **/
    public boolean replace(
    DataValueDescriptor[]   val, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

	/**
	Return a row location object of the correct type to be
	used in calls to fetchLocation.
	@see ScanController#newRowLocationTemplate
	**/
	public RowLocation newRowLocationTemplate()
		throws StandardException
 	{
        throw StandardException.newException(
                SQLState.SORT_IMPROPER_SCAN_METHOD);
	}

	/*
	** Methods of ScanManager
	*/

    /**
     * Do work necessary to maintain the current position in the scan.
     * <p>
     * The latched page in the conglomerate "congomid" is changing, do
     * whatever is necessary to maintain the current position of the scan.
     * For some conglomerates this may be a no-op.
     * <p>
     *
     * @param conlgom   Conglomerate object of the conglomerate being changed.
     * @param page      Page in the conglomerate being changed.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void savePosition(Conglomerate conglom, Page page)
        throws StandardException
	{
        // RESOLVE (mikem), under the current implementation all scans within
        // a transaction are called rather than just the ones with the right
        // conglomid.  For now just have sort scans ignore the call. 
        
        return;
	}

	/*
	 * Methods of ScanInfo
	 */
   
    /**
     * Return all information gathered about the scan.
     * <p>
     * This routine returns a list of properties which contains all information
     * gathered about the scan.  If a Property is passed in, then that property
     * list is appeneded to, otherwise a new property object is created and
     * returned.
     * <p>
     * Currently sort scans doesn't track any information.
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Properties getAllScanInfo(Properties prop)
		throws StandardException
    {
        if (prop == null)
            prop = new Properties();

        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_SCAN_TYPE),
			MessageService.getTextMessage(SQLState.STORE_RTS_SORT));

        return(prop);
    }
}
