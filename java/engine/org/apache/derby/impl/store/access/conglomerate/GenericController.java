/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.GenericController

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.SpaceInfo;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;


import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;

import java.util.Properties; 


/**
**/

public abstract class GenericController 
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    protected OpenConglomerate    open_conglom;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */
    protected void getRowPositionFromRowLocation(
    RowLocation row_loc,
    RowPosition pos)
        throws StandardException
    {
        // Not implemented in default conglomerate, needs to be overridden.
        throw StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE);
       
    }

    protected void queueDeletePostCommitWork(
    RowPosition pos)
        throws StandardException
    {
        // Not implemented in default conglomerate, needs to be overridden.
        throw StandardException.newException(
                SQLState.HEAP_UNIMPLEMENTED_FEATURE);
    }


    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */
    public void init(
    OpenConglomerate    open_conglom)
        throws StandardException
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(open_conglom != null);

        this.open_conglom = open_conglom;
    }

    public OpenConglomerate getOpenConglom()
    {
        return(open_conglom);
    }


    /**************************************************************************
     * Public Methods implementing ConglomerateController which just 
     *     delegate to OpenConglomerate:
     **************************************************************************
     */

    public void checkConsistency()
		throws StandardException
    {
        open_conglom.checkConsistency();
    }

    public void debugConglomerate()
		throws StandardException
    {
        open_conglom.debugConglomerate();
    }

    public void getTableProperties(Properties prop)
		throws StandardException
    {
        open_conglom.getTableProperties(prop);
    }

    public Properties getInternalTablePropertySet(Properties prop)
		throws StandardException
    {
        return(open_conglom.getInternalTablePropertySet(prop));
    }

    public SpaceInfo getSpaceInfo()
        throws StandardException
    {
        return(open_conglom.getSpaceInfo());
    }

    public void close()
        throws StandardException
    {
        if (open_conglom != null)
            open_conglom.close();
    }

	public boolean isKeyed()
	{
		return(open_conglom.isKeyed());
	}

	public RowLocation newRowLocationTemplate()
		throws StandardException
	{
        if (open_conglom.isClosed())
            open_conglom.reopen();

        return(open_conglom.newRowLocationTemplate());
	}

    /**
     * is the open btree table locked?
     **/
    public boolean isTableLocked()
    {
        return(open_conglom.isTableLocked());
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
        if (open_conglom.isClosed())
            open_conglom.reopen();

        // Don't return 0 rows (return 1 instead), as this often leads the 
        // optimizer to produce plans which don't use indexes because of the 0 
        // row edge case.
        //
        // Eventually the plan is recompiled when rows are added, but we
        // have seen multiple customer cases of deadlocks and timeouts 
        // because of these 0 row based plans.  
        long row_count = open_conglom.getContainer().getEstimatedRowCount(0);

        return( (row_count == 0) ? 1 : row_count);
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
        ContainerHandle container = open_conglom.getContainer();

        if (container == null)
            open_conglom.reopen();

        open_conglom.getContainer().setEstimatedRowCount(
                count, /* unused flag */ 0);
    }

    /**************************************************************************
     * Public Methods implementing ConglomerateController:
     **************************************************************************
     */

}
