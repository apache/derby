/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.GenericConglomerateController

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.shared.common.reference.SQLState;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;

import org.apache.derby.iapi.store.access.ConglomerateController;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.FetchDescriptor;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
**/

public abstract class GenericConglomerateController 
    extends GenericController implements ConglomerateController
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */


    /**************************************************************************
     * Public Methods implementing ConglomerateController which just 
     *     delegate to OpenConglomerate:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods implementing ConglomerateController:
     **************************************************************************
     */

    /**
     * @see ConglomerateController#close
     **/
    public void close()
        throws StandardException
	{
        super.close();

		// If we are closed due to catching an error in the middle of init,
		// xact_manager may not be set yet. 
        if ((open_conglom != null) && (open_conglom.getXactMgr() != null))
            open_conglom.getXactMgr().closeMe(this);
	}

    /**
     * Close conglomerate controller as part of terminating a transaction.
     * <p>
     * Use this call to close the conglomerate controller resources as part of
     * committing or aborting a transaction.  The normal close() routine may 
     * do some cleanup that is either unnecessary, or not correct due to the 
     * unknown condition of the controller following a transaction ending error.
     * Use this call when closing all controllers as part of an abort of a 
     * transaction.
     * <p>
     * This call is meant to only be used internally by the Storage system,
     * clients of the storage system should use the simple close() interface.
     * <p>
     * RESOLVE (mikem) - move this call to ConglomerateManager so it is
     * obvious that non-access clients should not call this.
     *
     * @param closeHeldScan           If true, means to close controller even if
     *                                it has been opened to be kept opened 
     *                                across commit.  This is
     *                                used to close these controllers on abort.
     *
	 * @return boolean indicating that the close has resulted in a real close
     *                 of the controller.  A held scan will return false if 
     *                 called by closeForEndTransaction(false), otherwise it 
     *                 will return true.  A non-held scan will always return 
     *                 true.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean closeForEndTransaction(boolean closeHeldScan)
		throws StandardException
    {
        super.close();

        if ((!open_conglom.getHold()) || closeHeldScan) 
        {
            // If we are closed due to catching an error in the middle of init,
            // xact_manager may not be set yet. 
            if ((open_conglom != null) && (open_conglom.getXactMgr() != null))
                open_conglom.getXactMgr().closeMe(this);

            return(true);

        }
        else
        {
            return(false);
        }
    }

    /**
     * @see ConglomerateController#delete
     **/
    public boolean delete(RowLocation loc)
		throws StandardException
	{
        if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                if (open_conglom.isClosed())
                    open_conglom.reopen();
            }
            else
            {
                throw(
                    StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            }
        }

        RowPosition pos = 
            open_conglom.getRuntimeMem().get_scratch_row_position();

        getRowPositionFromRowLocation(loc, pos);

        if (!open_conglom.latchPage(pos)) 
        {
            return false;
        }

        open_conglom.lockPositionForWrite(pos, true);

        boolean ret_val = true;

        // RESOLVE (mikem) - RECID - performance could be better if we did not
        // have to call isDeletedAtSlot().

        if (pos.current_page.isDeletedAtSlot(pos.current_slot))
        {
            ret_val = false;
        }
        else
        {
            // Delete the row 
            pos.current_page.deleteAtSlot(
                pos.current_slot, true, (LogicalUndo) null);

            // try to reclaim rows when the page is only full of deleted rows,
            // or in the special case of the first page when all rows except the
            // "control row" are deleted.  Or if the row we just deleted is
            // a long row or has a long column.
            if (pos.current_page.shouldReclaimSpace(
                pos.current_page.getPageNumber() == 1 ? 1 : 0,
                pos.current_slot))
            {
                queueDeletePostCommitWork(pos);
            }
        }

        pos.current_page.unlatch();

        return(ret_val);
	}

    /**
     * @see ConglomerateController#fetch
     **/
    public boolean fetch(
    RowLocation             loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns) 
		throws StandardException
	{
        if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                if (open_conglom.isClosed())
                    open_conglom.reopen();
            }
            else
            {
                throw(
                    StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            }
        }

        if (SanityManager.DEBUG)
        {
            // Make sure valid columns are in the list.  The RowUtil
            // call is too expensive to make in a released system for
            // every fetch.
            int invalidColumn = 
                RowUtil.columnOutOfRange(
                    row, validColumns, open_conglom.getFormatIds().length);

            if (invalidColumn >= 0)
            {
                throw(StandardException.newException(
                        SQLState.HEAP_TEMPLATE_MISMATCH,
                        invalidColumn, 
                        open_conglom.getFormatIds().length));
            }
        }

        // Get the record handle out of its wrapper.

        RowPosition pos = 
            open_conglom.getRuntimeMem().get_scratch_row_position();

        getRowPositionFromRowLocation(loc, pos);

        if (!open_conglom.latchPage(pos))
        {
            return(false);
        }


        // Do not get U row lock - only get X or S.  There is no good point
        // currently to convert the U lock to an S lock, we don't know when
        // the calling code is through with the lock.
        // RESOLVE (mikem) - talk to language and see if it is worth it to
        //     get U lock and have language call back when we should take
        //     appropriate action on the U lock.

        if (open_conglom.isForUpdate())
        {
            open_conglom.lockPositionForWrite(pos, true);
        }
        else
        {
            open_conglom.lockPositionForRead(
                pos, (RowPosition) null, false, true);
        }

        if (pos.current_page == null)
        {
            // The page is not latched after locking the row. This happens if
            // the row was deleted while we were waiting for the lock. Return
            // false to indicate that the row is no longer valid. (DERBY-4676)
            return false;
        }

        // Fetch the row.
        // RESOLVE (STO061) - don't know whether the fetch is for update or not.
        //
        // RESOLVE (mikem) - get rid of new here.
        boolean ret_val = 
            (pos.current_page.fetchFromSlot(
                pos.current_rh, pos.current_slot, 
                row, 
                new FetchDescriptor(
                    row.length, validColumns, (Qualifier[][]) null), 
                false) != null);

        // RESOLVE (mikem) - should be some way to hide this in the unlock call,
        // and just always make the unlock call.

        if (!open_conglom.isForUpdate())
            open_conglom.unlockPositionAfterRead(pos);

        pos.current_page.unlatch();

        return(ret_val);
	}

    /**
     * @see ConglomerateController#fetch
     **/
    public boolean fetch(
    RowLocation             loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns,
    boolean                 waitForLock) 
		throws StandardException
	{
        if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                if (open_conglom.isClosed())
                    open_conglom.reopen();
            }
            else
            {
                throw(
                    StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            }
        }

        if (SanityManager.DEBUG)
        {
            // Make sure valid columns are in the list.  The RowUtil
            // call is too expensive to make in a released system for
            // every fetch.
            int invalidColumn = 
                RowUtil.columnOutOfRange(
                    row, validColumns, open_conglom.getFormatIds().length);

            if (invalidColumn >= 0)
            {
                throw(StandardException.newException(
                        SQLState.HEAP_TEMPLATE_MISMATCH,
                        invalidColumn, 
                        open_conglom.getFormatIds().length));
            }
        }

        // Get the record handle out of its wrapper.

        RowPosition pos = 
            open_conglom.getRuntimeMem().get_scratch_row_position();

        getRowPositionFromRowLocation(loc, pos);

//IC see: https://issues.apache.org/jira/browse/DERBY-707
        if (!open_conglom.latchPage(pos)) 
        {
            return false;
        }

        // Do not get U row lock - only get X or S.  There is not good point
        // currently to convert the U lock to an S lock, we don't know when
        // the calling code is through with the lock.
        // RESOLVE (mikem) - talk to language and see if it is worth it to
        //     get U lock and have language call back when we should take
        //     appropriate action on the U lock.

        if (open_conglom.isForUpdate())
        {
//IC see: https://issues.apache.org/jira/browse/DERBY-4685
            open_conglom.lockPositionForWrite(pos, waitForLock);
        }
        else
        {
            open_conglom.lockPositionForRead(
                pos, (RowPosition) null, false, waitForLock);
        }

        if (pos.current_page == null)
        {
            // The page is not latched after locking the row. This happens if
            // the row was deleted while we were waiting for the lock. Return
            // false to indicate that the row is no longer valid. (DERBY-4676)
            return false;
        }

        // Fetch the row.
        // RESOLVE (STO061) - don't know whether the fetch is for update or not.
        //
        //
        // RESOLVE (mikem) - get rid of new here.
        boolean ret_val = 
            (pos.current_page.fetchFromSlot(
                pos.current_rh, pos.current_slot, 
                row, 
                new FetchDescriptor(
                    row.length, validColumns, (Qualifier[][]) null), 
                false) != null);

        // RESOLVE (mikem) - should be some way to hide this in the unlock call,
        // and just always make the unlock call.
        if (!open_conglom.isForUpdate())
            open_conglom.unlockPositionAfterRead(pos);

        pos.current_page.unlatch();

        return(ret_val);
	}


    /**
     * @see ConglomerateController#replace
     **/
    public boolean replace(
    RowLocation             loc, 
    DataValueDescriptor[]   row, 
    FormatableBitSet                 validColumns)
		throws StandardException
	{
        if (open_conglom.isClosed())
        {
            if (open_conglom.getHold())
            {
                if (open_conglom.isClosed())
                    open_conglom.reopen();
            }
            else
            {
                throw(
                    StandardException.newException(
                        SQLState.HEAP_IS_CLOSED, 
                        open_conglom.getConglomerate().getId()));
            }
        }

        if (SanityManager.DEBUG)
        {
            // Make sure valid columns are in the list.  The RowUtil
            // call is too expensive to make in a released system for
            // every fetch.
            int invalidColumn = 
                RowUtil.columnOutOfRange(
                    row, validColumns, open_conglom.getFormatIds().length);

            if (invalidColumn >= 0)
            {
                throw(StandardException.newException(
                        SQLState.HEAP_TEMPLATE_MISMATCH,
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                        invalidColumn, 
                        open_conglom.getFormatIds().length));
            }
        }

//IC see: https://issues.apache.org/jira/browse/DERBY-806
//IC see: https://issues.apache.org/jira/browse/DERBY-806
//IC see: https://issues.apache.org/jira/browse/DERBY-806
//IC see: https://issues.apache.org/jira/browse/DERBY-806
        RowPosition pos = 
            open_conglom.getRuntimeMem().get_scratch_row_position();

        getRowPositionFromRowLocation(loc, pos);

//IC see: https://issues.apache.org/jira/browse/DERBY-707
//IC see: https://issues.apache.org/jira/browse/DERBY-707
        if (!open_conglom.latchPage(pos)) 
        {
            return false;
        }

        open_conglom.lockPositionForWrite(pos, true);
//IC see: https://issues.apache.org/jira/browse/DERBY-4685
//IC see: https://issues.apache.org/jira/browse/DERBY-4685
//IC see: https://issues.apache.org/jira/browse/DERBY-4685

        boolean ret_val = true;

        if (pos.current_page.isDeletedAtSlot(pos.current_slot))
        {
            ret_val = false;
        }
        else
        {
            // Update the record.  
            pos.current_page.updateAtSlot(pos.current_slot, row, validColumns);
        }

        pos.current_page.unlatch();

        return(ret_val);
    }
}
