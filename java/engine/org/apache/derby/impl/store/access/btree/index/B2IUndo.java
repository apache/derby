/*

   Derby - Class org.apache.derby.impl.store.access.btree.index.B2IUndo

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

package org.apache.derby.impl.store.access.btree.index;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.io.ArrayInputStream;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Formatable;
import org.apache.derby.iapi.services.io.FormatIdUtil;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.LogicalUndoable;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

// imports of inherited impl's
import org.apache.derby.impl.store.access.btree.BTree;
import org.apache.derby.impl.store.access.btree.BTreeLockingPolicy;
import org.apache.derby.impl.store.access.btree.ControlRow;
import org.apache.derby.impl.store.access.btree.OpenBTree;
import org.apache.derby.impl.store.access.btree.SearchParameters;

import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;
import org.apache.derby.iapi.services.io.LimitObjectInput;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**
 * @format_id ACCESS_B2IUNDO_V1_ID
 *
 * @purpose   Implements the LogicalUndo and Formatable interfaces, basically
 *            providing a way for raw store recovery to "call back" access code
 *            to provide logical undo ability.
 *
 * @upgrade   RESOLVE.
 *
 * @disk_layout 
 *     No state associated with this format.
 *
 **/

/**

The B2IUndo interface packages up the routines which the rawstore needs
to call to perform logical undo of a record in a B2i.  The rawstore will
determine that a page has changed since the record was written, and if it
has it will call the findUndo() interface, to find the page where the record
exists (as it may have moved).
<p>
This class must not contain any persistent state, as this class is stored
in the log record of the insert/delete.

@see org.apache.derby.iapi.store.raw.LogicalUndoable
@see org.apache.derby.iapi.store.raw.Undoable#generateUndo 
**/
public class B2IUndo implements LogicalUndo, Formatable
{
	/**
	 * Find the page and record to undo.  If no logical undo is necessary,
	 * i.e., row has not moved, then just return the latched page where undo
	 * should go.  If the record has moved, it has a new recordId on the new
	 * page, this routine needs to call pageOp.resetRecord with the new
	 * RecordHandle so that the logging system can update the compensation
	 * Operation with the new location.
     *
	 * @param transaction the transaction doing the rollback
	 * @param pageOp the page operation that supports logical undo.  This
	 * 		LogicalUndo function pointer is a field of that pageOperation
	 * @param in data stored in the log stream that contains the record data
	 * 		necessary to restore the row.
     *
     *	@exception StandardException Standard Cloudscape error policy
	 *  @exception IOException Method may read from InputStream
	 */
	public Page findUndo(
    Transaction     rawtran, 
    LogicalUndoable pageOp,
    LimitObjectInput     in)
        throws StandardException, IOException
    {
        ControlRow            root                      = null;
        ControlRow            control_row               = null;
        DataValueDescriptor[] logged_index_row_template = null;
        DataValueDescriptor[] template                  = null;
        Page                  ret_page                  = null;
        ContainerHandle       container                 = pageOp.getContainer();
        RecordHandle          rechandle                 = pageOp.getRecordHandle();
        boolean               ok_exit                   = false;
        int                   compare_result            = 1;
        B2I                   btree                     = null;

        // Open the btree to associate the open contain handle, thus the 
        // current xact with all subsequent operations during undo.


        try
        {

            // Need Conglomerate to create templates - get from the root page.
            root = ControlRow.Get(container, BTree.ROOTPAGEID);

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(root.getPage().isLatched());

            btree = (B2I) root.getConglom(B2I.FORMAT_NUMBER);

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(btree instanceof B2I);

            // create a template for the logged index row from the conglomerate.
            logged_index_row_template = btree.createTemplate();

            // create a template for the page index row from the conglomerate.
            template                  = btree.createTemplate();
        }
        finally
        {
            if (root != null)
                root.release();
        }

        // Get logged row from record.
        pageOp.restoreLoggedRow(logged_index_row_template, in);

        // RESOLVE (mikem) - currently restoreLoggedRow() may latch and unlatch
        // a page in the container (see ST059).
        // Now get the page where the record used to be.

        ok_exit = false;
        try
        {
            // "open" the btree, using recovery's already opened container 
            OpenBTree open_btree = new OpenBTree();

            open_btree.init(
                (TransactionManager) null,  // current user xact - not needed
                (TransactionManager) null,  // current xact      - not needed
                pageOp.getContainer(),      // recovery already opened container
                rawtran, 
                false,
                ContainerHandle.MODE_FORUPDATE, 
                                            // open_mode not used - container is
                                            // already opened.
                TransactionManager.MODE_NONE,
                (BTreeLockingPolicy) null,  // don't get locks during undo
                btree,                       
                (LogicalUndo) null,         // no logical undo necessary, as 
                                            // this code only does read.
                (DynamicCompiledOpenConglomInfo) null);

            // System.out.println(
              //   "calling logical undo, recordhandle = " + rechandle);
            // System.out.println("calling logical undo, record= " + 
              //    logged_index_row_template);

            // Get the page where the record was originally, before splits
            // could have possibly moved it.
            control_row = ControlRow.Get(open_btree, rechandle.getPageNumber());

            // init compare_result, if record doesn't exist do the search 
            compare_result = 1;

            if (control_row.getPage().recordExists(rechandle, true))
            {

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        control_row.getPage().fetchNumFields(rechandle) ==
                        logged_index_row_template.length);
                }

                // create template for the page index row from the conglomerate.
                RecordHandle ret_rechandle = 
                    control_row.getPage().fetchFromSlot(
                        (RecordHandle) null,
                        control_row.getPage().getSlotNumber(rechandle),
                        template, 
                        (FetchDescriptor) null,
                        true);

                // compare the 2 rows, and if they are the same then the raw 
                // store has the right page and record and there is no work to
                // be done (this is usual case).
                compare_result = ControlRow.CompareIndexRowToKey(
                    template, logged_index_row_template, 
                    logged_index_row_template.length, 1, 
                    open_btree.getColumnSortOrderInfo());
            }

            if (compare_result == 0)
            {
                ret_page = control_row.getPage();
            }
            else
            {
                // if the 2 don't compare equal, search the btree from the root 
                // for the logged row, find the leaf, reset the row for the raw 
                // store, and return the new page latched.
                
                // Create the objects needed for the insert.
                SearchParameters sp = new SearchParameters(
                        logged_index_row_template, ScanController.GE, 
                        template, open_btree, false);

                control_row.release();
                control_row = null;
                control_row = 
                    ControlRow.Get(open_btree, BTree.ROOTPAGEID).search(sp);

                if (!sp.resultExact)
                {
                    if (SanityManager.DEBUG)
                    {
                        SanityManager.THROWASSERT(
                            "B2IUndo - could not find key being searched for:" +
                            ";key = " + 
                                RowUtil.toString(logged_index_row_template) + 
                            ";sp = " + sp +
                            "control_row = " + control_row +
                            "control_row.debugPage() = " +
                                control_row.debugPage(open_btree) +
                            "control_row.getPage() = " + 
                                control_row.getPage());
                    }

                    throw StandardException.newException(
                            SQLState.BTREE_ROW_NOT_FOUND_DURING_UNDO);
                }
                else
                {
                    RecordHandle rh = 
                        control_row.getPage().fetchFromSlot(
                            (RecordHandle) null,
                            sp.resultSlot, new DataValueDescriptor[0], 
                            (FetchDescriptor) null,
                            true);

                    pageOp.resetRecordHandle(rh);

                    ret_page = control_row.getPage();
                }
            }
            ok_exit = true;
        }
        finally
        {
            //System.out.println("B2iUndo returning with rec handle: " +  
             //                   pageOp.getRecordHandle());
            if ((!ok_exit) && (control_row != null))
                control_row.release();
        }

        return(ret_page);
    }

	/**
		Return my format identifier.

		@see org.apache.derby.iapi.services.io.TypedFormat#getTypeFormatId
	*/
	public int getTypeFormatId() 
    {
		return StoredFormatIds.ACCESS_B2IUNDO_V1_ID;
	}

	/**
    This object has no state, so nothing to write.*/

	public void writeExternal(ObjectOutput out) throws IOException
    {
        return;
	}

	/**
	Restore the in-memory representation from the stream.

    This object has no state, so nothing to restore.
	@exception ClassNotFoundException Thrown if the stored representation is
	serialized and a class named in the stream could not be found.

	@see java.io.Externalizable#readExternal
	*/
	public void readExternal(ObjectInput in)
		throws IOException, ClassNotFoundException
	{
        return;
	}
	public void readExternal(ArrayInputStream in)
		throws IOException, ClassNotFoundException
	{
        return;
	}
}
