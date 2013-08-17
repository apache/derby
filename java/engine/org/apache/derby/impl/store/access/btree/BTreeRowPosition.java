/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeRowPosition

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

package org.apache.derby.impl.store.access.btree;


import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Page;

import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.FetchDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.store.access.conglomerate.RowPosition;

/**

**/

public class BTreeRowPosition extends RowPosition
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    public    DataValueDescriptor[] current_positionKey;
    public    LeafControlRow        current_leaf;
    protected LeafControlRow        next_leaf;
    public    DataValueDescriptor[] current_lock_template;
    public    RowLocation           current_lock_row_loc;

    /** The scan that owns this position object. */
    private final BTreeScan parent;

    /**
     * The version number of the leaf page when this position was saved by
     * key. Only valid if {@link #current_positionKey} is non-null. This value
     * is used to decide whether repositioning should be performed by using
     * the key, or if {@link #current_rh} could be used directly.
     */
    long versionWhenSaved;

    /** Cached template for saving this position by key. */
    private DataValueDescriptor[] positionKey_template;

    /**
     * Cached fetch descriptor that can be used to fetch the key columns that
     * are not already fetched by the scan. The fetch descriptor is used when
     * this position is about to be saved by its full key.
     */
    private FetchDescriptor savedFetchDescriptor;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public BTreeRowPosition(BTreeScan parent)
    {
        this.parent = parent;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */
    public void init()
    {
        super.init();

        current_leaf        = null;
        current_positionKey = null;
    }

    public final void unlatch()
    {
        // This method is never called for a BTreeRowPosition. If it is ever
        // used, make sure that the key is saved first, unless the scan won't
        // use that page again. DERBY-2991
        if (SanityManager.DEBUG) {
            SanityManager.THROWASSERT("Did you really call me?!?");
        }
        if (current_leaf != null)
        {
            current_leaf.release();
            current_leaf = null;
        }
        current_slot = Page.INVALID_SLOT_NUMBER;
    }

    /**
     * Save this position by key and release the latch on the current leaf.
     * @throws StandardException if an error occurs while saving the position
     * @see BTreeScan#savePositionAndReleasePage()
     */
    public void saveMeAndReleasePage() throws StandardException {
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(parent.scan_position == this);
        }
        parent.savePositionAndReleasePage();
    }

    /**
     * Get a template into which the position key can be copied. The value
     * is cached, so two calls to this method on the same object will return
     * the same object.
     *
     * @return an array into which the position key can be copied
     * @throws StandardException if an error occurs while allocating the
     * template array
     */
    DataValueDescriptor[] getKeyTemplate() throws StandardException {
        if (positionKey_template == null) {
            positionKey_template = parent.getRuntimeMem().
                    get_row_for_export(parent.getRawTran());
        }
        return positionKey_template;
    }

    /**
     * Get a fetch descriptor that can be used to fetch the missing columns
     * in a partial key. The fetch descriptor is only created on the first
     * call to this method. The returned descriptor will be cached, so
     * subsequent calls will return the same descriptor and the arguments
     * to this method should be the same between invokations.
     *
     * @param vcols an array which tells which columns the partial key contains
     * (valid columns have non-zero values in the array)
     * @param fullLength the length of the full key to create a fetch
     * descriptor for (may be greater than {@code vcols.length})
     * @return a fetch descriptor
     */
    FetchDescriptor getFetchDescriptorForSaveKey(int[] vcols, int fullLength) {
        if (savedFetchDescriptor == null) {
            FormatableBitSet columns = new FormatableBitSet(fullLength);
            for (int i = 0; i < vcols.length; i++) {
                if (vcols[i] == 0) {
                    // partial key does not have a valid value for this
                    // column, add it to the set of columns to fetch
                    columns.set(i);
                }
            }
            // also fetch the columns behind the ones in the partial key
            for (int i = vcols.length; i < fullLength; i++) {
                columns.set(i);
            }
            savedFetchDescriptor =
                    new FetchDescriptor(fullLength, columns, null);
        }

        // Verify that the cached fetch descriptor matches the arguments
        // (will fail if this method is not called with the same parameters
        // as when the descriptor was created and cached).
        if (SanityManager.DEBUG) {
            FormatableBitSet fetchCols = savedFetchDescriptor.getValidColumns();
            SanityManager.ASSERT(fullLength == fetchCols.size());
            for (int i = 0; i < vcols.length; i++) {
                SanityManager.ASSERT((vcols[i] == 0) == fetchCols.get(i));
            }
            for (int i = vcols.length; i < fullLength; i++) {
                SanityManager.ASSERT(fetchCols.get(i));
            }
        }

        return savedFetchDescriptor;
    }

    public final String toString()
    {
        String ret_string = null;

        if (SanityManager.DEBUG)
        {
            ret_string = 
                super.toString() + 
                "current_positionKey = " + current_positionKey + 
                ";key = " + RowUtil.toString(current_positionKey) + 
                ";next_leaf" + next_leaf + 
                ";current_leaf" + current_leaf;
        }

        return(ret_string);
    }


    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
