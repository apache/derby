/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeRowPosition

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

package org.apache.derby.impl.store.access.btree;


import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Page;

import org.apache.derby.iapi.store.access.RowUtil;

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
    public    long                  current_scan_pageno;
    public    LeafControlRow        current_leaf;
    protected LeafControlRow        next_leaf;
    public    DataValueDescriptor[] current_lock_template;
    public    RowLocation           current_lock_row_loc;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public BTreeRowPosition()
    {
        super();
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
        if (current_leaf != null)
        {
            current_leaf.release();
            current_leaf = null;
        }
        current_slot = Page.INVALID_SLOT_NUMBER;
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
                ";current_scan_pageno" + current_scan_pageno + 
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
