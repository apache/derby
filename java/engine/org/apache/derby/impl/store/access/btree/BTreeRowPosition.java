/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.btree
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
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
