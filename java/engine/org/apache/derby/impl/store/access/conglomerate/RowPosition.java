/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.conglomerate
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

/**

Just an easy way to pass information back and forth about current position of
a row in a table.

**/

public class RowPosition
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    public Page            current_page; 
    public RecordHandle    current_rh;
    public int             current_slot;
    public boolean         current_rh_qualified;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    public RowPosition()
    {
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
        current_page            = null;
        current_rh              = null;
        current_slot            = Page.INVALID_SLOT_NUMBER;
        current_rh_qualified    = false;
    }

    public final void positionAtNextSlot()
    {
        current_slot++;
        current_rh   = null;
    }

    public void unlatch()
    {
        if (current_page != null)
        {
            current_page.unlatch();
            current_page = null;
        }
        current_slot = Page.INVALID_SLOT_NUMBER;
    }

    public String toString()
    {
        String ret_string = null;

        if (SanityManager.DEBUG)
        {
            ret_string = 
                ";current_slot=" + current_slot +
                ";current_rh=" + current_rh +
                ";current_page=" + current_page;
        }

        return(ret_string);
    }

    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
