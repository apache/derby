/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.RowPosition

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.store.raw.RecordHandle;

/**

Just an easy way to pass information back and forth about current position of
a row in a table.

**/

public class RowPosition
{
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
