/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access;

/**

Manage the result information from a single call to
ConglomerateController.getSpaceInfo().
<p>
@see org.apache.derby.iapi.store.access.ConglomerateController

**/

public interface SpaceInfo
{
    /**
     * Get the estimated number of allocated pages
     **/
    public long getNumAllocatedPages();

    /**
     * Get the estimated number of free pages
     **/
    public long getNumFreePages();

    /**
     * Get the estimated number of unfilled pages
     **/
    public long getNumUnfilledPages();

    /**
     * Get the page size
     **/
    public int getPageSize();

}
