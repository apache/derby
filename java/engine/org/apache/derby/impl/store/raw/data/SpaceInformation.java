/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.raw.data
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.store.raw.data;

import org.apache.derby.iapi.store.access.SpaceInfo;
/**

Manage the result information from a single call to
ConglomerateController.getSpaceInfo().
<p>

**/

public class SpaceInformation implements SpaceInfo
{

    private long numAllocatedPages;
    private long numFreePages;
    private long numUnfilledPages;
    private int pageSize;

    public SpaceInformation(
        long numAllocatedPages,
        long numFreePages,
        long numUnfilledPages)
    {
        this.numAllocatedPages = numAllocatedPages;
        this.numFreePages = numFreePages;
        this.numUnfilledPages = numUnfilledPages;
    }

    /**
     * Get the estimated number of allocated pages
     **/
    public long getNumAllocatedPages()
    {
        return numAllocatedPages;
    }

    /**
     * Get the estimated number of free pages
     **/
    public long getNumFreePages()
    {
        return numFreePages;
    }

    /**
     * Get the estimated number of unfilled pages
     **/
    public long getNumUnfilledPages()
    {
        return numUnfilledPages;
    }

    /*
    Get the page size for the conglomerate.
    */
    public int getPageSize()
    {
        return pageSize;
    }

    /*
    record the page size for the conglomerate.
    */
    public void setPageSize(int pageSize)
    {
        this.pageSize = pageSize;
    }
}
