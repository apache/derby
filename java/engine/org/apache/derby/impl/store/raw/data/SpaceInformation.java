/*

   Derby - Class org.apache.derby.impl.store.raw.data.SpaceInformation

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
