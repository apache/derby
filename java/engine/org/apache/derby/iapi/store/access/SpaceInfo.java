/*

   Derby - Class org.apache.derby.iapi.store.access.SpaceInfo

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
