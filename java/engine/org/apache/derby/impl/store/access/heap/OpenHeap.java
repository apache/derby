/*

   Derby - Class org.apache.derby.impl.store.access.heap.OpenHeap

   Copyright 1998, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.store.access.heap;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.impl.store.access.conglomerate.OpenConglomerate;

/**

**/

class OpenHeap extends OpenConglomerate
{
    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */
    public int[] getFormatIds()
    {
        return(((Heap) getConglomerate()).format_ids);
    }

    /**
     * Return an "empty" row location object of the correct type.
     * <p>
     *
	 * @return The empty Rowlocation.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public RowLocation newRowLocationTemplate()
		throws StandardException
	{
		if (getContainer() == null)
        {
			throw StandardException.newException(
                    SQLState.HEAP_IS_CLOSED, 
                    getConglomerate().getId());
        }

		return new HeapRowLocation();
	}
}
