/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.store.access.heap
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
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
