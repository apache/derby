/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access.conglomerate
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.store.access.conglomerate;

import java.util.Properties;
import org.apache.derby.catalog.UUID;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.SortCostController;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

/**

  The factory interface for all sort access methods.

**/

public interface SortFactory extends MethodFactory
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;
	/**
	Used to identify this interface when finding it with the Monitor.
	**/
	public static final String MODULE = 
	  "org.apache.derby.iapi.store.access.conglomerate.SortFactory";

	/**
	Create the sort and return a sort object for it.

 	@exception StandardException if the sort could not be
	opened for some reason, or if an error occurred in one of
	the lower level modules.

	**/
	Sort createSort(
    TransactionController   tran,
    int                     segment,
    Properties              implParameters,
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    SortObserver          	sortObserver,
    boolean                 alreadyInOrder,
    long                    estimatedRows,
    int                     estimatedRowSize)
        throws StandardException;

    /**
     * Return an open SortCostController.
     * <p>
     * Return an open SortCostController which can be used to ask about 
     * the estimated costs of SortController() operations.
     * <p>
     *
	 * @return The open StoreCostController.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see  org.apache.derby.iapi.store.access.StoreCostController
     **/
    SortCostController openSortCostController()
		throws StandardException;
}
