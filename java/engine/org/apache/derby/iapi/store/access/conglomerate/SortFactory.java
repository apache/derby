/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.SortFactory

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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
