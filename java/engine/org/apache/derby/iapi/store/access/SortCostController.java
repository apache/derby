/*

   Derby - Class org.apache.derby.iapi.store.access.SortCostController

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.types.DataValueDescriptor;

/**

The SortCostController interface provides methods that an access client
(most likely the system optimizer) can use to get sorter's estimated cost of
various operations on the SortController.
<p>
@see TransactionController#openSortCost
@see TransactionController#createSort
@see RowCountable

**/

import java.util.Properties;

public interface SortCostController
{
    /**
     * Close the controller.
     * <p>
     * Close the open controller.  This method always succeeds, and never 
     * throws any exceptions. Callers must not use the StoreCostController 
     * after closing it; they are strongly advised to clear
     * out the StoreCostController reference after closing.
     * <p>
     **/
    void close();


	/**
     * Calculate the cost of a sort.
     * <p>
     * The cost of a sort includes the time spent in the sorter inserting
     * the rows into the sort, and the time spent in the sorter returning the
     * rows.  Note that it does not include the cost of scanning the rows from
     * the source table, for insert into the sort.
     * <p>
     * Arguments to getSortCost(), should be the same as those to be passed to
     * TransactionController.createSort().
     *
     * @param template        A row which is prototypical for the sort.  All
     *                        rows inserted into the sort controller must have 
	 *                        exactly the same number of columns as the 
     *                        template row.  Every column in an inserted row
     *                        must have the same type as the corresponding
     *                        column in the template.
     *
     * @param columnOrdering  An array which specifies which columns 
     *                        participate in ordering - see interface 
     *                        ColumnOrdering for details.  The column
     *                        referenced in the 0th columnOrdering object is
     *                        compared first, then the 1st, etc.
     *
     * @param alreadyInOrder  Indicates that the rows inserted into the sort
     *                        controller will already be in order.  This is used
	 *                        to perform aggregation only.
     * 
     * @param estimatedInputRows   The number of rows that the caller estimates
     *                        will be inserted into the sort.  This number must
     *                        be >= 0.
     *
     * @param estimatedExportRows   The number of rows that the caller estimates
     *                        will be exported by the sorter.  For instance if
     *                        the sort is doing duplicate elimination and all
     *                        rows are expected to be duplicates then the 
     *                        estimatedExportRows would be 1.  If no duplicate
     *                        eliminate is to be done then estimatedExportRows 
     *                        would be the same as estimatedInputRows.  This 
     *                        number must be >= 0.
     *
     * @param estimatedRowSize The estimated average row size of the rows 
     *                         being sorted.  This is the client portion of the 
     *                         rowsize, it should not attempt to calculate 
     *                         Store's overhead.  -1 indicates that the caller
     *                         has no idea (and the sorter will use 100 bytes
     *                         in that case.  Used by the sort to make good 
     *                         choices about in-memory vs. external sorting, 
     *                         and to size merge runs.  The client is not
     *                         expected to estimate the per column/ per row 
     *                         overhead of raw store, just to make a guess
     *                         about the storage associated with each row 
     *                         (ie. reasonable estimates for some 
     *                         implementations would be 4 for int, 8 for long,
     *                         102 for char(100), 202 for varchar(200), a
     *                         number out of hat for user types, ...).
     * 
     * @return The cost of the sort.
     *
     * @exception  StandardException  Standard exception policy.
     *
	**/

	double getSortCost(
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    boolean                 alreadyInOrder,
    long                    estimatedInputRows,
    long                    estimatedExportRows,
    int                     estimatedRowSize)
        throws StandardException;

}
