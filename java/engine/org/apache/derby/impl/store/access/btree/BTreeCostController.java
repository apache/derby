/*

   Derby - Class org.apache.derby.impl.store.access.btree.BTreeCostController

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

package org.apache.derby.impl.store.access.btree;

import org.apache.derby.iapi.reference.Property;
import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.access.conglomerate.LogicalUndo;
import org.apache.derby.iapi.store.access.conglomerate.TransactionManager;

import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.RowUtil;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.StoreCostResult;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.store.raw.ContainerHandle;
import org.apache.derby.iapi.store.raw.LockingPolicy;
import org.apache.derby.iapi.store.raw.RawStoreFactory;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.RowLocation;


import org.apache.derby.iapi.services.io.FormatableBitSet;
import java.util.Properties;

/**

The StoreCostController interface provides methods that an access client
(most likely the system optimizer) can use to get store's estimated cost of
various operations on the conglomerate the StoreCostController was opened
for.
<p>
It is likely that the implementation of StoreCostController will open 
the conglomerate and will leave the conglomerate open until the
StoreCostController is closed.  This represents a significant amount of
work, so the caller if possible should attempt to open the StoreCostController
once per unit of work and rather than close and reopen the controller.  For
instance if the optimizer needs to cost 2 different scans against a single
conglomerate, it should use one instance of the StoreCostController.
<p>
The locking behavior of the implementation of a StoreCostController is
undefined, it may or may not get locks on the underlying conglomerate.  It
may or may not hold locks until end of transaction.  
An optimal implementation will not get any locks on the underlying 
conglomerate, thus allowing concurrent access to the table by a executing
query while another query is optimizing.
<p>
@see TransactionController#openStoreCost

**/

public class BTreeCostController extends OpenBTree 
    implements StoreCostController
{

    // 1.5 numbers on mikem old machine:
    //
    // The magic numbers are based on the following benchmark results:
    //
    //                                         no col   one int col  all cols
    //                                         ------   -----------  --------
    //100 byte heap fetch by row loc, cached   0.3625     0.5098     0.6629
    //100 byte heap fetch by row loc, uncached 1.3605769  1.5168269  1.5769231
    //4 byte   heap fetch by row loc, cached   0.3745     0.4016     0.3766
    //4 byte   heap fetch by row loc, uncached 4.1938777  3.5714285  4.4897957
    //
    //                                 no col    one int col  all cols
    //                                 ------    -----------  --------
    //Int col one level btree
    //  fetch by exact key, cached     0.781     1.012         0.42
    //  fetch by exact key, sort merge 1.081     1.221         0.851
    //  fetch by exact key, uncached   0.0       0.0           0.0
    //Int col two level btree
    //  fetch by exact key, cached     1.062     1.342         0.871
    //  fetch by exact key, sort merge 1.893     2.273         1.633
    //  fetch by exact key, uncached   5.7238097 5.3428574     4.7714286
    //String key one level btree
    //  fetch by exact key, cached     1.082     0.811         0.781
    //  fetch by exact key, sort merge 1.572     1.683         1.141
    //  fetch by exact key, uncached   0.0       0.0           0.0
    //String key two level btree
    //  fetch by exact key, cached     2.143     2.664         1.953
    //  fetch by exact key, sort merge 3.775     4.116         3.505
    //  fetch by exact key, uncached   4.639474  5.0052633     4.4289474

    // mikem new machine - insane, codeline, non-jit 1.1.7 numbers
    //
    //                                         no col   one int col  all cols
    //                                         ------   -----------  --------
    //100 byte heap fetch by row loc, cached   0.1662    0.4597      0.5618
    //100 byte heap fetch by row loc, uncached 0.7565947 1.2601918   1.6690648
    //4 byte   heap fetch by row loc, cached   0.1702    0.1983      0.1903
    //4 byte   heap fetch by row loc, uncached 1.5068493 1.3013699   1.6438357
    //
    //                                 no col    one int col  all cols
    //                                 ------    -----------  --------
    // Int col one level btree
    //   fetch by exact key, cached     0.271    0.511        0.33
    //   fetch by exact key, sort merge 0.691    0.921        0.771
    //   fetch by exact key, uncached   0.0      0.0          0.0
    // Int col two level btree
    //   fetch by exact key, cached     0.541    0.711        0.561
    //   fetch by exact key, sort merge 1.432    1.682        1.533
    //   fetch by exact key, uncached   3.142857 3.6285715    3.2380953
    // String key one level btree
    //   fetch by exact key, cached     0.611    0.851        0.701
    //   fetch by exact key, sort merge 1.051    1.272        1.122
    //   fetch by exact key, uncached   0.0      0.0          0.0
    // String key two level btree
    //   fetch by exact key, cached     1.532    1.843        1.622
    //   fetch by exact key, sort merge 2.844    3.155        2.984
    //   fetch by exact key, uncached   3.4      3.636842     3.531579
    // 


    // The following costs are search costs to find a row on a leaf, use
    // the heap costs to determine scan costs, for now ignore qualifier 
    // application and stop comparisons.
    // I used the int key, 2 level numbers divided by 2 to get per level.
    
    private static final double 
        BTREE_CACHED_FETCH_BY_KEY_PER_LEVEL    = (0.541 / 2);

    private static final double 
        BTREE_SORTMERGE_FETCH_BY_KEY_PER_LEVEL = (1.432 / 2);

    private static final double 
        BTREE_UNCACHED_FETCH_BY_KEY_PER_LEVEL  = (3.143 / 2);

    // saved values passed to init().
    TransactionManager  init_xact_manager;
    Transaction         init_rawtran;
    Conglomerate        init_conglomerate;

    /**
     * Only lookup these estimates from raw store once.
     **/
    long    num_pages;
    long    num_rows;
    long    page_size;
    int     tree_height;

    /* Constructors for This class: */

    public BTreeCostController()
    {
    }

    /* Private/Protected methods of This class: */

    /**
     * Initialize the cost controller.
     * <p>
     * Save initialize parameters away, and open the underlying container.
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param xact_manager access manager transaction.
     * @param sementid     The id of the segment where container can be found.
     * @param rawtran      Raw store transaction.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void init(
    TransactionManager  xact_manager,
    BTree               conglomerate,
    Transaction         rawtran)
        throws StandardException
    {
        super.init(
            xact_manager, 
            xact_manager, 
            (ContainerHandle) null,         // open the btree.
            rawtran, 
            false,
            ContainerHandle.MODE_READONLY,
            TransactionManager.MODE_NONE,
            (BTreeLockingPolicy) null,      // RESOLVE (mikem) - this means
                                            // no locks during costing - will
                                            // that work?????
            conglomerate, 
            (LogicalUndo) null,             // read only, so no undo necessary
            (DynamicCompiledOpenConglomInfo) null);

        // look up costs from raw store.  For btrees these numbers are out
        // of whack as they want to be leaf specific numbers but they include
        // every page branch and leafs.
        num_pages = this.container.getEstimatedPageCount(/* unused flag */ 0);

        // subtract one row for every page to account for internal control row
        // which exists on every page.
        num_rows  = 
            this.container.getEstimatedRowCount(/*unused flag*/ 0) - num_pages;

        Properties prop = new Properties();
        prop.put(Property.PAGE_SIZE_PARAMETER, "");
        this.container.getContainerProperties(prop);
        page_size = 
            Integer.parseInt(prop.getProperty(Property.PAGE_SIZE_PARAMETER));

        tree_height = getHeight();

        return;
    }

    /* Public Methods of This class: */

    /**
     * Close the controller.
     * <p>
     * Close the open controller.  This method always succeeds, and never 
     * throws any exceptions. Callers must not use the StoreCostController 
     * Cost controller after closing it; they are strongly advised to clear
     * out the scan controller reference after closing.
     * <p>
     **/
    public void close()
        throws StandardException
    {
        super.close();
    }

    /**
     * Return the cost of calling ConglomerateController.fetch().
     * <p>
     * Return the estimated cost of calling ConglomerateController.fetch()
     * on the current conglomerate.  This gives the cost of finding a record
     * in the conglomerate given the exact RowLocation of the record in
     * question. 
     * <p>
     * The validColumns parameter describes what kind of row 
     * is being fetched, ie. it may be cheaper to fetch a partial row than a 
     * complete row.
     * <p>
     *
     *
	 * @param validColumns    A description of which columns to return from
     *                        row on the page into "templateRow."  templateRow,
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
     *
     * @param access_type     Describe the type of access the query will be
     *                        performing to the ConglomerateController.  
     *
     *                        STORECOST_CLUSTERED - The location of one fetch
     *                            is likely clustered "close" to the next 
     *                            fetch.  For instance if the query plan were
     *                            to sort the RowLocations of a heap and then
     *                            use those RowLocations sequentially to 
     *                            probe into the heap, then this flag should
     *                            be specified.  If this flag is not set then
     *                            access to the table is assumed to be
     *                            random - ie. the type of access one gets 
     *                            if you scan an index and probe each row
     *                            in turn into the base table is "random".
     *
     *
	 * @return The cost of the fetch.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
    public double getFetchFromRowLocationCost(
    FormatableBitSet      validColumns,
    int         access_type)
		throws StandardException
    {
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

    /**
     * Return the cost of exact key lookup.
     * <p>
     * Return the estimated cost of calling ScanController.fetch()
     * on the current conglomerate, with start and stop positions set such
     * that an exact match is expected.
     * <p>
     * This call returns the cost of a fetchNext() performed on a scan which
     * has been positioned with a start position which specifies exact match
     * on all keys in the row.
     * <p>
     * Example:
     * <p>
     * In the case of a btree this call can be used to determine the cost of
     * doing an exact probe into btree, giving all key columns.  This cost
     * can be used if the client knows it will be doing an exact key probe
     * but does not have the key's at optimize time to use to make a call to
     * getScanCost()
     * <p>
     *
     *
	 * @param validColumns    A description of which columns to return from
     *                        row on the page into "templateRow."  templateRow,
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
     *
     * @param access_type     Describe the type of access the query will be
     *                        performing to the ScanController.  
     *
     *                        STORECOST_CLUSTERED - The location of one scan
     *                            is likely clustered "close" to the previous 
     *                            scan.  For instance if the query plan were
     *                            to used repeated "reopenScan()'s" to probe
     *                            for the next key in an index, then this flag
     *                            should be be specified.  If this flag is not 
     *                            set then each scan will be costed independant
     *                            of any other predicted scan access.
     *
	 * @return The cost of the fetch.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
    public double getFetchFromFullKeyCost(
    FormatableBitSet      validColumns,
    int         access_type)
		throws StandardException
    {
        double ret_cost;

        if ((access_type & StoreCostController.STORECOST_CLUSTERED) == 0)
        {
            // uncached fetch
            ret_cost = BTREE_UNCACHED_FETCH_BY_KEY_PER_LEVEL;
        }
        else
        {
            ret_cost = BTREE_SORTMERGE_FETCH_BY_KEY_PER_LEVEL;
        }
        ret_cost *= tree_height;

        return(ret_cost);
    }


    /**
     * Calculate the cost of a scan.
     * <p>
     * Cause this object to calculate the cost of performing the described
     * scan.  The interface is setup such that first a call is made to
     * calcualteScanCost(), and then subsequent calls to accessor routines
     * are made to get various pieces of information about the cost of
     * the scan.
     * <p>
     * For the purposes of costing this routine is going to assume that 
     * a page will remain in cache between the time one next()/fetchNext()
     * call and a subsequent next()/fetchNext() call is made within a scan.
     * <p>
     * The result of costing the scan is placed in the "cost_result".  
     * The cost of the scan is stored by calling 
     * cost_result.setEstimatedCost(cost).
     * The estimated row count is stored by calling 
     * cost_result.setEstimatedRowCount(row_count).
     * <p>
     * The estimated cost of the scan assumes the caller will 
     * execute a fetchNext() loop for every row that qualifies between
     * start and stop position.  Note that this cost is different than
     * execution a next(),fetch() loop; or if the scan is going to be
     * terminated by client prior to reaching the stop condition.
     * <p>
     * The estimated number of rows returned from the scan 
     * assumes the caller will execute a fetchNext() loop for every 
     * row that qualifies between start and stop position.
     * <p>
     *
     *
     * @param scan_type       The type of scan that will be executed.  There
     *                        are currently 2 types:
     *                        STORECOST_SCAN_NORMAL - scans will be executed
     *                        using the standard next/fetch, where each fetch
     *                        can retrieve 1 or many rows (if fetchNextGroup()
     *                        interface is used).
     *
     *                        STORECOST_SCAN_SET - The entire result set will
     *                        be retrieved using the the fetchSet() interface.
     *
     * @param row_count       Estimated total row count of the table.  The 
     *                        current system tracks row counts in heaps better
     *                        than btree's (btree's have "rows" which are not
     *                        user rows - branch rows, control rows), so 
     *                        if available the client should
     *                        pass in the base table's row count into this
     *                        routine to be used as the index's row count.
     *                        If the caller has no idea, pass in -1.
     *
     * @param group_size      The number of rows to be returned by a single
     *                        fetch call for STORECOST_SCAN_NORMAL scans.
     *
	 * @param forUpdate       Should be true if the caller intends to update 
     *                        through the scan.
     * 
	 * @param scanColumnList  A description of which columns to return from 
     *                        every fetch in the scan.  template, 
     *                        and scanColumnList work together
     *                        to describe the row to be returned by the scan - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a "row".
     * 
     * @param template        A prototypical row which the scan may use to
	 *                        maintain its position in the conglomerate.  Not 
     *                        all access method scan types will require this, 
     *                        if they don't it's ok to pass in null.
     *                        In order to scan a conglomerate one must 
     *                        allocate 2 separate "row" templates.  The "row" 
     *                        template passed into openScan is for the private
     *                        use of the scan itself, and no access to it
     *                        should be made by the caller while the scan is 
     *                        still open.  Because of this the scanner must 
     *                        allocate another "row" template to hold the 
     *                        values returned from fetch().  Note that this 
     *                        template must be for the full row, whether a 
     *                        partial row scan is being executed or not.
     *
	 * @param startKeyValue   An indexable row which holds a (partial) key 
     *                        value which, in combination with the 
     *                        startSearchOperator, defines the starting 
     *                        position of the scan.  If null, the starting
     *                        position of the scan is the first row of the 
     *                        conglomerate.  The startKeyValue must only
     *                        reference columns included in the scanColumnList.
     *
	 * @param startSearchOperation 
     *                        an operator which defines how the startKeyValue
     *                        is to be searched for.  If startSearchOperation 
     *                        is ScanController.GE, the scan starts on the 
     *                        first row which is greater than or equal to the 
	 *                        startKeyValue.  If startSearchOperation is 
     *                        ScanController.GT, the scan starts on the first
     *                        row whose key is greater than startKeyValue.  The
     *                        startSearchOperation parameter is ignored if the
     *                        startKeyValue parameter is null.
     *
	 * @param stopKeyValue    An indexable row which holds a (partial) key 
     *                        value which, in combination with the 
     *                        stopSearchOperator, defines the ending position
     *                        of the scan.  If null, the ending position of the
     *                        scan is the last row of the conglomerate.  The
     *                        stopKeyValue must only reference columns included
     *                        in the scanColumnList.
     *
	 * @param stopSearchOperation
     *                        an operator which defines how the stopKeyValue
     *                        is used to determine the scan stopping position. 
     *                        If stopSearchOperation is ScanController.GE, the
     *                        scan stops just before the first row which is
     *                        greater than or equal to the stopKeyValue.  If 
     *                        stopSearchOperation is ScanController.GT, the 
     *                        scan stops just before the first row whose key 
     *                        is greater than startKeyValue.  The
     *                        stopSearchOperation parameter is ignored if the
     *                        stopKeyValue parameter is null.
     *
     *                        
     * @param access_type     Describe the type of access the query will be
     *                        performing to the ScanController.  
     *
     *                        STORECOST_CLUSTERED - The location of one scan
     *                            is likely clustered "close" to the previous 
     *                            scan.  For instance if the query plan were
     *                            to used repeated "reopenScan()'s" to probe
     *                            for the next key in an index, then this flag
     *                            should be be specified.  If this flag is not 
     *                            set then each scan will be costed independant
     *                            of any other predicted scan access.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see RowUtil
     **/
	public void getScanCost(
    int                     scan_type,
    long                    row_count,
    int                     group_size,
    boolean                 forUpdate,
    FormatableBitSet                 scanColumnList,
    DataValueDescriptor[]   template,
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator,
    boolean                 reopen_scan,
    int                     access_type,
    StoreCostResult         cost_result)
        throws StandardException
    {
        float       left_of_start;
        float       left_of_stop;
        ControlRow  control_row = null;
        long        input_row_count = (row_count < 0 ? num_rows : row_count);

        try
        {
            // Find the starting page and row slot.
            if (startKeyValue == null)
            {
                left_of_start = 0;
            }
            else
            {
                // Search for the starting row.

                SearchParameters sp = new SearchParameters(
                    startKeyValue, 
                    ((startSearchOperator == ScanController.GE) ? 
                        SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH : 
                        SearchParameters.POSITION_RIGHT_OF_PARTIAL_KEY_MATCH),
                    template, this, true);

                control_row =
                    ControlRow.Get(this, BTree.ROOTPAGEID).search(sp);

                control_row.release();
                control_row = null;

                left_of_start = sp.left_fraction;
            }

            if (stopKeyValue == null)
            {
                left_of_stop = 1;
            }
            else
            {
                // Search for the stopping row.

                SearchParameters sp = 
                    new SearchParameters(
                        stopKeyValue, 
                        ((stopSearchOperator == ScanController.GE) ? 
                          SearchParameters.POSITION_LEFT_OF_PARTIAL_KEY_MATCH : 
                          SearchParameters.POSITION_RIGHT_OF_PARTIAL_KEY_MATCH),
                        template, this, true);

                control_row =
                    ControlRow.Get(this, BTree.ROOTPAGEID).search(sp);

                control_row.release();
                control_row = null;

                left_of_stop = sp.left_fraction;
            }

            // System.out.println(
              //   "\n\tleft_of_start = " + left_of_start +
                // "\n\tleft_of_stop  = " + left_of_stop);

            // what percentage of rows are between start and stop?

            float ret_fraction = left_of_stop - left_of_start;

            // If for some reason the stop position comes before the start
            // position, assume 0 rows will return from query.
            if (ret_fraction < 0)
                ret_fraction = 0;

            if (SanityManager.DEBUG)
                SanityManager.ASSERT(ret_fraction >= 0 && ret_fraction <= 1);

            float estimated_row_count = input_row_count * ret_fraction;

            // first the base cost of positioning on the first row in the scan.
            double cost = 
                getFetchFromFullKeyCost(scanColumnList, access_type);

            // add the base cost of bringing each page for the first time into
            // the cache.  This is basically the cost of bringing each leaf
            // uncached into the cache and reading the control row off of it.:
            cost += 
                (num_pages * ret_fraction) * BASE_UNCACHED_ROW_FETCH_COST;

            // Now some magic to try and figure out the cost of doing a
            // scan along the leaf level of the tree.  Mostly just assume
            // the costs are the same as the heap, and ignore qualifier
            // processing and stop row comparisons for now.

            // the base cost of getting each of the rows from a page assumed
            // to already be cached (by the scan fetch) - this is only for all
            // rows after the initial row on the page has been accounted for
            // under the BASE_UNCACHED_ROW_FETCH_COST cost.:
            long cached_row_count = ((long) estimated_row_count) - num_pages;
            if (cached_row_count < 0)
                cached_row_count = 0;

            if (scan_type == StoreCostController.STORECOST_SCAN_NORMAL)
                cost += cached_row_count * BASE_GROUPSCAN_ROW_COST;
            else
                cost += cached_row_count * BASE_HASHSCAN_ROW_FETCH_COST;

            // finally add the cost associated with the number of bytes in row:
            long row_size = 
                (input_row_count == 0) ? 
                    4 : (num_pages * page_size) / input_row_count;

            cost += 
                (estimated_row_count * row_size) * BASE_ROW_PER_BYTECOST;

            if (SanityManager.DEBUG)
            {
                if (cost < 0)
                    SanityManager.THROWASSERT("cost " + cost);

                if (estimated_row_count < 0)
                    SanityManager.THROWASSERT(
                        "estimated_row_count = " + estimated_row_count);
            }

            // return the cost
            cost_result.setEstimatedCost(cost);

            // RESOLVE - should we make sure this number is > 0?
            cost_result.setEstimatedRowCount(Math.round(estimated_row_count));
        }
        finally
        {
            if (control_row != null)
                control_row.release();
        }

        // System.out.println("BTreeCostController.getScanCost():" + 
          //   "\n\t cost = " + cost_result.getEstimatedCost() +
            // "\n\t rows = " + cost_result.getEstimatedRowCount());

        return;
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
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
	}
}
