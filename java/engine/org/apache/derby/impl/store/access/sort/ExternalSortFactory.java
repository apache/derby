/*

   Derby - Class org.apache.derby.impl.store.access.sort.ExternalSortFactory

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

package org.apache.derby.impl.store.access.sort;

import java.util.Properties;

import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.services.monitor.ModuleSupportable;
import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.property.PropertyUtil;
import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.conglomerate.MethodFactory;
import org.apache.derby.iapi.store.access.conglomerate.Sort;
import org.apache.derby.iapi.store.access.conglomerate.SortFactory;

import org.apache.derby.iapi.store.access.SortObserver;
import org.apache.derby.iapi.store.access.SortCostController;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.uuid.UUIDFactory;

import org.apache.derby.catalog.UUID;

/**

**/

public class ExternalSortFactory implements 
    SortFactory, ModuleControl, ModuleSupportable, SortCostController
{

	private boolean userSpecified; // did the user specify sortBufferMax
	private int defaultSortBufferMax; 
	private int sortBufferMax;

	private static final String IMPLEMENTATIONID = "sort external";
	private static final String FORMATUUIDSTRING = "D2976090-D9F5-11d0-B54D-00A024BF8879";
	private UUID formatUUID = null;
	private static final int DEFAULT_SORTBUFFERMAX = 1024;
	private static final int MINIMUM_SORTBUFFERMAX = 4;

	protected static final int DEFAULT_MEM_USE = 1024*1024; // aim for about 1Meg
	// how many sort runs to combined into a larger sort run
	protected static final int DEFAULT_MAX_MERGE_RUN = 1024; 

	// sizeof Node + reference to Node + 12 bytes tax
	private static final int SORT_ROW_OVERHEAD = 8*4+12; 


	/*
	** Methods of MethodFactory
	*/

	/**
	There are no default properties for the external sort..
	@see MethodFactory#defaultProperties
	**/
	public Properties defaultProperties()
	{
		return new Properties();
	}

	/**
	@see MethodFactory#supportsImplementation
	**/
	public boolean supportsImplementation(String implementationId)
	{
		return implementationId.equals(IMPLEMENTATIONID);
	}

	/**
	@see MethodFactory#primaryImplementationType
	**/
	public String primaryImplementationType()
	{
		return IMPLEMENTATIONID;
	}

	/**
	@see MethodFactory#supportsFormat
	**/
	public boolean supportsFormat(UUID formatid)
	{
		return formatid.equals(formatUUID);
	}

	/**
	@see MethodFactory#primaryFormat
	**/
	public UUID primaryFormat()
	{
		return formatUUID;
	}

	/*
	** Methods of SortFactory
	*/

	/**
	Create a sort.
	This method could choose among different sort options, 
	depending on the properties etc., but currently it always
	returns a merge sort.
	@see SortFactory#createSort
	**/
	public Sort createSort(
    TransactionController   tran,
    int                     segment,
    Properties              implParameters,
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    SortObserver          	sortObserver,
    boolean                 alreadyInOrder,
    long                    estimatedRows,
    int                     estimatedRowSize)
        throws StandardException
	{
		MergeSort sort = new MergeSort();

        // RESOLVE - mikem change this to use estimatedRows and 
        // estimatedRowSize to come up with a smarter number for sortBufferMax
        // than a fixed number of rows.  At least 2 possibilities:
        //     1) add sortBufferMaxMem which would be the amount of memory
        //        the sorter could use, and then just pick the number of 
        //        rows as (sortBufferMaxMem / (estimatedRows * estimatedRowSize)
        //     2) add sortBufferUsePercentFree.  This would be how much of
        //        the current free memory can the current sort use.
        //

		if (!userSpecified)	
		{
			// derby.storage.sortBufferMax is not specified by the
			// user, use default or try to figure out a reasonable sort
			// size.

			// if we have some idea on row size, set sort approx 1 meg of
			// memory sort.
			if (estimatedRowSize > 0)
			{
				// 
				// for each column, there is a reference from the key array and
				//   the 4 bytes reference to the column object plus 12 bytes
				//   tax on the  column object  
				// for each row, SORT_ROW_OVERHEAD is the Node and 4 bytes to
				// point to the column array and 4 for alignment
				//
				estimatedRowSize += SORT_ROW_OVERHEAD +
					(template.length*(4+12)) + 8; 
				sortBufferMax = DEFAULT_MEM_USE/estimatedRowSize;
			}
			else
			{
				sortBufferMax = defaultSortBufferMax;
			}
			
			// if there are barely more rows than sortBufferMax, use 2
			// smaller runs of similar size instead of one larger run
			//
			// 10% slush is added to estimated Rows to catch the case where
			// estimated rows underestimate the actual number of rows by 10%.
			//
			if (estimatedRows > sortBufferMax &&
				(estimatedRows*1.1) < sortBufferMax*2)
				sortBufferMax = (int)(estimatedRows/2 + estimatedRows/10);

			// Make sure it is at least the minimum sort buffer size
			if (sortBufferMax < MINIMUM_SORTBUFFERMAX)
				sortBufferMax = MINIMUM_SORTBUFFERMAX;
		}
		else
		{
			// if user specified derby.storage.sortBufferMax, use it.
				sortBufferMax = defaultSortBufferMax;
		}

		if (SanityManager.DEBUG)
        {
            if (SanityManager.DEBUG_ON("SortTuning"))
            {
                SanityManager.DEBUG("SortTuning",
                    "sortBufferMax = " + sortBufferMax + 
                    " estimatedRows = " + estimatedRows +
                    " estimatedRowSize = " + estimatedRowSize +
                    " defaultSortBufferMax = " + defaultSortBufferMax);
            }
        }

		sort.initialize(
            template, columnOrdering, sortObserver, 
            alreadyInOrder, estimatedRows, sortBufferMax);
		return sort;
	}

    /**
     * Return an open SortCostController.
     * <p>
     * Return an open SortCostController which can be used to ask about 
     * the estimated costs of SortController() operations.
     * <p>
     *
	 * @return The open SortCostController.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see SortCostController
     **/
    public SortCostController openSortCostController()
		throws StandardException
    {
        return(this);
    }

	/*
	** Methods of SortCostController
	*/

    public void close()
    {
        // nothing to do.
    }

    /**
     * Short one line description of routine.
     * <p>
     * The sort algorithm is a N * log(N) algorithm.  The following numbers
     * on a PII, 400 MHZ machine, jdk117 with jit, insane.zip.  This test
     * is a simple "select * from table order by first_int_column.  I then
     * subtracted the time it takes to do "select * from table" from the
     * result.
     *
     * number of rows       elaspsed time in seconds
     * --------------       -----------------------------
     * 1000                  0.20
     * 10000                10.5
     * 100000               80.0
     *
     * We assume that the formula for sort performance is of the form:
     * performance = K * N * log(N).  Solving the equation for the 1000
     * and 100000 case we come up with:
     *
     * performance = 1 + 0.08 N ln(n)
	 *
	 * NOTE: Apparently, these measurements were done on a faster machine
	 * than was used for other performance measurements used by the optimizer.
	 * Experiments show that the 0.8 multiplier is off by a factor of 4
	 * with respect to other measurements (such as the time it takes to
	 * scan a conglomerate).  I am correcting the formula to use 0.32
	 * rather than 0.08.
	 *
	 *					-	Jeff
     *
     * <p>
     * RESOLVE (mikem) - this formula is very crude at the moment and will be
     * refined later.  known problems:
     * 1) internal vs. external sort - we know that the performance of sort
     *    is discontinuous when we go from an internal to an external sort.
     *    A better model is probably a different set of contants for internal
     *    vs. external sort and some way to guess when this is going to happen.
     * 2) current row size is never considered but is critical to performance.
     * 3) estimatedExportRows is not used.  This is a critical number to know
     *    if an internal vs. an external sort will happen.  
     *
     * <p>
     *
	 * @return The identifier to be used to open the conglomerate later.
     *
     * @param param1 param1 does this.
     * @param param2 param2 does this.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public double getSortCost(
    DataValueDescriptor[]   template,
    ColumnOrdering          columnOrdering[],
    boolean                 alreadyInOrder,
    long                    estimatedInputRows,
    long                    estimatedExportRows,
    int                     estimatedRowSize)
        throws StandardException
    {
		/* Avoid taking the log of 0 */
		if (estimatedInputRows == 0)
			return 0.0;

        // RESOLVE - come up with some real benchmark.  For now the cost
        // of sort is 3 times the cost of scanning the data.

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(estimatedInputRows  >= 0);
            SanityManager.ASSERT(estimatedExportRows >= 0);
        }

        double ret_val = 
            1 + 
            ((0.32) * (estimatedInputRows) * Math.log(estimatedInputRows));

        return(ret_val);
    }

	/*
	** Methods of ModuleControl.
	*/

	public boolean canSupport(Properties startParams) {

        if (startParams == null)
            return false; 

		String impl = startParams.getProperty("derby.access.Conglomerate.type");
		if (impl == null)
			return false;

		return supportsImplementation(impl);
	}


	public void	boot(boolean create, Properties startParams)
		throws StandardException
	{
		// Find the UUID factory.
		UUIDFactory uuidFactory = Monitor.getMonitor().getUUIDFactory();

		// Make a UUID that identifies this sort's format.
		formatUUID = uuidFactory.recreateUUID(FORMATUUIDSTRING);

		// See if there's a new maximum sort buffer size.
		defaultSortBufferMax = PropertyUtil.getSystemInt("derby.storage.sortBufferMax",
								0, Integer.MAX_VALUE, 0);

		// if defaultSortBufferMax is 0, the user did not specify
		// sortBufferMax, then just set it to DEFAULT_SORTBUFFERMAX.
		// if defaultSortBufferMax is not 0, the user specified sortBufferMax,
		// do not override it.
		if (defaultSortBufferMax == 0)
		{
			userSpecified = false;
			defaultSortBufferMax = DEFAULT_SORTBUFFERMAX;
		}
		else
		{
			userSpecified = true;
			if (defaultSortBufferMax < MINIMUM_SORTBUFFERMAX)
				defaultSortBufferMax = MINIMUM_SORTBUFFERMAX;
		}

	}

	public void	stop()
	{
	}

}
