/*

   Derby - Class org.apache.derby.impl.store.access.sort.MergeSortInfo

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.store.access.SortInfo;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.i18n.MessageService;

import java.util.Vector;
import java.util.Properties;

/**

  This object provides performance information related to a sort.
  The information is accumulated during operations on a SortController() and
  then copied into this object and returned by a call to
  SortController.getSortInfo().

  @see org.apache.derby.iapi.store.access.SortController#getSortInfo()

**/
class MergeSortInfo implements SortInfo
{
    /**
     * Performance counters ...
     */

    private String  stat_sortType;
    // private long    stat_estimMemUsed;
    private int     stat_numRowsInput;
    private int     stat_numRowsOutput;
    private int     stat_numMergeRuns;
    private Vector  stat_mergeRunsSize;


    /* Constructors for This class: */
    MergeSortInfo(MergeInserter sort)
    {
        // copy perfomance state out of sort, to get a fixed set of stats
        stat_sortType               = sort.stat_sortType;
        // stat_estimMemUsed           = sort.estimatedMemoryUsed;
        stat_numRowsInput           = sort.stat_numRowsInput;
        stat_numRowsOutput          = sort.stat_numRowsOutput;
        stat_numMergeRuns           = sort.stat_numMergeRuns;
        stat_mergeRunsSize          = sort.stat_mergeRunsSize;
    }

    /**
     * Return all information gathered about the sort.
     * <p>
     * This routine returns a list of properties which contains all information
     * gathered about the sort.  If a Property is passed in, then that property
     * list is appended to, otherwise a new property object is created and
     * returned.
     * <p>
     * Not all sorts may support all properties, if the property is not
     * supported then it will not be returned.  The following is a list of
     * properties that may be returned:
     *
     *     sortType
     *         - type of the sort being performed:
     *           internal
     *           external
     *     numRowsInput
     *         - the number of rows input to the sort.  This
     *           number includes duplicates.
     *     numRowsOutput
     *         - the number of rows to be output by the sort.  This number
     *           may be different from numRowsInput since duplicates may not
     *           be output.
     *     numMergeRuns
     *         - the number of merge runs for the sort.
     *           Applicable to external sorts only.
     *           Note: when a SortController is closed, numMergeRuns may increase by 1, to
     *           reflect the additional merge run that may be created for
     *           any data still in the sort buffer.
     *     mergeRunsSize
     *         - the size (number of rows) of each merge run for the sort.
     *           Applicable to external sorts only.
     *           e.g. [3,3,2] indicates 3 merge runs, where the first two runs
     *           have 3 rows each, and the last run has 2 rows.
     *           Note: when a SortController is closed, this vector may get an
     *           additional element, to reflect the additional merge run that
     *           may be created for any data still in the sort buffer.
     *     NOTE - this list will be expanded as more information about the sort
     *            is gathered and returned.
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/

    public Properties getAllSortInfo(Properties prop)
		throws StandardException
    {
        if (prop == null)
            prop = new Properties();

        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_SORT_TYPE),
			"external".equals(this.stat_sortType) ?
				MessageService.getTextMessage(SQLState.STORE_RTS_EXTERNAL) :
				MessageService.getTextMessage(SQLState.STORE_RTS_INTERNAL));
        // prop.put(
		//  MessageService.getTextMessage(SQLState.STORE_RTS_ESTIMATED_MEMORY_USED),
		//  Long.toString(stat_estimMemUsed));
        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_INPUT),
			Integer.toString(stat_numRowsInput));
        prop.put(
			MessageService.getTextMessage(SQLState.STORE_RTS_NUM_ROWS_OUTPUT),
			Integer.toString(stat_numRowsOutput));
        if (this.stat_sortType == "external")
        {
            prop.put(
			  MessageService.getTextMessage(SQLState.STORE_RTS_NUM_MERGE_RUNS),
			  Integer.toString(stat_numMergeRuns));
            prop.put(
			  MessageService.getTextMessage(SQLState.STORE_RTS_MERGE_RUNS_SIZE),
			  stat_mergeRunsSize.toString());
        }
        return(prop);
    }
}


    /**
     *     estimMemUsed IS NOT CURRENTLY SUPPORTED SINCE IT IS UNRELIABLE
     *     estimMemUsed
     *         - the estimated memory used by the sort.
     *
     *           This is only measured when the system runs out of sort
     *           buffer space, AND when it tries to avoid doing an external sort.
     *           It measures this by subtracting the memory usage at initialization
     *           from the memory usage at the time we are trying to avoid doing an
     *           external sort.  The result could be negative: this probably indicates
     *           that there has been some garbage collection in the interim.
     *           If the attempt at keeping the sort internal succeeds, the buffer grows
     *           but the increased memory usage is not measured.
     *
     *           The system may never measure the memory usage. This happens if
     *           it never runs out of sort buffer space, or if it is set up not
     *           to avoid external sorts. In cases that it is not measured, it returns 0.
     *
     *           In future, this info may improve with an improved JVM API.
     */

