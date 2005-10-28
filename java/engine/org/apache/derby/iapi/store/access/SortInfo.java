/*

   Derby - Class org.apache.derby.iapi.store.access.SortInfo

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException;

import java.util.Properties;

/**

  This object provides performance information related to a sort.
  The information is accumulated during operations on a SortController() and
  then copied into this object and returned by a call to
  SortController.getSortInfo().

**/

public interface SortInfo
{
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
    Properties getAllSortInfo(Properties prop)
		throws StandardException;
}
