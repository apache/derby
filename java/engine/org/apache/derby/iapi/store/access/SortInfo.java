/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.store.access
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
