/*

   Derby - Class org.apache.derby.iapi.store.access.ScanInfo

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

import java.util.Properties;

/**

  This object provides performance information related to an open scan.
  The information is accumulated during operations on a ScanController() and
  then copied into this object and returned by a call to 
  ScanController.getStatistic().

  @see GenericScanController#getScanInfo()

**/

public interface ScanInfo
{
    /**
     * Return all information gathered about the scan.
     * <p>
     * This routine returns a list of properties which contains all information
     * gathered about the scan.  If a Property is passed in, then that property
     * list is appeneded to, otherwise a new property object is created and
     * returned.
     * <p>
     * Not all scans may support all properties, if the property is not 
     * supported then it will not be returned.  The following is a list of
     * properties that may be returned. These names have been internationalized,
	 * the names shown here are the old, non-internationalized names:
     *
     *     scanType
     *         - type of the scan being performed:
     *           btree
     *           heap
     *           sort
     *     numPagesVisited
     *         - the number of pages visited during the scan.  For btree scans
     *           this number only includes the leaf pages visited.  
     *     numDeletedRowsVisited
     *         - the number of deleted rows visited during the scan.  This
     *           number includes only those rows marked deleted.
     *     numRowsVisited
     *         - the number of rows visited during the scan.  This number 
     *           includes all rows, including: those marked deleted, those
     *           that don't meet qualification, ...
     *     numRowsQualified
     *         - the number of rows which met the qualification.
     *     treeHeight (btree's only)
     *         - for btree's the height of the tree.  A tree with one page
     *           has a height of 1.  Total number of pages visited in a btree
     *           scan is (treeHeight - 1 + numPagesVisited).
     *     numColumnsFetched
     *         - the number of columns Fetched - partial scans will result
     *           in fetching less columns than the total number in the scan.
     *     columnsFetchedBitSet
     *         - The BitSet.toString() method called on the validColumns arg.
     *           to the scan, unless validColumns was set to null, and in that
     *           case we will return "all".
     *     NOTE - this list will be expanded as more information about the scan
     *            is gathered and returned.
     *
     * @param prop   Property list to fill in.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    Properties getAllScanInfo(Properties prop)
		throws StandardException;
}
