/*

   Derby - Class org.apache.derby.iapi.store.raw.UndoHandler

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.store.raw.PageKey;
import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.error.StandardException; 


/**
	A class that provides interface to be called with undo of an Insert
    happens in raw store.
*/


public interface UndoHandler
{
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
    /**
     * Interface to be called when an undo of an insert is processed.
     * <p>
     * Implementer of this class provides interface to be called by the raw
     * store when an undo of an insert is processed.  Initial implementation
     * will be by Access layer to queue space reclaiming events if necessary
     * when a rows is logically "deleted" as part of undo of the original
     * insert.  This undo can happen a lot for many applications if they
     * generate expected and handled duplicate key errors.
     * <p>
     * It may be useful at some time to include the recordId of the deleted
     * row, but it is not used currently by those notified.  The post commit
     * work ultimately processes all rows on the table while
     * it has the latch which is more efficient than one row at time per latch.
     * <p>
     * It is expected that notifies only happen for pages that caller
     * is interested in.  Currently only the following aborted inserts
     * cause a notify:
     * o must be on a non overflow page
     * o if all "user" rows on page are deleted a notify happens (page 1 
     *   has a system row so on page one notifies happen if all but the first
     *   row is deleted).
     * o if the aborted insert row has either an overflow row or column
     *   component then the notify is executed.
     *
     * @param xact      transaction that is being backed out.
     * @param page_key  key that uniquely identifies page in question, container
     *                  key information is embedded in the PageKey
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public void insertUndoNotify(
    Transaction         xact,
    PageKey             page_key)
       throws StandardException;


    /**************************************************************************
     * Public Methods of XXXX class:
     **************************************************************************
     */
}
