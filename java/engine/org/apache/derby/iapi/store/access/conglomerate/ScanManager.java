/*

   Derby - Class org.apache.derby.iapi.store.access.conglomerate.ScanManager

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

import org.apache.derby.iapi.store.access.GroupFetchScanController;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.raw.Page;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.BackingStoreHashtable;

/**

The ScanManager interface contains those methods private to access method
implementors necessary to implement Scans on Conglomerates.  Client of scans
use the ScanController to interact with the scan.
<P>
@see ScanController

**/

public interface ScanManager extends ScanController, GroupFetchScanController
{

    /**
     * Close scan as part of terminating a transaction.
     * <p>
     * Use this call to close the scan resources as part of committing or
     * aborting a transaction.  The normal close() routine may do some cleanup
     * that is either unnecessary, or not correct due to the unknown condition
     * of the scan following a transaction ending error.  Use this call when
     * closing all scans as part of an abort of a transaction.
     *
     * @param closeHeldScan     If true, means to close scan even if it has been
     *                          opened to be kept opened across commit.  This is
     *                          used to close these scans on abort.
     *
	 * @return boolean indicating that the close has resulted in a real close
     *                 of the scan.  A held scan will return false if called 
     *                 by closeForEndTransaction(false), otherwise it will 
     *                 return true.  A non-held scan will always return true.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    boolean closeForEndTransaction(boolean closeHeldScan)
		throws StandardException;

    /**
     * Insert all rows that qualify for the current scan into the input
     * Hash table.  
     * <p>
     * This routine scans executes the entire scan as described in the 
     * openScan call.  For every qualifying unique row value an entry is
     * placed into the HashTable. For unique row values the entry in the
     * Hashtable has a key value of the object stored in 
     * row[key_column_number], and the value of the data is row.  For row 
     * values with duplicates, the key value is also row[key_column_number], 
     * but the value of the data is a Vector of
     * rows.  The caller will have to call "instanceof" on the data value
     * object if duplicates are expected, to determine if the data value
     * of the Hashtable entry is a row or is a Vector of rows.
     * <p>
     * Note, that for this routine to work efficiently the caller must 
     * ensure that the object in row[key_column_number] implements 
     * the hashCode and equals method as appropriate for it's datatype.
     * <p>
     * It is expected that this call will be the first and only call made in
     * an openscan.  Qualifiers and stop position of the openscan are applied
     * just as in a normal scan.  This call is logically equivalent to the 
     * caller performing the following:
     *
     * import java.util.Hashtable;
     *
     * hash_table = new Hashtable();
     *
     * while (next())
     * {
     *     row = create_new_row();
     *     fetch(row);
     *     if ((duplicate_value = 
     *         hash_table.put(row[key_column_number], row)) != null)
     *     {
     *         Vector row_vec;
     *
     *         // inserted a duplicate
     *         if ((duplicate_value instanceof vector))
     *         {
     *             row_vec = (Vector) duplicate_value;
     *         }
     *         else
     *         {
     *             // allocate vector to hold duplicates
     *             row_vec = new Vector(2);
     *
     *             // insert original row into vector
     *             row_vec.addElement(duplicate_value);
     *
     *             // put the vector as the data rather than the row
     *             hash_table.put(row[key_column_number], row_vec);
     *         }
     *         
     *         // insert new row into vector
     *         row_vec.addElement(row);
     *     }
     * }
     * <p>
     * The columns of the row will be the standard columns returned as
     * part of a scan, as described by the validColumns - see openScan for
     * description.
     * RESOLVE - is this ok?  or should I hard code somehow the row to
     *           be the first column and the row location?
     * <p>
     * No overflow to external storage is provided, so calling this routine
     * on a 1 gigabyte conglomerate will incur at least 1 gigabyte of memory
     * (probably failing with a java out of memory condition).  If this
     * routine gets an out of memory condition, or if "max_rowcnt" is 
     * exceeded then then the routine will give up, empty the Hashtable, 
     * and return "false."
     * <p>
     * On exit from this routine, whether the fetchSet() succeeded or not
     * the scan is complete, it is positioned just the same as if the scan
     * had been drained by calling "next()" until it returns false (ie. 
     * fetchNext() and next() calls will return false).  
     * reopenScan() can be called to restart the scan.
     * <p>
     *
     * RESOLVE - until we get row counts what should we do for sizing the
     *           the size, capasity, and load factor of the hash table.
     *           For now it is up to the caller to create the Hashtable,
     *           Access does not reset any parameters.
     * <p>
     * RESOLVE - I am not sure if access should be in charge of allocating
     *           the new row objects.  I know that I can do this in the
     *           case of btree's, but I don't think I can do this in heaps.
     *           Maybe this is solved by work to be done on the sort 
     *           interface.
     *
     *
	 * @return boolean indicating that the fetch set succeeded.  If it failed
     *                 Hashtable.clear() will be called leaving an empty 
     *                 table.
     *
     * @param max_rowcnt        The maximum number of rows to insert into the 
     *                          Hash table.  Pass in -1 if there is no maximum.
     * @param key_column_numbers The column numbers of the columns in the
     *                          scan result row to be the key to the Hashtable.
     *                          "0" is the first column in the scan result
     *                          row (which may be different than the first
     *                          row in the table of the scan).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    void fetchSet(
    long                    max_rowcnt,
    int[]                   key_column_numbers,
    BackingStoreHashtable   hash_table)
        throws StandardException;


    /**
     * Do work necessary to maintain the current position in the scan.
     * <p>
     * The latched page in the conglomerate "congomid" is changing, do
     * whatever is necessary to maintain the current position of the scan.
     * For some conglomerates this may be a no-op.
     * <p>
     *
     * @param conlgom   Conglomerate object of the conglomerate being changed.
     * @param page      Page in the conglomerate being changed.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void savePosition(Conglomerate conglom, Page page)
        throws StandardException;
}
