/*

   Derby - Class org.apache.derby.impl.store.access.btree.BranchRow

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

package org.apache.derby.impl.store.access.btree;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.ContainerHandle;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.services.io.FormatableBitSet;


/**
 * Implements row which is stored in the branch pages of a btree.  A non-suffix
 * compressed branch row contains all of the columns of the leaf rows of a btree
 * and contains an additional field at the end.  The extra field of a branch row
 * in a branch page at level N, is the child page field pointing the page at 
 * level N-1 which has keys which follow or equal the branch row entry.
 *
 * There are 3 ways to use this class to produce a branch row:
 *   createEmptyTemplate() 
 *      creates a empty row template
 *   createBranchRowFromOldBranchRow() 
 *      creates a new row with reference to an old branch row.
 *   createBranchRowFromOldLeafRow()
 *      creates a new row with reference to an old leaf row.
 */

public class BranchRow
{
    /* a dummy page number value (should not be compressable) */
    public static final long DUMMY_PAGE_NUMBER = 0xffffffffffffffffL;

    /**
     * The branch child page pointer.  All keys that Follow or equal the
     * key in this row can be found by following the child page pointer.
     * A reference to this object will be placed in the last slot of branchrow,
     * and this class expects that no-one will replace that reference.
     */
    // private SQLLongint      child_page = null;

    /**
     * The array of object to be used as the row.
     */
    private DataValueDescriptor[]    branchrow    = null;

	/*
	** Constructors of BranchRow
	*/

    /**
    Constuctor for creating an "empty" BranchRow template, suitable for reading
    in a branchRow from disk.
    **/
	private BranchRow()
	{
	}

	private BranchRow(BTree btree)
        throws StandardException
	{
        SQLLongint child_page  = 
            new SQLLongint(ContainerHandle.INVALID_PAGE_NUMBER);

        branchrow   = btree.createBranchTemplate(child_page);

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(
                child_page == ((SQLLongint) branchrow[branchrow.length - 1]));
        }
	}

    /*
    ** The following methods implement the BranchRow Private interface.
    */

    /**
     * Accessor for the child page field of the branch row.
     *
	 * @return The child page object.
     **/
    private SQLLongint getChildPage()
    {
        // last column of branch row should be the child page pointer.
        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(branchrow != null);
            SanityManager.ASSERT(
                branchrow[branchrow.length - 1] instanceof SQLLongint);
        }

        return((SQLLongint) branchrow[branchrow.length - 1]);
    }

    /*
    ** The following methods implement the BranchRow Public interface.
    */

    /**
     * Create an empty branch row template suitable for reading branch rows in
     * from disk. This routine will create newly allocated "empty" objects for
     * every column in the template row.
     *
     * @exception StandardException Standard exception policy.
     */
    public static BranchRow createEmptyTemplate(BTree   btree)
        throws StandardException
    {
        BranchRow  newbranch   = new BranchRow(btree);

        return(new BranchRow(btree));
    }

    /**
     * Create a new branch row, given a old branch row and a new child page.
     * Used by BranchControlRow to manufacture new branch rows when splitting
     * or growing the tree.
     *
     * There is no way to "copy" values of a template row, so this class just
     * stores a reference to each of the columns of the Indexable row passed 
     * in.  This is ok as all
     * usages of this class when instantiated this way, have an old branch row
     * from which they are creating a new branch row with the same key values,
     * and a different child page number.
     *
     * WARNING - this branch row is only valid while the old branch row is
     * valid, as it contains references to the columns of the old branch row.
     * So use of the row should only provide read-only access to the objects
     * of the old branch row which are referenced.
     */
    public BranchRow createBranchRowFromOldBranchRow(long childpageno)
    {
        BranchRow newbranch = new BranchRow();

        /* create new object array, and shallow copy all object references 
         * from old branch row to new branch row.
         */

        newbranch.branchrow = new DataValueDescriptor[this.branchrow.length]; 
        System.arraycopy(
            this.branchrow, 0, newbranch.branchrow, 0, 
            newbranch.branchrow.length - 1);

        /* now create a different child page pointer object and place it as
         * last column in the new branch row.
         */
        newbranch.branchrow[newbranch.branchrow.length - 1] = 
            new SQLLongint(childpageno);

        return(newbranch);
    }

    /**
     * Create a new branch row, given a old leaf row and a new child page.
     * Used by LeafControlRow to manufacture new branch rows when splitting
     * or growing the tree.
     *
     * There is no way to "copy" values of a template row, so this class just
     * stores a referece to the Indexable row passed in.  This is ok as all
     * usages of this class when instantiated this way, have an old leaf row
     * from which they are creating a new branch row with the same key values,
     * and a different child page number.
     *
     * WARNING - this branch row is only valid while the old leaf row is
     * valid, as it contains references to the columns of the old leaf row.
     * So use of the row should only provide read-only access to the objects
     * of the old leaf row which are referenced.
     */
    public static BranchRow createBranchRowFromOldLeafRow(
    DataValueDescriptor[]   leafrow, 
    long                    childpageno)
    {
        BranchRow newbranch = new BranchRow();

        /* create new object array for the row, and copy all object references 
         * from old leaf row to new branch row.
         */
        newbranch.branchrow = new DataValueDescriptor[leafrow.length + 1];

        System.arraycopy(leafrow, 0, newbranch.branchrow, 0, leafrow.length);

        /* now create a different child page pointer object and place it as
         * last column in the new branch row.
         */
        newbranch.branchrow[newbranch.branchrow.length - 1] = 
            new SQLLongint(childpageno);

        return(newbranch);
    }

    /**
     * Return the branch row.
     * <p>
     * Return the DataValueDescriptor array that represents the branch row, 
     * for use in raw store calls to fetch, insert, and update.
     * <p>
     *
	 * @return The branch row object array.
     **/
    protected DataValueDescriptor[] getRow()
    {
        return(this.branchrow);
    }

    /**
     * Set the page number field of the branch row to a new value.
     *
     * @param page_number the new page number.
     **/
    protected void setPageNumber(long page_number)
    {
        getChildPage().setValue(page_number);
    }


	public String toString()
	{
        if (SanityManager.DEBUG)
        {
            return(
                RowUtil.toString(branchrow) + 
                "child page: (" + getChildPage() + ")");
        }
        else
        {
            return(null);
        }
	}
}
