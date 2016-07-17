/*

   Derby - Class org.apache.derby.iapi.store.access.BackingStoreHashtable

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

package org.apache.derby.iapi.store.access;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.LocatedRow;
import org.apache.derby.iapi.types.RowLocation;

import org.apache.derby.iapi.services.cache.ClassSize;

import org.apache.derby.shared.common.sanity.SanityManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties; 
import java.util.NoSuchElementException;

/**
<p>
A BackingStoreHashtable is a utility class which will store a set of rows into
an in memory hash table, or overflow the hash table to a tempory on disk 
structure.
</p>

<p>
All rows must contain the same number of columns, and the column at position
N of all the rows must have the same format id.  If the BackingStoreHashtable needs to be
overflowed to disk, then an arbitrary row will be chosen and used as a template
for creating the underlying overflow container.
</p>

<p>
The hash table will be built logically as follows (actual implementation
may differ).  The important points are that the hash value is the standard
java hash value on the row[key_column_numbers[0], if key_column_numbers.length is 1,
or row[key_column_numbers[0, 1, ...]] if key_column_numbers.length &gt; 1, 
and that duplicate detection is done by the standard java duplicate detection provided by 
java.util.Hashtable.
</p>

<pre>
import java.util.Hashtable;

hash_table = new Hashtable();

Object row; // is a DataValueDescriptor[] or a LocatedRow

boolean  needsToClone = rowSource.needsToClone();

while((row = rowSource.getNextRowFromRowSource()) != null)
{
    if (needsToClone)
        row = clone_row_from_row(row);

    Object key = KeyHasher.buildHashKey(row, key_column_numbers);

    if ((duplicate_value = 
        hash_table.put(key, row)) != null)
    {
        Vector row_vec;

        // inserted a duplicate
        if ((duplicate_value instanceof vector))
        {
            row_vec = (Vector) duplicate_value;
        }
        else
        {
            // allocate vector to hold duplicates
            row_vec = new Vector(2);

            // insert original row into vector
            row_vec.addElement(duplicate_value);

            // put the vector as the data rather than the row
            hash_table.put(key, row_vec);
        }
        
        // insert new row into vector
        row_vec.addElement(row);
    }
}
</pre>

<p>
What actually goes into the hash table is a little complicated. That is because
the row may either be an array of column values (i.e. DataValueDescriptor[])
or a LocatedRow (i.e., a structure holding the columns plus a RowLocation).
In addition, the hash value itself may either be one of these rows or
(in the case of multiple rows which hash to the same value) a bucket (List)
of rows. To sum this up, the values in a hash table which does not spill
to disk may be the following:
</p>

<ul>
<li>DataValueDescriptor[] and ArrayList<DataValueDescriptor></li>
<li>or LocatedRow and ArrayList<LocatedRow></li>
</ul>

<p>
If rows spill to disk, then they just become arrays of columns. In this case,
a LocatedRow becomes a DataValueDescriptor[], where the last cell contains
the RowLocation.
</p>

**/

public class BackingStoreHashtable
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private TransactionController tc;
    private HashMap<Object,Object>     hash_table;
    private int[]       key_column_numbers;
    private boolean     remove_duplicates;
	private boolean		skipNullKeyColumns;
    private Properties  auxillary_runtimestats;
	private RowSource	row_source;
    /* If max_inmemory_rowcnt > 0 then use that to decide when to spill to disk.
     * Otherwise compute max_inmemory_size based on the JVM memory size when the BackingStoreHashtable
     * is constructed and use that to decide when to spill to disk.
     */
    private long max_inmemory_rowcnt;
    private long inmemory_rowcnt;
    private long max_inmemory_size;
    private boolean keepAfterCommit;

    /**
     * The estimated number of bytes used by ArrayList(0)
     */  
    private final static int ARRAY_LIST_SIZE =
        ClassSize.estimateBaseFromCatalog(ArrayList.class);
    
    private DiskHashtable diskHashtable;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    private BackingStoreHashtable(){}

    /**
     * Create the BackingStoreHashtable from a row source.
     * <p>
     * This routine drains the RowSource.  The performance characteristics
     * depends on the number of rows inserted and the parameters to the 
     * constructor. RowLocations are supported iff row_source is null.
     * RowLocations in a non-null row_source can be added later
     * if there is a use-case that stresses this behavior.
     * <p>
     * If the number of rows is &lt;= "max_inmemory_rowcnt", then the rows are
     * inserted into a java.util.HashMap. In this case no
     * TransactionController is necessary, a "null" tc is valid.
     * <p>
     * If the number of rows is &gt; "max_inmemory_rowcnt", then the rows will
     * be all placed in some sort of Access temporary file on disk.  This 
     * case requires a valid TransactionController.
     *
     * @param tc                An open TransactionController to be used if the
     *                          hash table needs to overflow to disk.
     *
     * @param row_source        RowSource to read rows from.
     *
     * @param key_column_numbers The column numbers of the columns in the
     *                          scan result row to be the key to the HashMap.
     *                          "0" is the first column in the scan result
     *                          row (which may be different than the first
     *                          row in the table of the scan).
     *
     * @param remove_duplicates Should the HashMap automatically remove
     *                          duplicates, or should it create the list of
     *                          duplicates?
     *
     * @param estimated_rowcnt  The estimated number of rows in the hash table.
     *                          Pass in -1 if there is no estimate.
     *
     * @param max_inmemory_rowcnt
     *                          The maximum number of rows to insert into the 
     *                          inmemory Hash table before overflowing to disk.
     *                          Pass in -1 if there is no maximum.
     *
     * @param initialCapacity   If not "-1" used to initialize the java HashMap
     *
     * @param loadFactor        If not "-1" used to initialize the java HashMap
	 *
	 * @param skipNullKeyColumns	Skip rows with a null key column, if true.
     *
     * @param keepAfterCommit If true the hash table is kept after a commit,
     *                        if false the hash table is dropped on the next commit.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public BackingStoreHashtable(
    TransactionController   tc,
    RowSource               row_source,
    int[]                   key_column_numbers,
    boolean                 remove_duplicates,
    long                    estimated_rowcnt,
    long                    max_inmemory_rowcnt,
    int                     initialCapacity,
    float                   loadFactor,
	boolean					skipNullKeyColumns,
    boolean                 keepAfterCommit)
        throws StandardException
    {
        this.key_column_numbers    = key_column_numbers;
        this.remove_duplicates    = remove_duplicates;
		this.row_source			   = row_source;
		this.skipNullKeyColumns	   = skipNullKeyColumns;
        this.max_inmemory_rowcnt = max_inmemory_rowcnt;
        if ( max_inmemory_rowcnt > 0)
        {
            max_inmemory_size = Long.MAX_VALUE;
        }
        else
        {
            max_inmemory_size = Runtime.getRuntime().totalMemory()/100;
        }
        this.tc = tc;
        this.keepAfterCommit = keepAfterCommit;

        if (SanityManager.DEBUG)
        {
            // RowLocations are not currently supported if the
            // hash table is being filled from a non-null
            // row_source arg.
            if ( row_source != null )
            {
                SanityManager.ASSERT( !includeRowLocations() );
            }
        }

        // use passed in capacity and loadfactor if not -1, you must specify
        // capacity if you want to specify loadfactor.
        if (initialCapacity != -1)
        {
            hash_table = 
                ((loadFactor == -1) ? 
                     new HashMap<Object,Object>(initialCapacity) :
                     new HashMap<Object,Object>(initialCapacity, loadFactor));
        }
        else
        {
            /* We want to create the hash table based on the estimated row
             * count if a) we have an estimated row count (i.e. it's greater
             * than zero) and b) we think we can create a hash table to
             * hold the estimated row count without running out of memory.
             * The check for "b" is required because, for deeply nested
             * queries and/or queries with a high number of tables in
             * their FROM lists, the optimizer can end up calculating
             * some very high row count estimates--even up to the point of
             * Double.POSITIVE_INFINITY (see DERBY-1259 for an explanation
             * of how that can happen).  In that case any attempts to
             * create a hash table of size estimated_rowcnt can cause
             * OutOfMemory errors when we try to create the hash table.
             * So as a "red flag" for that kind of situation, we check to
             * see if the estimated row count is greater than the max
             * in-memory size for this table.  Unit-wise this comparison
             * is relatively meaningless: rows vs bytes.  But if our
             * estimated row count is greater than the max number of
             * in-memory bytes that we're allowed to consume, then
             * it's very likely that creating a hash table with a capacity
             * of estimated_rowcnt will lead to memory problems.  So in
             * that particular case we leave hash_table null here and
             * initialize it further below, using the estimated in-memory
             * size of the first row to figure out what a reasonable size
             * for the hash table might be.
             */
            hash_table = 
                (((estimated_rowcnt <= 0) || (row_source == null)) ?
                     new HashMap<Object,Object>() :
                     (estimated_rowcnt < max_inmemory_size) ?
                         new HashMap<Object,Object>((int) estimated_rowcnt) :
                         null);
        }

        if (row_source != null)
        {
            boolean needsToClone = row_source.needsToClone();

            DataValueDescriptor[] row;
            while ((row = getNextRowFromRowSource()) != null)
            {
                // If we haven't initialized the hash_table yet then that's
                // because a hash table with capacity estimated_rowcnt would
                // probably cause memory problems.  So look at the first row
                // that we found and use that to create the hash table with
                // an initial capacity such that, if it was completely full,
                // it would still satisfy the max_inmemory condition.  Note
                // that this isn't a hard limit--the hash table can grow if
                // needed.
                if (hash_table == null)
                {
                    // Check to see how much memory we think the first row
                    // is going to take, and then use that to set the initial
                    // capacity of the hash table.
                    double rowUsage = getEstimatedMemUsage(row);
                    hash_table =
                        new HashMap<Object,Object>((int)(max_inmemory_size / rowUsage));
                }
               
                add_row_to_hash_table(row, null, needsToClone);
            }
        }

        // In the (unlikely) event that we received a "red flag" estimated_rowcnt
        // that is too big (see comments above), it's possible that, if row_source
        // was null or else didn't have any rows, hash_table could still be null
        // at this point.  So we initialize it to an empty hashtable (representing
        // an empty result set) so that calls to other methods on this
        // BackingStoreHashtable (ex. "size()") will have a working hash_table
        // on which to operate.
        if (hash_table == null)
        {
            hash_table = new HashMap<Object,Object>();
        }
    }

    /**
     * Return true if we should include RowLocations with the rows
     * stored in this hash table.
     */
    public  boolean includeRowLocations()
    {
        return false;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

	/**
	 * Call method to either get next row or next row with non-null
	 * key columns. Currently, RowLocation information is not included in
     * rows siphoned out of a RowSource. That functionality is only supported
     * if the rows come from the scan of a base table.
	 *
     *
	 * @exception  StandardException  Standard exception policy.
	 */
	private DataValueDescriptor[] getNextRowFromRowSource()
		throws StandardException
	{
		DataValueDescriptor[] row = row_source.getNextRowFromRowSource();

		if (skipNullKeyColumns)
		{
			while (row != null)
			{
				// Are any key columns null?
				int index = 0;
				for ( ; index < key_column_numbers.length; index++)
				{
					if (row[key_column_numbers[index]].isNull())
					{
						break;
					}
				}
				// No null key columns
				if (index == key_column_numbers.length)
				{
					return row;
				}
				// 1 or more null key columns
				row = row_source.getNextRowFromRowSource();
			}
		}
		return row;
	}

    /**
     * Return a cloned copy of the row.
     *
	 * @return The cloned row row to use.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private static DataValueDescriptor[] cloneRow(DataValueDescriptor[] old_row)
        throws StandardException
    {
        DataValueDescriptor[] new_row = new DataValueDescriptor[old_row.length];

        // History: We used to materialize streams when getting a clone
        //          here (i.e. used getClone, not cloneObject). We still do.
        // Beetle 4896.
        for (int i = 0; i < old_row.length; i++)
        {
            if ( old_row[i] != null)
            {
                new_row[i] = old_row[i].cloneValue(false);
            }
        }

        return(new_row);
    }

    /**
     * Return a shallow cloned row
     *
     * @return The cloned row row to use.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    static DataValueDescriptor[] shallowCloneRow(DataValueDescriptor[] old_row)
        throws StandardException
    {
        DataValueDescriptor[] new_row = new DataValueDescriptor[old_row.length];
        // History: We used to *not* materialize streams when getting a clone
        //          here (i.e. used cloneObject, not getClone).
        //          We still don't materialize, just clone the holder.
        // DERBY-802
        for (int i = 0; i < old_row.length; i++)
        {
            if ( old_row[i] != null)
            {
                new_row[i] = old_row[i].cloneHolder();
            }
        }

        return(new_row);
    }

    /**
     * Do the work to add one row to the hash table.
     * <p>
     *
     * @param columnValues               Row to add to the hash table.
     * @param rowLocation   Location of row in conglomerate; could be null.
     * @param needsToClone      If the row needs to be cloned
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private void add_row_to_hash_table
        (
         DataValueDescriptor[] columnValues,
         RowLocation rowLocation,
         boolean needsToClone
         )
		throws StandardException
    {
        if (spillToDisk( columnValues, rowLocation ))
        {
            return;
        }
        
        if (needsToClone)
        {
            columnValues = cloneRow( columnValues );
        }

        Object key = KeyHasher.buildHashKey( columnValues, key_column_numbers );
        Object hashValue = !includeRowLocations() ?
            columnValues : new LocatedRow( columnValues, rowLocation );
        Object duplicate_value = hash_table.put( key, hashValue );

        if (duplicate_value == null)
        {
            doSpaceAccounting( hashValue, false );
        }
        else
        {
            if (!remove_duplicates)
            {
                RowList row_vec;

                // inserted a duplicate
                if (duplicate_value instanceof RowList)
                {
                    doSpaceAccounting( hashValue, false );
                    row_vec = (RowList) duplicate_value;
                }
                else
                {
                    // allocate list to hold duplicates
                    row_vec = new RowList(2);

                    // insert original row into vector
                    row_vec.add( duplicate_value );
                    doSpaceAccounting( hashValue, true );
                }

                // insert new row into list
                row_vec.add( hashValue );

                // store list of rows back into hash table,
                // overwriting the duplicate key that was 
                // inserted.
                hash_table.put(key, row_vec);
            }
        }
    }

    private void doSpaceAccounting(Object hashValue,
                                    boolean firstDuplicate)
    {
        inmemory_rowcnt++;
        if ( max_inmemory_rowcnt <= 0)
        {
            max_inmemory_size -= getEstimatedMemUsage( hashValue );
            if ( firstDuplicate)
            {
                max_inmemory_size -= ARRAY_LIST_SIZE;
            }
        }
    } // end of doSpaceAccounting

    /**
     * Determine whether a new row should be spilled to disk and, if so, do it.
     *
     * @param columnValues  Actual columns from source row.
     * @param rowLocation       Optional row location.
     *
     * @return true if the row was spilled to disk, false if not
     *
     * @exception  StandardException  Standard exception policy.
     */
    private boolean spillToDisk
        (
         DataValueDescriptor[] columnValues,
         RowLocation rowLocation
         )
        throws StandardException
    {
        // Once we have started spilling all new rows will go to disk, even if we have freed up some
        // memory by moving duplicates to disk. This simplifies handling of duplicates and accounting.

        DataValueDescriptor[]   diskRow = null;
        
        if ( diskHashtable == null)
        {
            if ( max_inmemory_rowcnt > 0)
            {
                if ( inmemory_rowcnt < max_inmemory_rowcnt)
                {
                    return false; // Do not spill
                }
            }
            else if
                (
                 max_inmemory_size >
                 getEstimatedMemUsage
                 (
                  !includeRowLocations() ?
                  columnValues : new LocatedRow( columnValues, rowLocation )
                 )
                )
            {
                return false;
            }
            
            // Want to start spilling
            diskRow = makeDiskRow( columnValues, rowLocation );
 
            diskHashtable = 
                new DiskHashtable(
                       tc,
                       diskRow,
                       (int[]) null, //TODO-COLLATION, set non default collation if necessary.
                       key_column_numbers,
                       remove_duplicates,
                       keepAfterCommit);
        }
        Object key = KeyHasher.buildHashKey( columnValues, key_column_numbers );
        Object duplicateValue = hash_table.get( key);
        if ( duplicateValue != null)
        {
            if ( remove_duplicates)
                return true; // a degenerate case of spilling
            // If we are keeping duplicates then move all the duplicates from memory to disk
            // This simplifies finding duplicates: they are either all in memory or all on disk.
            if (duplicateValue instanceof List)
            {
                List duplicateVec = (List) duplicateValue;
                for( int i = duplicateVec.size() - 1; i >= 0; i--)
                {
                    diskHashtable.put
                        ( key, makeDiskRow( duplicateVec.get( i ) ));
                }
            }
            else
            {
                diskHashtable.put( key, makeDiskRow( duplicateValue ) );
            }
            hash_table.remove( key);
        }

        if ( diskRow == null )
        { diskRow = makeDiskRow( columnValues, rowLocation ); }
        
        diskHashtable.put( key, diskRow );
        return true;
    } // end of spillToDisk

    /**
     * <p>
     * Make a full set of columns from an object which is either already
     * an array of column or otherwise a LocatedRow. The full set of columns
     * is what's stored on disk when we spill to disk. This is the inverse of
     * makeInMemoryRow().
     * </p>
     */
    private DataValueDescriptor[]   makeDiskRow( Object raw )
    {
        DataValueDescriptor[]   allColumns = null;
        if ( includeRowLocations() )
        {
            LocatedRow  locatedRow = (LocatedRow) raw;
            allColumns = makeDiskRow
                ( locatedRow.columnValues(), locatedRow.rowLocation() );
        }
        else { allColumns = (DataValueDescriptor[]) raw; }

        return allColumns;
    }

    /**
     * <p>
     * Turn a list of disk rows into a list of in-memory rows. The on disk
     * rows are always of type DataValueDescriptor[]. But the in-memory rows
     * could be of type LocatedRow.
     * </p>
     */
    private List    makeInMemoryRows( List diskRows )
    {
        if ( !includeRowLocations() )
        {
            return diskRows;
        }
        else
        {
            ArrayList<Object>   result = new ArrayList<Object>();
            for ( Object diskRow : diskRows )
            {
                result.add
                    ( makeInMemoryRow( (DataValueDescriptor[]) diskRow ) );
            }

            return result;
        }
    }

    /**
     * <p>
     * Make an in-memory row from an on-disk row. This is the inverse
     * of makeDiskRow().
     * </p>
     */
    private Object  makeInMemoryRow( DataValueDescriptor[] diskRow )
    {
        if ( !includeRowLocations() )
        {
            return diskRow;
        }
        else
        {
            return new LocatedRow( diskRow );
        }
    }

    /**
     * <p>
     * Construct a full set of columns, which may need to end
     * with the row location.The full set of columns is what's
     * stored on disk when we spill to disk.
     * </p>
     */
    private DataValueDescriptor[]   makeDiskRow
        ( DataValueDescriptor[] columnValues, RowLocation rowLocation )
    {
        if ( !includeRowLocations() )
        {
            return columnValues;
        }
        else
        {
            return LocatedRow.flatten( columnValues, rowLocation );
        }
    }

    /**
     * Take a value which will go into the hash table and return an estimate
     * of how much memory that value will consume. The hash value could
     * be either an array of columns or a LocatedRow.
     * 
     * @param hashValue The object for which we want to know the memory usage.
     * @return A guess as to how much memory the current hash value will
     *  use.
     */
    private long getEstimatedMemUsage( Object hashValue )
    {
        long rowMem = 0;
        DataValueDescriptor[] row = null;

        if ( hashValue instanceof DataValueDescriptor[] )
        {
            row = (DataValueDescriptor[]) hashValue;
        }
        else
        {
            LocatedRow  locatedRow = (LocatedRow) hashValue;
            row = locatedRow.columnValues();

            // account for the RowLocation size and class overhead
            RowLocation rowLocation = locatedRow.rowLocation();
            if ( rowLocation != null )
            {
                rowMem += locatedRow.rowLocation().estimateMemoryUsage();
                rowMem += ClassSize.refSize;
            }

            // account for class overhead of the LocatedRow itself
            rowMem += ClassSize.refSize;
        }
        
        for( int i = 0; i < row.length; i++)
        {
            // account for the column's size and class overhead
            rowMem += row[i].estimateMemoryUsage();
            rowMem += ClassSize.refSize;
        }

        // account for the class overhead of the array itself
        rowMem += ClassSize.refSize;
        return rowMem;
    }

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Close the BackingStoreHashtable.
     * <p>
     * Perform any necessary cleanup after finishing with the hashtable.  Will
     * deallocate/dereference objects as necessary.  If the table has gone
     * to disk this will drop any on disk files used to support the hash table.
     * <p>
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void close() 
		throws StandardException
    {
        hash_table = null;
        if ( diskHashtable != null)
        {
            diskHashtable.close();
            diskHashtable = null;
        }
        return;
    }

    /**
     * <p>
     * Return an Enumeration that can be used to scan the entire table. The objects
     * in the Enumeration can be either of the following:
     * </p>
     *
     * <ul>
     * <li>a row - This is a single row with a unique hash key.</li>
     * <li>or a bucket of rows - This is a list of rows which all have the same hash key.</li>
     * </ul>
     *
     * <p>
     * The situation is a little more complicated because the row representation
     * is different depending on whether the row includes a RowLocation.
     * If includeRowLocations()== true, then the row is a LocatedRow. Otherwise,
     * the row is an array of DataValueDescriptor. Putting all of this together,
     * if the row contains a RowLocation, then the objects in the Enumeration returned
     * by this method can be either of the following:
     * </p>
     *
     * <ul>
     * <li>a LocatedRow</li>
     * <li>or a List&lt;LocatedRow&gt;</li>
     * </ul>
     *
     * <p>
     * But if the row does not contain a RowLocation, then the objects in the
     * Enumeration returned by this method can be either of the following:
     * </p>
     *
     * <ul>
     * <li>a DataValueDescriptor[]</li>
     * <li>or a List&lt;DataValueDescriptor[]&gt;</li>
     * </ul>
     *
     * <p>
     * RESOLVE - is it worth it to support this routine when we have a
     *           disk overflow hash table?
     * </p>
     *
	 * @return The Enumeration.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Enumeration<Object> elements()
        throws StandardException
    {
        if ( diskHashtable == null)
        {
            return Collections.enumeration(hash_table.values());
        }
        return new BackingStoreHashtableEnumeration();
    }

    /**
     * <p>
     * Get data associated with given key.
     * </p>
     *
     * <p>
     * There are 2 different types of objects returned from this routine.
     * </p>
     *
     * <p>
	 * In both cases, the key value is either the object stored in 
     * row[key_column_numbers[0]], if key_column_numbers.length is 1, 
     * otherwise it is a KeyHasher containing
	 * the objects stored in row[key_column_numbers[0, 1, ...]].
     * For every qualifying unique row value an entry is placed into the 
     * hash table.
     * </p>
     *
     * <p>
     * For row values with duplicates, the value of the data is a list of
     * rows.
     * </p>
     *
     * <p>
     * The situation is a little more complicated because the row representation
     * is different depending on whether the row includes a RowLocation.
     * If includeRowLocations() == true, then the row is a LocatedRow. Otherwise,
     * the row is an array of DataValueDescriptor. Putting all of this together,
     * if the row contains a RowLocation, then the objects returned by this method
     * can be either of the following:
     * </p>
     *
     * <ul>
     * <li>a LocatedRow</li>
     * <li>or a List&lt;LocatedRow&gt;</li>
     * </ul>
     *
     * <p>
     * But if the row does not contain a RowLocation, then the objects
     * returned by this method can be either of the following:
     * </p>
     *
     * <ul>
     * <li>a DataValueDescriptor[]</li>
     * <li>or a List&lt;DataValueDescriptor[]&gt;</li>
     * </ul>
     *
     * <p>
     * The caller will have to call "instanceof" on the data value
     * object if duplicates are expected, to determine if the data value
     * of the hash table entry is a row or is a list of rows.
     * </p>
     *
     * <p>
     * See the javadoc for elements() for more information on the objects
     * returned by this method.
     * </p>
     *
     * <p>
     * The BackingStoreHashtable "owns" the objects returned from the get()
     * routine.  They remain valid until the next access to the 
     * BackingStoreHashtable.  If the client needs to keep references to these
     * objects, it should clone copies of the objects.  A valid 
     * BackingStoreHashtable can place all rows into a disk based conglomerate,
     * declare a row buffer and then reuse that row buffer for every get()
     * call.
     * </p>
     *
	 * @return The value to which the key is mapped in this hashtable; 
     *         null if the key is not mapped to any value in this hashtable.
     *
     * @param key    The key to hash on.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Object get(Object key)
		throws StandardException
    {
        Object obj = hash_table.get(key);
        if ( diskHashtable == null || obj != null)
        {
            return obj;
        }

        Object  diskHashtableValue = diskHashtable.get( key );
        if ( diskHashtableValue == null )
        { return null; }

        if ( diskHashtableValue instanceof List )
        {
            return makeInMemoryRows( (List) diskHashtableValue );
        }
        else
        {
            return makeInMemoryRow
                ( (DataValueDescriptor[]) diskHashtableValue );
        }
    }

    /**
     * Return runtime stats to caller by adding them to prop.
     * <p>
     *
     * @param prop   The set of properties to append to.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void getAllRuntimeStats(Properties   prop)
		throws StandardException
    {
        if (auxillary_runtimestats != null)
        {
            org.apache.derby.iapi.util.PropertyUtil.copyProperties
                (auxillary_runtimestats, prop);
        }
    }

    /**
     * remove a row from the hash table.
     * <p>
     * a remove of a duplicate removes the entire duplicate list.
     *
     * @param key          The key of the row to remove.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Object remove(
    Object      key)
		throws StandardException
    {
        Object obj = hash_table.remove(key);
        if ( obj != null || diskHashtable == null)
        {
            return obj;
        }
        return diskHashtable.remove(key);
    }

    /**
     * Set the auxillary runtime stats.
     * <p>
     * getRuntimeStats() will return both the auxillary stats and any
     * BackingStoreHashtable() specific stats.  Note that each call to
     * setAuxillaryRuntimeStats() overwrites the Property set that was
     * set previously.
     *
     * @param prop   The set of properties to append from.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void setAuxillaryRuntimeStats(Properties   prop)
		throws StandardException
    {
        auxillary_runtimestats = prop;
    }

    /**
     * Put a row into the hash table.
     * <p>
     * The in memory hash table will need to keep a reference to the row
     * after the put call has returned.  If "needsToClone" is true then the
     * hash table will make a copy of the row and put that, else if 
     * "needsToClone" is false then the hash table will keep a reference to
     * the row passed in and no copy will be made.
     * <p>
     * If routine returns false, then no reference is kept to the duplicate
     * row which was rejected (thus allowing caller to reuse the object).
     *
     * @param needsToClone does this routine have to make a copy of the row,
     *                     in order to keep a reference to it after return?
     * @param row          The row to insert into the table.
     * @param rowLocation  Location of row in conglomerate; could be null.
     *
	 * @return true if row was inserted into the hash table.  Returns
     *              false if the BackingStoreHashtable is eliminating 
     *              duplicates, and the row being inserted is a duplicate,
	 *				or if we are skipping rows with 1 or more null key columns
	 *				and we find a null key column.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean putRow
        (
         boolean     needsToClone,
         DataValueDescriptor[]    row,
         RowLocation    rowLocation
         )
		throws StandardException
    {
		// Are any key columns null?
		if (skipNullKeyColumns)
		{
			int index = 0;
			for ( ; index < key_column_numbers.length; index++)
			{
				if (row[key_column_numbers[index]].isNull())
				{
					return false;
				}
			}
		}

        Object key = KeyHasher.buildHashKey(row, key_column_numbers);

        if ((remove_duplicates) && (get(key) != null))
        {
            return(false);
        }
        else
        {
            add_row_to_hash_table( row, rowLocation, needsToClone );
            return(true);
        }
    }

    /**
     * Return number of unique rows in the hash table.
     * <p>
     *
	 * @return The number of unique rows in the hash table.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public int size()
		throws StandardException
    {
        if ( diskHashtable == null)
        {
            return(hash_table.size());
        }
        return hash_table.size() + diskHashtable.size();
    }

    private class BackingStoreHashtableEnumeration implements Enumeration<Object>
    {
        private Iterator<Object> memoryIterator;
        private Enumeration<Object> diskEnumeration;

        BackingStoreHashtableEnumeration()
        {
            memoryIterator = hash_table.values().iterator();
            if ( diskHashtable != null)
            {
                try
                {
                    diskEnumeration = diskHashtable.elements();
                }
                catch( StandardException se)
                {
                    diskEnumeration = null;
                }
            }
        }
        
        public boolean hasMoreElements()
        {
            if (memoryIterator != null) {
                if (memoryIterator.hasNext()) {
                    return true;
                }
                memoryIterator = null;
            }
            if ( diskEnumeration == null)
            {
                return false;
            }
            return diskEnumeration.hasMoreElements();
        }

        public Object nextElement() throws NoSuchElementException
        {
            if (memoryIterator != null) {
                if (memoryIterator.hasNext()) {
                    return memoryIterator.next();
                }
                memoryIterator = null;
            }
            return makeInMemoryRow
                ( ((DataValueDescriptor[]) diskEnumeration.nextElement()) );
        }
    } // end of class BackingStoreHashtableEnumeration

    /**
     * List of {@code DataValueDescriptor[]} instances that represent rows.
     * This class is used when the hash table contains multiple rows for the
     * same hash key.
     */
    private static class RowList extends ArrayList<Object> {

        private RowList(int initialCapacity) {
            super(initialCapacity);
        }

        // The class is mostly empty and provides no functionality in addition
        // to what's provided by ArrayList<Object>. The main
        // purpose of the class is to allow type-safe casts from Object. These
        // casts are needed because the hash table can store both DVD[] and
        // List<DVD[]>, so its declared type is HashMap<Object, Object>.
        // Because of type erasure, casts to ArrayList<DataValueDescriptor[]>
        // will make the compiler generate unchecked conversion warnings.
        // Casts to RowList, on the other hand, won't cause warnings, as there
        // are no parameterized types and type erasure doesn't come into play.
    }
}
