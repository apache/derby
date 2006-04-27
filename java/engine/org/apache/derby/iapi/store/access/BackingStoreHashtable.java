/*

   Derby - Class org.apache.derby.iapi.store.access.BackingStoreHashtable

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

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.types.CloneableObject;
import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.cache.ClassSize;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties; 
import java.util.Vector;
import java.util.NoSuchElementException;

/**
A BackingStoreHashtable is a utility class which will store a set of rows into
an in memory hash table, or overflow the hash table to a tempory on disk 
structure.
<p>
All rows must contain the same number of columns, and the column at position
N of all the rows must have the same format id.  If the BackingStoreHashtable needs to be
overflowed to disk, then an arbitrary row will be chosen and used as a template
for creating the underlying overflow container.

<p>
The hash table will be built logically as follows (actual implementation
may differ).  The important points are that the hash value is the standard
java hash value on the row[key_column_numbers[0], if key_column_numbers.length is 1,
or row[key_column_numbers[0, 1, ...]] if key_column_numbers.length > 1, 
and that duplicate detection is done by the standard java duplicate detection provided by 
java.util.Hashtable.
<p>
import java.util.Hashtable;

hash_table = new Hashtable();

Object[] row;
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

**/

public class BackingStoreHashtable
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private TransactionController tc;
    private Hashtable   hash_table;
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
     * The estimated number of bytes used by Vector(0)
     */  
    private final static int vectorSize = ClassSize.estimateBaseFromCatalog(java.util.Vector.class);
    
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
     * constructor.  
     * <p>
     * If the number of rows is <= "max_inmemory_rowcnt", then the rows are
     * inserted into a java.util.Hashtable.  In this case no 
     * TransactionController is necessary, a "null" tc is valid.
     * <p>
     * If the number of rows is > "max_inmemory_rowcnt", then the rows will
     * be all placed in some sort of Access temporary file on disk.  This 
     * case requires a valid TransactionController.
     *
     * @param tc                An open TransactionController to be used if the
     *                          hash table needs to overflow to disk.
     *
     * @param row_source        RowSource to read rows from.
     *
     * @param key_column_numbers The column numbers of the columns in the
     *                          scan result row to be the key to the Hashtable.
     *                          "0" is the first column in the scan result
     *                          row (which may be different than the first
     *                          row in the table of the scan).
     *
     * @param remove_duplicates Should the Hashtable automatically remove
     *                          duplicates, or should it create the Vector of
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
     * @param initialCapacity   If not "-1" used to initialize the java 
     *                          Hashtable.
     *
     * @param loadFactor        If not "-1" used to initialize the java 
     *                          Hashtable.
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
        if( max_inmemory_rowcnt > 0)
            max_inmemory_size = Long.MAX_VALUE;
        else
            max_inmemory_size = Runtime.getRuntime().totalMemory()/100;
        this.tc = tc;
        this.keepAfterCommit = keepAfterCommit;

        Object[] row;

        // use passed in capacity and loadfactor if not -1, you must specify
        // capacity if you want to specify loadfactor.
        if (initialCapacity != -1)
        {
            hash_table = 
                ((loadFactor == -1) ? 
                     new Hashtable(initialCapacity) : 
                     new Hashtable(initialCapacity, loadFactor));
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
             * create a Hashtable of size estimated_rowcnt can cause
             * OutOfMemory errors when we try to create the Hashtable.
             * So as a "red flag" for that kind of situation, we check to
             * see if the estimated row count is greater than the max
             * in-memory size for this table.  Unit-wise this comparison
             * is relatively meaningless: rows vs bytes.  But if our
             * estimated row count is greater than the max number of
             * in-memory bytes that we're allowed to consume, then
             * it's very likely that creating a Hashtable with a capacity
             * of estimated_rowcnt will lead to memory problems.  So in
             * that particular case we leave hash_table null here and
             * initialize it further below, using the estimated in-memory
             * size of the first row to figure out what a reasonable size
             * for the Hashtable might be.
             */
            hash_table = 
                (((estimated_rowcnt <= 0) || (row_source == null)) ?
                     new Hashtable() :
                     (estimated_rowcnt < max_inmemory_size) ?
                         new Hashtable((int) estimated_rowcnt) :
                         null);
        }

        if (row_source != null)
        {
            boolean needsToClone = row_source.needsToClone();

            while ((row = getNextRowFromRowSource()) != null)
            {
                // If we haven't initialized the hash_table yet then that's
                // because a Hashtable with capacity estimated_rowcnt would
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
                    // capacity of the Hashtable.
                    double rowUsage = getEstimatedMemUsage(row);
                    hash_table = new Hashtable((int)(max_inmemory_size / rowUsage));
                }

                if (needsToClone)
                {
                    row = cloneRow(row);
                }

                Object key = 
                    KeyHasher.buildHashKey(row, key_column_numbers);

                add_row_to_hash_table(hash_table, key, row);
            }
        }
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

	/**
	 * Call method to either get next row or next row with non-null
	 * key columns.
	 *
     *
	 * @exception  StandardException  Standard exception policy.
	 */
	private Object[] getNextRowFromRowSource()
		throws StandardException
	{
		Object[] row = row_source.getNextRowFromRowSource();

		if (skipNullKeyColumns)
		{
			while (row != null)
			{
				// Are any key columns null?
				int index = 0;
				for ( ; index < key_column_numbers.length; index++)
				{
					if (SanityManager.DEBUG)
					{
						if (! (row[key_column_numbers[index]] instanceof Storable))
						{
							SanityManager.THROWASSERT(
								"row[key_column_numbers[index]] expected to be Storable, not " +
								row[key_column_numbers[index]].getClass().getName());
						}
					}
					Storable storable = (Storable) row[key_column_numbers[index]];
					if (storable.isNull())
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
    static Object[] cloneRow(Object[] old_row)
        throws StandardException
    {
        Object[] new_row = new DataValueDescriptor[old_row.length];

		// the only difference between getClone and cloneObject is cloneObject does
		// not objectify a stream.  We use getClone here.  Beetle 4896.
        for (int i = 0; i < old_row.length; i++)
        {
            if( old_row[i] != null)
                new_row[i] = ((DataValueDescriptor) old_row[i]).getClone();
        }

        return(new_row);
    }

    /**
     * Do the work to add one row to the hash table.
     * <p>
     *
     * @param row               Row to add to the hash table.
     * @param hash_table        The java HashTable to load into.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private void add_row_to_hash_table(
    Hashtable   hash_table,
    Object      key,
    Object[]    row)
		throws StandardException
    {
        if( spillToDisk( hash_table, key, row))
            return;
        
        Object  duplicate_value = null;

        if ((duplicate_value = hash_table.put(key, row)) == null)
            doSpaceAccounting( row, false);
        else
        {
            if (!remove_duplicates)
            {
                Vector row_vec;

                // inserted a duplicate
                if ((duplicate_value instanceof Vector))
                {
                    doSpaceAccounting( row, false);
                    row_vec = (Vector) duplicate_value;
                }
                else
                {
                    // allocate vector to hold duplicates
                    row_vec = new Vector(2);

                    // insert original row into vector
                    row_vec.addElement(duplicate_value);
                    doSpaceAccounting( row, true);
                }

                // insert new row into vector
                row_vec.addElement(row);

                // store vector of rows back into hash table,
                // overwriting the duplicate key that was 
                // inserted.
                hash_table.put(key, row_vec);
            }
        }

        row = null;
    }

    private void doSpaceAccounting( Object[] row,
                                    boolean firstDuplicate)
    {
        inmemory_rowcnt++;
        if( max_inmemory_rowcnt <= 0)
        {
            max_inmemory_size -= getEstimatedMemUsage(row);
            if( firstDuplicate)
                max_inmemory_size -= vectorSize;
        }
    } // end of doSpaceAccounting

    /**
     * Determine whether a new row should be spilled to disk and, if so, do it.
     *
     * @param hash_table The in-memory hash table
     * @param key The row's key
     * @param row
     *
     * @return true if the row was spilled to disk, false if not
     *
     * @exception  StandardException  Standard exception policy.
     */
    private boolean spillToDisk( Hashtable   hash_table,
                                 Object      key,
                                 Object[]    row)
		throws StandardException
    {
        // Once we have started spilling all new rows will go to disk, even if we have freed up some
        // memory by moving duplicates to disk. This simplifies handling of duplicates and accounting.
        if( diskHashtable == null)
        {
            if( max_inmemory_rowcnt > 0)
            {
                if( inmemory_rowcnt < max_inmemory_rowcnt)
                    return false; // Do not spill
            }
            else if( max_inmemory_size > 0)
                return false;
            // Want to start spilling
            if( ! (row instanceof DataValueDescriptor[]))
            {
                if( SanityManager.DEBUG)
                    SanityManager.THROWASSERT( "BackingStoreHashtable row is not DataValueDescriptor[]");
                // Do not know how to put it on disk
                return false;
            }
            diskHashtable = new DiskHashtable( tc,
                                               (DataValueDescriptor[]) row,
                                               key_column_numbers,
                                               remove_duplicates,
                                               keepAfterCommit);
        }
        
        Object duplicateValue = hash_table.get( key);
        if( duplicateValue != null)
        {
            if( remove_duplicates)
                return true; // a degenerate case of spilling
            // If we are keeping duplicates then move all the duplicates from memory to disk
            // This simplifies finding duplicates: they are either all in memory or all on disk.
            if( duplicateValue instanceof Vector)
            {
                Vector duplicateVec = (Vector) duplicateValue;
                for( int i = duplicateVec.size() - 1; i >= 0; i--)
                {
                    Object[] dupRow = (Object[]) duplicateVec.elementAt(i);
                    diskHashtable.put( key, dupRow);
                }
            }
            else
                diskHashtable.put( key, (Object []) duplicateValue);
            hash_table.remove( key);
        }
        diskHashtable.put( key, row);
        return true;
    } // end of spillToDisk

    /**
     * Take a row and return an estimate as to how much memory that
     * row will consume.
     * 
     * @param row The row for which we want to know the memory usage.
     * @return A guess as to how much memory the current row will
     *  use.
     */
    private long getEstimatedMemUsage(Object [] row)
    {
        long rowMem = 0;
        for( int i = 0; i < row.length; i++)
        {
            if (row[i] instanceof DataValueDescriptor)
                rowMem += ((DataValueDescriptor) row[i]).estimateMemoryUsage();
            rowMem += ClassSize.refSize;
        }

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
        if( diskHashtable != null)
        {
            diskHashtable.close();
            diskHashtable = null;
        }
        return;
    }

    /**
     * Return an Enumeration that can be used to scan entire table.
     * <p>
     * RESOLVE - is it worth it to support this routine when we have a
     *           disk overflow hash table?
     *
	 * @return The Enumeration.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Enumeration elements()
        throws StandardException
    {
        if( diskHashtable == null)
            return(hash_table.elements());
        return new BackingStoreHashtableEnumeration();
    }

    /**
     * get data associated with given key.
     * <p>
     * There are 2 different types of objects returned from this routine.
     * <p>
	 * In both cases, the key value is either the object stored in 
     * row[key_column_numbers[0]], if key_column_numbers.length is 1, 
     * otherwise it is a KeyHasher containing
	 * the objects stored in row[key_column_numbers[0, 1, ...]].
     * For every qualifying unique row value an entry is placed into the 
     * Hashtable.
     * <p>
     * For row values with duplicates, the value of the data is a Vector of
     * rows.
     * <p>
     * The caller will have to call "instanceof" on the data value
     * object if duplicates are expected, to determine if the data value
     * of the Hashtable entry is a row or is a Vector of rows.
     * <p>
     * The BackingStoreHashtable "owns" the objects returned from the get()
     * routine.  They remain valid until the next access to the 
     * BackingStoreHashtable.  If the client needs to keep references to these
     * objects, it should clone copies of the objects.  A valid 
     * BackingStoreHashtable can place all rows into a disk based conglomerate,
     * declare a row buffer and then reuse that row buffer for every get()
     * call.
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
        if( diskHashtable == null || obj != null)
            return obj;
        return diskHashtable.get( key);
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
            org.apache.derby.iapi.util.PropertyUtil.copyProperties(auxillary_runtimestats, prop);
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
        if( obj != null || diskHashtable == null)
            return obj;
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
     * If rouine returns false, then no reference is kept to the duplicate
     * row which was rejected (thus allowing caller to reuse the object).
     *
     * @param needsToClone does this routine have to make a copy of the row,
     *                     in order to keep a reference to it after return?
     * @param row          The row to insert into the table.
     *
	 * @return true if row was inserted into the hash table.  Returns
     *              false if the BackingStoreHashtable is eliminating 
     *              duplicates, and the row being inserted is a duplicate,
	 *				or if we are skipping rows with 1 or more null key columns
	 *				and we find a null key column.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public boolean put(
    boolean     needsToClone,
    Object[]    row)
		throws StandardException
    {
		// Are any key columns null?
		if (skipNullKeyColumns)
		{
			int index = 0;
			for ( ; index < key_column_numbers.length; index++)
			{
				if (SanityManager.DEBUG)
				{
					if (! (row[key_column_numbers[index]] instanceof Storable))
					{
						SanityManager.THROWASSERT(
							"row[key_column_numbers[index]] expected to be Storable, not " +
							row[key_column_numbers[index]].getClass().getName());
					}
				}
				Storable storable = (Storable) row[key_column_numbers[index]];
				if (storable.isNull())
				{
					return false;
				}
			}
		}

        if (needsToClone)
        {
            row = cloneRow(row);
        }

        Object key = KeyHasher.buildHashKey(row, key_column_numbers);

        if ((remove_duplicates) && (get(key) != null))
        {
            return(false);
        }
        else
        {
            add_row_to_hash_table(hash_table, key, row);
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
        if( diskHashtable == null)
            return(hash_table.size());
        return hash_table.size() + diskHashtable.size();
    }

    private class BackingStoreHashtableEnumeration implements Enumeration
    {
        private Enumeration memoryEnumeration;
        private Enumeration diskEnumeration;

        BackingStoreHashtableEnumeration()
        {
            memoryEnumeration = hash_table.elements();
            if( diskHashtable != null)
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
            if( memoryEnumeration != null)
            {
                if( memoryEnumeration.hasMoreElements())
                    return true;
                memoryEnumeration = null;
            }
            if( diskEnumeration == null)
                return false;
            return diskEnumeration.hasMoreElements();
        }

        public Object nextElement() throws NoSuchElementException
        {
            if( memoryEnumeration != null)
            {
                if( memoryEnumeration.hasMoreElements())
                    return memoryEnumeration.nextElement();
                memoryEnumeration = null;
            }
            return diskEnumeration.nextElement();
        }
    } // end of class BackingStoreHashtableEnumeration
}
