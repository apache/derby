/*

   Derby - Class org.apache.derby.iapi.store.access.DiskHashtable

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Vector;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.impl.store.access.heap.HeapRowLocation;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * This class is used by BackingStoreHashtable when the BackingStoreHashtable must spill to disk.
 * It implements the methods of a hash table: put, get, remove, elements, however it is not implemented
 * as a hash table. In order to minimize the amount of unique code it is implemented using a Btree and a heap
 * conglomerate. The Btree indexes the hash code of the row key. The actual key may be too long for
 * our Btree implementation.
 *
 * Created: Fri Jan 28 13:58:03 2005
 *
 * @author <a href="mailto:klebanof@us.ibm.com">Jack Klebanoff</a>
 * @version 1.0
 */

public class DiskHashtable 
{
    private final long rowConglomerateId;
    private ConglomerateController rowConglomerate;
    private final long btreeConglomerateId;
    private ConglomerateController btreeConglomerate;
    private final DataValueDescriptor[] btreeRow;
    private final int[] key_column_numbers;
    private final boolean remove_duplicates;
    private final TransactionController tc;
    private final DataValueDescriptor[] row;
    private final DataValueDescriptor[] scanKey = { new SQLInteger()};
    private int size;
    private boolean keepStatistics;

    /**
     * Creates a new <code>DiskHashtable</code> instance.
     *
     * @param tc
     * @param template An array of DataValueDescriptors that serves as a template for the rows.
     * @param key_column_numbers The indexes of the key columns (0 based)
     * @param remove_duplicates If true then rows with duplicate keys are removed
     * @param keepAfterCommit If true then the hash table is kept after a commit
     */
    public DiskHashtable( TransactionController tc,
                          DataValueDescriptor[] template,
                          int[] key_column_numbers,
                          boolean remove_duplicates,
                          boolean keepAfterCommit)
        throws StandardException
    {
        this.tc = tc;
        this.key_column_numbers = key_column_numbers;
        this.remove_duplicates = remove_duplicates;
        LanguageConnectionContext lcc = (LanguageConnectionContext)
				ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
        keepStatistics = (lcc != null) && lcc.getRunTimeStatisticsMode();
        row = new DataValueDescriptor[ template.length];
        for( int i = 0; i < row.length; i++)
            row[i] = template[i].getNewNull();
        int tempFlags = keepAfterCommit ? (TransactionController.IS_TEMPORARY | TransactionController.IS_KEPT)
          : TransactionController.IS_TEMPORARY;
        
        rowConglomerateId = tc.createConglomerate( "heap",
                                                   template,
                                                   (ColumnOrdering[]) null,
                                                   (Properties) null,
                                                   tempFlags);
        rowConglomerate = tc.openConglomerate( rowConglomerateId,
                                               keepAfterCommit,
                                               TransactionController.OPENMODE_FORUPDATE,
                                               TransactionController.MODE_TABLE,
                                               TransactionController.ISOLATION_NOLOCK /* Single thread only */ );

        btreeRow = new DataValueDescriptor[] { new SQLInteger(), rowConglomerate.newRowLocationTemplate()};
        Properties btreeProps = new Properties();
        btreeProps.put( "baseConglomerateId", String.valueOf( rowConglomerateId));
        btreeProps.put( "rowLocationColumn", "1");
        btreeProps.put( "allowDuplicates", "false"); // Because the row location is part of the key
        btreeProps.put( "nKeyFields", "2"); // Include the row location column
        btreeProps.put( "nUniqueColumns", "2"); // Include the row location column
        btreeProps.put( "maintainParentLinks", "false");
        btreeConglomerateId = tc.createConglomerate( "BTREE",
                                                     btreeRow,
                                                     (ColumnOrdering[]) null,
                                                     btreeProps,
                                                     tempFlags);

        btreeConglomerate = tc.openConglomerate( btreeConglomerateId,
                                                 keepAfterCommit,
                                                 TransactionController.OPENMODE_FORUPDATE,
                                                 TransactionController.MODE_TABLE,
                                                 TransactionController.ISOLATION_NOLOCK /* Single thread only */ );
    } // end of constructor

    public void close() throws StandardException
    {
        btreeConglomerate.close();
        rowConglomerate.close();
        tc.dropConglomerate( btreeConglomerateId);
        tc.dropConglomerate( rowConglomerateId);
    } // end of close
    
    /**
     * Put a new row in the overflow structure.
     *
     * @param row The row to be inserted.
     *
     * @return true if the row was added,
     *         false if it was not added (because it was a duplicate and we are eliminating duplicates).
     *
     * @exception StandardException standard error policy
     */
    public boolean put( Object key, Object[] row)
        throws StandardException
    {
        boolean isDuplicate = false;
        if( remove_duplicates || keepStatistics)
        {
            // Go to the work of finding out whether it is a duplicate
            isDuplicate = (getRemove( key, false, true) != null);
            if( remove_duplicates && isDuplicate)
                return false;
        }
        rowConglomerate.insertAndFetchLocation( (DataValueDescriptor[]) row, (RowLocation) btreeRow[1]);
        btreeRow[0].setValue( key.hashCode());
        btreeConglomerate.insert( btreeRow);
        if( keepStatistics && !isDuplicate)
            size++;
        return true;
    } // end of put

    /**
     * Get a row from the overflow structure.
     *
     * @param key If the rows only have one key column then the key value. If there is more than one
     *            key column then a KeyHasher
     *
     * @return null if there is no corresponding row,
     *         the row (DataValueDescriptor[]) if there is exactly one row with the key
     *         a Vector of all the rows with the key if there is more than one.
     *
     * @exception StandardException
     */
    public Object get( Object key)
        throws StandardException
    {
        return getRemove( key, false, false);
    }

    private Object getRemove( Object key, boolean remove, boolean existenceOnly)
        throws StandardException
    {
        int hashCode = key.hashCode();
        int rowCount = 0;
        Object retValue = null;

        scanKey[0].setValue( hashCode);
        ScanController scan = tc.openScan( btreeConglomerateId,
                                           false, // do not hold
                                           remove ? TransactionController.OPENMODE_FORUPDATE : 0,
                                           TransactionController.MODE_TABLE,
                                           TransactionController.ISOLATION_READ_UNCOMMITTED,
                                           null, // Scan all the columns
                                           scanKey,
                                           ScanController.GE,
                                           (Qualifier[][]) null,
                                           scanKey,
                                           ScanController.GT);
        try
        {
            while( scan.fetchNext( btreeRow))
            {
                if( rowConglomerate.fetch( (RowLocation) btreeRow[1], row, (FormatableBitSet) null /* all columns */)
                    && rowMatches( row, key))
                {
                    if( existenceOnly)
                        return this;

                    rowCount++;
                    if( rowCount == 1) 
                    {
                        retValue = BackingStoreHashtable.shallowCloneRow( row);                        
                    } 
                    else 
                    {
                        Vector v;
                        if( rowCount == 2)
                        {
                            v = new Vector( 2);
                            v.add( retValue);
                            retValue = v;
                        }
                        else
                        {
                            v = (Vector) retValue;
                        }
                        v.add( BackingStoreHashtable.shallowCloneRow( row));
                    }
                    if( remove)
                    {
                        rowConglomerate.delete( (RowLocation) btreeRow[1]);
                        scan.delete();
                        size--;
                    }
                    if( remove_duplicates)
                        // This must be the only row with the key
                        return retValue;
                }
            }
        }
        finally
        {
            scan.close();
        }
        return retValue;
    } // end of getRemove


    private boolean rowMatches( DataValueDescriptor[] row,
                                Object key)
    {
        if( key_column_numbers.length == 1)
            return row[ key_column_numbers[0]].equals( key);

        KeyHasher kh = (KeyHasher) key;
        for( int i = 0; i < key_column_numbers.length; i++)
        {
            if( ! row[ key_column_numbers[i]].equals( kh.getObject(i)))
                return false;
        }
        return true;
    } // end of rowMatches

    /**
     * remove all rows with a given key from the hash table.
     *
     * @param key          The key of the rows to remove.
     *
     * @return The removed row(s).
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Object remove( Object key)
		throws StandardException
    {
        return getRemove( key, true, false);
    } // end of remove

    /**
     * @return The number of rows in the hash table
     */
    public int size()
    {
        return size;
    }
    
    /**
     * Return an Enumeration that can be used to scan entire table.
     * <p>
     * RESOLVE - is it worth it to support this routine?
     *
	 * @return The Enumeration.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public Enumeration elements()
        throws StandardException
    {
        return new ElementEnum();
    }

    private class ElementEnum implements Enumeration
    {
        private ScanController scan;
        private boolean hasMore;

        ElementEnum()
        {
            try
            {
                scan = tc.openScan( rowConglomerateId,
                                    false, // do not hold
                                    0, // read only
                                    TransactionController.MODE_TABLE,
                                    TransactionController.ISOLATION_NOLOCK,
                                    (FormatableBitSet) null, // all columns
                                    (DataValueDescriptor[]) null, // no start key
                                    0, // no start key operator
                                    (Qualifier[][]) null,
                                    (DataValueDescriptor[]) null, // no stop key
                                    0 /* no stop key operator */);
                hasMore = scan.next();
                if( ! hasMore)
                {
                    scan.close();
                    scan = null;
                }
            }
            catch( StandardException se)
            {
                hasMore = false;
                if( scan != null)
                {
                    try
                    {
                        scan.close();
                    }
                    catch( StandardException se1){};
                    scan = null;
                }
            }
        } // end of constructor

        public boolean hasMoreElements()
        {
            return hasMore;
        }

        public Object nextElement()
        {
            if( ! hasMore)
                throw new NoSuchElementException();
            try
            {
                scan.fetch( row);
                Object retValue =  BackingStoreHashtable.shallowCloneRow( row);
                hasMore = scan.next();
                if( ! hasMore)
                {
                    scan.close();
                    scan = null;
                }

                return retValue;
            }
            catch( StandardException se)
            {
                if( scan != null)
                {
                    try
                    {
                        scan.close();
                    }
                    catch( StandardException se1){};
                    scan = null;
                }
                throw new NoSuchElementException();
            }
        } // end of nextElement
    } // end of class ElementEnum
}
