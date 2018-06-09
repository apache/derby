/*

   Derby - Class org.apache.derby.iapi.store.access.DiskHashtable

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

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.StringDataValue;

import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

/**
 * This class is used by BackingStoreHashtable when the BackingStoreHashtable 
 * must spill to disk.  It implements the methods of a hash table: put, get, 
 * remove, elements, however it is not implemented as a hash table. In order to
 * minimize the amount of unique code it is implemented using a Btree and a 
 * heap conglomerate. The Btree indexes the hash code of the row key. The 
 * actual key may be too long for our Btree implementation.
 *
 * Created: Fri Jan 28 13:58:03 2005
 *
 * @version 1.0
 */

public class DiskHashtable 
{
    private final long                    rowConglomerateId;
    private       ConglomerateController  rowConglomerate;
    private final long                    btreeConglomerateId;
    private       ConglomerateController  btreeConglomerate;
    private final DataValueDescriptor[]   btreeRow;
    private final int[]                   key_column_numbers;
    private final boolean                 remove_duplicates;
    private final TransactionController   tc;
    private final DataValueDescriptor[]   row;
    private final DataValueDescriptor[]   scanKey = { new SQLInteger()};
    private int                           size;
    private boolean                       keepStatistics;
    private final boolean                 keepAfterCommit;

    /**
     * Creates a new <code>DiskHashtable</code> instance.
     *
     * @param tc
     * @param template              An array of DataValueDescriptors that 
     *                              serves as a template for the rows.
     * @param key_column_numbers    The indexes of the key columns (0 based)
     * @param remove_duplicates     If true then rows with duplicate keys are 
     *                              removed.
     * @param keepAfterCommit       If true then the hash table is kept after 
     *                              a commit
     */
    public DiskHashtable( 
    TransactionController   tc,
    DataValueDescriptor[]   template,
    int[]                   collation_ids,
    int[]                   key_column_numbers,
    boolean                 remove_duplicates,
    boolean                 keepAfterCommit)
        throws StandardException
    {
        this.tc                         = tc;
        this.key_column_numbers         = key_column_numbers;
        this.remove_duplicates          = remove_duplicates;
        this.keepAfterCommit            = keepAfterCommit;
        LanguageConnectionContext lcc   = (LanguageConnectionContext)
            getContextOrNull(
                LanguageConnectionContext.CONTEXT_ID);

        keepStatistics = (lcc != null) && lcc.getRunTimeStatisticsMode();

        // Create template row used for creating the conglomerate and 
        // fetching rows.
        row = new DataValueDescriptor[template.length];
        for( int i = 0; i < row.length; i++)
        {
            row[i] = template[i].getNewNull();

            if (SanityManager.DEBUG)
            {
                // must have an object template for all cols in hash overflow.
                SanityManager.ASSERT(
                    row[i] != null, 
                    "Template for the hash table must have non-null object");
            }
        }

        int tempFlags = 
            keepAfterCommit ? 
            (TransactionController.IS_TEMPORARY | 
             TransactionController.IS_KEPT) : 
            TransactionController.IS_TEMPORARY;
        
        // create the "base" table of the hash overflow.
        rowConglomerateId = 
            tc.createConglomerate( 
                "heap",
                template,
                (ColumnOrdering[]) null,
                collation_ids,
                (Properties) null,
                tempFlags);

        // open the "base" table of the hash overflow.
        rowConglomerate = 
            tc.openConglomerate( 
                rowConglomerateId,
                keepAfterCommit,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_NOLOCK/* Single thread only */);

        // create the index on the "hash" base table.  The key of the index
        // is the hash code of the row key.  The second column is the 
        // RowLocation of the row in the "base" table of the hash overflow.
        btreeRow = 
            new DataValueDescriptor[] 
                { new SQLInteger(), rowConglomerate.newRowLocationTemplate()};

        Properties btreeProps = new Properties();

        btreeProps.put("baseConglomerateId", 
                String.valueOf(rowConglomerateId));
        btreeProps.put("rowLocationColumn",  
                "1");
        btreeProps.put("allowDuplicates",    
                "false"); // Because the row location is part of the key
        btreeProps.put("nKeyFields",         
                "2"); // Include the row location column
        btreeProps.put("nUniqueColumns",     
                "2"); // Include the row location column
        btreeProps.put("maintainParentLinks", 
                "false");

        // default collation is used for hash code and row location
        int[] index_collation_ids = 
            {StringDataValue.COLLATION_TYPE_UCS_BASIC,
             StringDataValue.COLLATION_TYPE_UCS_BASIC};

        btreeConglomerateId = 
            tc.createConglomerate( 
                "BTREE",
                btreeRow,
                (ColumnOrdering[]) null,
                index_collation_ids,
                btreeProps,
                tempFlags);

        // open the "index" of the hash overflow.
        btreeConglomerate = 
            tc.openConglomerate( 
                btreeConglomerateId,
                keepAfterCommit,
                TransactionController.OPENMODE_FORUPDATE,
                TransactionController.MODE_TABLE,
                TransactionController.ISOLATION_NOLOCK /*Single thread only*/ );

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
     * @return true  if the row was added,
     *         false if it was not added (because it was a duplicate and we 
     *               are eliminating duplicates).
     *
     * @exception StandardException standard error policy
     */
    public boolean put(Object key, Object[] row)
        throws StandardException
    {
        boolean isDuplicate = false;
        if (remove_duplicates || keepStatistics)
        {
            // Go to the work of finding out whether it is a duplicate
            isDuplicate = (getRemove(key, false, true) != null);
            if (remove_duplicates && isDuplicate)
                return false;
        }

        // insert the row into the "base" conglomerate.
        rowConglomerate.insertAndFetchLocation( 
            (DataValueDescriptor[]) row, (RowLocation) btreeRow[1]);

        // create index row from hashcode and rowlocation just inserted, and
        // insert index row into index.
        btreeRow[0].setValue( key.hashCode());
        btreeConglomerate.insert( btreeRow);

        if (keepStatistics && !isDuplicate)
            size++;

        return true;

    } // end of put

    /**
     * Get a row from the overflow structure.
     *
     * @param key If the rows only have one key column then the key value. 
     *            If there is more than one key column then a KeyHasher
     *
     * @return null if there is no corresponding row,
     *         the row (DataValueDescriptor[]) if there is exactly one row 
     *         with the key, or
     *         a Vector of all the rows with the key if there is more than one.
     *
     * @exception StandardException
     */
    public Object get(Object key)
        throws StandardException
    {
        return getRemove(key, false, false);
    }

    private Object getRemove(Object key, boolean remove, boolean existenceOnly)
        throws StandardException
    {
        int hashCode = key.hashCode();
        int rowCount = 0;
        DataValueDescriptor[] firstRow = null;
        List<DataValueDescriptor[]> allRows = null;

        scanKey[0].setValue( hashCode);
        ScanController scan = 
            tc.openScan( 
                btreeConglomerateId,
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
            while (scan.fetchNext(btreeRow))
            {
                if (rowConglomerate.fetch(
                        (RowLocation) btreeRow[1], 
                        row, 
                        (FormatableBitSet) null /* all columns */)
                    && rowMatches( row, key))
                {
                    if( existenceOnly)
                        return this;

                    DataValueDescriptor[] clonedRow =
                            BackingStoreHashtable.shallowCloneRow(row);

                    rowCount++;
                    if( rowCount == 1)
                    {
                        // if there is only one matching row just return row. 
                        firstRow = clonedRow;
                    }
                    else 
                    {
                        // If there is more than one row, return a list of
                        // the rows.
                        //
                        if (allRows == null)
                        {
                            // convert the "single" row retrieved from the
                            // first trip in the loop, to a vector with the
                            // first two rows.
                            allRows = new ArrayList<DataValueDescriptor[]>(2);
                            allRows.add(firstRow);
                        }
                        allRows.add(clonedRow);
                    }
                    if( remove)
                    {
                        rowConglomerate.delete( (RowLocation) btreeRow[1]);
                        scan.delete();
                        size--;
                    }
                    if( remove_duplicates)
                        // This must be the only row with the key
                        return clonedRow;
                }
            }
        }
        finally
        {
            scan.close();
        }

        if (allRows == null) {
            // No duplicates. Return the single row, or null if no row was
            // found for the given key.
            return firstRow;
        } else {
            // Return list of all duplicate values.
            return allRows;
        }
    } // end of getRemove


    private boolean rowMatches( 
    DataValueDescriptor[] row,
    Object                key)
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
    public Enumeration<Object> elements()
        throws StandardException
    {
        return new ElementEnum();
    }

    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContextOrNull( final String contextID )
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContextOrNull( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContextOrNull( contextID );
                     }
                 }
                 );
        }
    }

    private class ElementEnum implements Enumeration<Object>
    {
        private ScanController scan;
        private boolean hasMore;
        private RowLocation rowloc;

        ElementEnum()
        {
            try
            {
                scan = tc.openScan( rowConglomerateId,
                                    keepAfterCommit,
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
                } else if (keepAfterCommit) {
                    rowloc = rowConglomerate.newRowLocationTemplate();
                    scan.fetchLocation(rowloc);
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
                if (scan.isHeldAfterCommit()) {
                    // automatically reopens scan:
                    if (!scan.positionAtRowLocation(rowloc)) {
                        // Will not happen unless compress of this table
                        // has invalidated the row location. Possible?
                        throw StandardException.
                            newException(SQLState.NO_CURRENT_ROW);
                    }
                }

                scan.fetch(row);

                Object retValue =  BackingStoreHashtable.shallowCloneRow( row);
                hasMore = scan.next();

                if( ! hasMore)
                {
                    scan.close();
                    scan = null;
                } else if (keepAfterCommit) {
                    scan.fetchLocation(rowloc);
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
