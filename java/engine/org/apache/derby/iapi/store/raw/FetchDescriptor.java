/*

   Derby - Class org.apache.derby.iapi.store.raw.FetchDescriptor

   Copyright 2002, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.store.raw;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.store.access.Qualifier;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**

FetchDescriptor is used to package up all the arguments necessary to 
describe what rows and what row parts should be returned from the store
back to language as part of a fetch.
<p>
The FetchDescriptor may also contain scratch space used to process the 
qualifiers passed in the scan.  This scratch space will be used to cache
information about the qualifiers, valid column list, row size so that 
calculations need only be done once per scan rather than every iteration.
**/

public final class FetchDescriptor
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */
    private int             row_length;
    private FormatableBitSet         validColumns;
    private Qualifier[][]   qualifier_list;
    private int[]           materialized_cols;
    private int             maxFetchColumnId;

    private static final int ZERO_FILL_LENGTH  = 100;
    private static final int[] zero_fill_array = new int[ZERO_FILL_LENGTH];

    // use int arrays rather than FormatableBitSet's to get most efficient processing
    // in performance critical loop which reads columns from page.
    private int[]           validColumnsArray;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */
    FetchDescriptor()
    {
    }

    public FetchDescriptor(
    int             input_row_length)
    {
        row_length      = input_row_length;
    }

    public FetchDescriptor(
    int             input_row_length,
    int             single_valid_column_number)
    {
        row_length        = input_row_length;
        maxFetchColumnId  = single_valid_column_number;
        validColumnsArray = new int[maxFetchColumnId + 1];
        validColumnsArray[single_valid_column_number] = 1;
    }

    public FetchDescriptor(
    int             input_row_length,
    FormatableBitSet         input_validColumns,
    Qualifier[][]   input_qualifier_list)
    {
        row_length      = input_row_length;
        qualifier_list  = input_qualifier_list;

        if (qualifier_list != null)
        {
            materialized_cols           = new int[row_length];
        }

        setValidColumns(input_validColumns);
    }


    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Return the column list bit map.
     * <p>
     * A description of which columns to return from every fetch in the scan.  
     * A row array and a valid column bit map work together to describe the row
     * to be returned by the scan - see RowUtil for description of how these two
     * parameters work together to describe a "row".
     *
	 * @return The column list bit map.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public final FormatableBitSet getValidColumns()
    {
        return(validColumns);
    }

    public final int[] getValidColumnsArray()
    {
        return(validColumnsArray);
    }

    public final void setValidColumns(
    FormatableBitSet   input_validColumns)
    {
        validColumns = input_validColumns;

        setMaxFetchColumnId();

        if (validColumns != null)
        {
            validColumnsArray = new int[maxFetchColumnId + 1];
            for (int i = maxFetchColumnId; i >= 0; i--)
            {
                validColumnsArray[i] = ((validColumns.isSet(i)) ? 1 : 0);
            }
        }
    }

    /**
     * Return the qualifier array.
     * <p>
     * Return the array of qualifiers in this FetchDescriptor.  The array of 
     * qualifiers which, applied to each key, restricts the rows returned by 
     * the scan.  Rows for which any one of the qualifiers returns false are 
     * not returned by the scan. If null, all rows are returned.  Qualifiers 
     * can only reference columns which are included in the scanColumnList.  
     * The column id that a qualifier returns in the column id the table, not 
     * the column id in the partial row being returned.
     * <p>
     * A null qualifier array means there are no qualifiers.
     *
	 * @return The qualifier array, it may be null.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public final Qualifier[][] getQualifierList()
    {
        return(qualifier_list);
    }


    /**
     * Internal to store.
     **/
    public final int[] getMaterializedColumns()
    {
        return(materialized_cols);
    }


    /**
     * Internal to store.
     **/
    public final int getMaxFetchColumnId()
    {
        return(maxFetchColumnId);
    }

    private final void setMaxFetchColumnId()
    {
        maxFetchColumnId = row_length - 1;

        if (validColumns != null)
        {
            int vCol_length = validColumns.getLength();

            if (vCol_length < maxFetchColumnId + 1)
                maxFetchColumnId = vCol_length - 1;

            for (; maxFetchColumnId >= 0; maxFetchColumnId--)
            {
                if (validColumns.isSet(maxFetchColumnId))
                    break;
            }
        }
    }

    /**
     * Internal to store.
     **/
    public final void reset()
    {
        int[]   cols = materialized_cols;

        if (cols != null)
        {
            // New row, clear the array map.

            /*
             * this was too slow.
            for (int i = cols.length - 1; i >= 0;) 
            {
                
                cols[i--] = 0;
            }
            */

            if (cols.length <= ZERO_FILL_LENGTH)
            {
                // fast path the usual case.
                System.arraycopy(
                    zero_fill_array,   0, 
                    cols, 0, 
                    cols.length);
            }
            else
            {
                int offset  = 0;
                int howMany = cols.length;

                while (howMany > 0) 
                {
                    int count = 
                        howMany > zero_fill_array.length ? 
                                zero_fill_array.length : howMany;

                    System.arraycopy(
                        zero_fill_array, 0, cols, offset, count);
                    howMany -= count;
                    offset  += count;
                }
            }
        }
    }
}
