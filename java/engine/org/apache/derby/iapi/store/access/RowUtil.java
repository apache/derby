/*

   Derby - Class org.apache.derby.iapi.store.access.RowUtil

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

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException; 
import org.apache.derby.iapi.services.io.Storable;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.InstanceGetter;

import org.apache.derby.iapi.store.raw.FetchDescriptor;

import java.lang.reflect.InvocationTargetException;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
  A set of static utility methods to work with rows.
  <P>
  A row or partial row is described by two or three parameters.
  <OL>
  <LI>DataValueDescriptor[] row - an array of objects, one per column.
  <LI>FormatableBitSet validColumns - 
      an indication of which objects in row map to which columns
  </OL>
  These objects can describe a complete row or a partial row. A partial row is 
  one where a sub-set (e.g. columns 0, 4 and 7) of the columns are supplied 
  for update, or requested to be fetched on a read.  Here's an example
  of code to set up a partial column list to fetch the 0th (type FOO), 
  4th (type BAR), and 7th (type MMM) columns from a row with 10 columns, note
  that the format for a partial row changed from a "packed" representation
  in the 3.0 release to a "sparse" representation in later releases:

  <blockquote><pre>

  // allocate/initialize the row 
  DataValueDescriptor row = new DataValueDescriptor[10]
  row[0] = new FOO();
  row[4] = new BAR();
  row[7] = new MMM();
  
  // allocate/initialize the bit set 
  FormatableBitSet FormatableBitSet = new FormatableBitSet(10);
  
  FormatableBitSet.set(0);
  FormatableBitSet.set(4);
  FormatableBitSet.set(7);
  </blockquote></pre>


  <BR><B>Column mapping<B><BR>
  When validColumns is null:
  <UL>
  <LI> The number of columns is given by row.length
  <LI> Column N maps to row[N], where column numbers start at zero.
  </UL>
  <BR>
  When validColumns is not null, then
  <UL>
  <LI> The number of requested columns is given by the number of bits set in 
       validColumns.
  <LI> Column N is not in the partial row if validColumns.isSet(N) 
       returns false.
  <LI> Column N is in the partial row if validColumns.isSet(N) returns true.
  <LI> If column N is in the partial row then it maps to row[N].
	   If N >= row.length then the column is taken as non existent for an
	   insert or update, and not fetched on a fetch.
  </UL>
  If row.length is greater than the number of columns indicated by validColumns
  the extra entries are ignored.

**/
public class RowUtil
{
	private RowUtil() {}

	/**
		An object that can be used on a fetch to indicate no fields
		need to be fetched.
	*/
	public static final DataValueDescriptor[] EMPTY_ROW = 
        new DataValueDescriptor[0];

	/**
		An object that can be used on a fetch as a FormatableBitSet to indicate no fields
		need to be fetched.
	*/
	public static final FormatableBitSet EMPTY_ROW_BITSET  = 
        new FormatableBitSet(0);

	/**
		An object that can be used on a fetch as a FormatableBitSet to indicate no fields
		need to be fetched.
	*/
	public static final FetchDescriptor EMPTY_ROW_FETCH_DESCRIPTOR  = 
        new FetchDescriptor(0);

	public static final FetchDescriptor[] ROWUTIL_FETCH_DESCRIPTOR_CONSTANTS  =
        {EMPTY_ROW_FETCH_DESCRIPTOR,
         new FetchDescriptor(1, 1),
         new FetchDescriptor(2, 2),
         new FetchDescriptor(3, 3),
         new FetchDescriptor(4, 4),
         new FetchDescriptor(5, 5),
         new FetchDescriptor(6, 6),
         new FetchDescriptor(7, 7)};


	/**
		Get the object for a column identifer (0 based) from a complete or 
        partial row.

		@param row the row
		@param columnList valid columns in the row
		@param columnId which column to return (0 based)

		@return the obejct for the column, or null if the column is not represented.
	*/
	public static DataValueDescriptor getColumn(
    DataValueDescriptor[]   row, 
    FormatableBitSet                 columnList, 
    int                     columnId) 
    {

		if (columnList == null)
			return columnId < row.length ? row[columnId] : null;


		if (!(columnList.getLength() > columnId && columnList.isSet(columnId)))
			return null;

        return columnId < row.length ? row[columnId] : null;

	}

	public static Object getColumn(
    Object[]   row, 
    FormatableBitSet                 columnList, 
    int                     columnId) 
    {

		if (columnList == null)
			return columnId < row.length ? row[columnId] : null;


		if (!(columnList.getLength() > columnId && columnList.isSet(columnId)))
			return null;

        return columnId < row.length ? row[columnId] : null;

	}

	/**
		Get a FormatableBitSet representing all the columns represented in
		a qualifier list.

		@return a FormatableBitSet describing the valid columns.
	*/
	public static FormatableBitSet getQualifierBitSet(Qualifier[][] qualifiers) 
    {
		FormatableBitSet qualifierColumnList = new FormatableBitSet();

		if (qualifiers != null) 
        {
			for (int i = 0; i < qualifiers.length; i++)
			{
                for (int j = 0; j < qualifiers[i].length; j++)
                {
                    int colId = qualifiers[i][j].getColumnId();

                    // we are about to set bit colId, need length to be colId+1
                    qualifierColumnList.grow(colId+1);
                    qualifierColumnList.set(colId);
                }
			}
		}

		return qualifierColumnList;
	}

    /**
     * Get the number of columns represented by a FormatableBitSet.
     * <p>
     * This is simply a count of the number of bits set in the FormatableBitSet.
     * <p>
     *
     * @param maxColumnNumber Because the FormatableBitSet.size() can't be used as
     *                        the number of columns, allow caller to tell
     *                        the maximum column number if it knows.  
     *                        -1  means caller does not know.
     *                        >=0 number is the largest column number.
     *                           
     * @param columnList valid columns in the row
     *
	 * @return The number of columns represented in the FormatableBitSet.
     **/
    public static int getNumberOfColumns(
    int     maxColumnNumber,
    FormatableBitSet  columnList)
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(columnList != null);

        int max_col_number = columnList.getLength();

		if (maxColumnNumber > 0 && maxColumnNumber < max_col_number)
			max_col_number = maxColumnNumber;

        int ret_num_cols = 0;

        for (int i = 0; i < max_col_number; i++)
        {
            if (columnList.isSet(i))
                ret_num_cols++;
        }

        return(ret_num_cols);
    }

	/**
		See if a row actually contains no columns.
		Returns true if row is null, row.length is null,
		or columnList is not null but has not bits set.

		@return true if no columns are selected in this row.
	*/
	public static boolean isRowEmpty(
    DataValueDescriptor[]   row, 
    FormatableBitSet                 columnList) 
    {

		if (row == null)
			return true;

		if (row.length == 0)
			return true;

		if (columnList == null)
			return false;

		int size = columnList.getLength();
		for (int i = 0; i < size; i--) {
			if (columnList.isSet(i))
				return true;
		}
		return false;
	}

	/**
		Return the column number of the first column out of range, or a number
        less than zero if all columns are in range.
	*/
	public static int columnOutOfRange(
    DataValueDescriptor[]   row, 
    FormatableBitSet                 columnList, 
    int                     maxColumns) 
    {

		if (columnList == null) {
			if (row.length > maxColumns)
				return maxColumns;

			return -1;
		}

		int size = columnList.getLength();
		for (int i = maxColumns; i < size; i++) {
			if (columnList.isSet(i))
				return i;
		}

		return -1;
	}

	/**
		Get the next valid column after or including start column.
		Returns -1 if no valid columns exist after startColumn
	*/
	public static int nextColumn(
    Object[]   row, 
    FormatableBitSet                 columnList, 
    int                     startColumn) 
    {

		if (columnList != null) {

			int size = columnList.getLength();

			for (; startColumn < size; startColumn++) {
				if (columnList.isSet(startColumn)) {
					return startColumn;
				}
			}

			return -1;
		}

		if (row == null)
			return -1;

		return startColumn < row.length ? startColumn : -1;
	}

    /**
     * Return a FetchDescriptor which describes a single column set.
     * <p>
     * This routine returns one of a set of constant FetchDescriptor's, and
     * should not be altered by the caller.
     **/
    public static final FetchDescriptor getFetchDescriptorConstant(
    int     single_column_number)
    {
        if (single_column_number < ROWUTIL_FETCH_DESCRIPTOR_CONSTANTS.length)
        {
            return(ROWUTIL_FETCH_DESCRIPTOR_CONSTANTS[single_column_number]);
        }
        else
        {
            return(
                new FetchDescriptor(
                    single_column_number, single_column_number));
        }
    }

    /**************************************************************************
     * Public Methods dealing with cloning and row copying util functions
     **************************************************************************
     */

    /**
     * Generate a row of InstanceGetter objects to be used to generate  "empty" rows.
     * <p>
     * Generate an array of InstanceGetter objects which will be used to make
     * repeated calls to newRowFromClassInfoTemplate(), to repeatedly and
     * efficiently generate new rows.  This is important for certain 
     * applications like the sorter and fetchSet which generate large numbers
     * of "new" empty rows.
     * <p>
     *
	 * @return The new row.
     *
     * @param format_ids an array of format id's, one per column in row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static InstanceGetter[] newClassInfoTemplate(
    FormatableBitSet column_list,
    int[]    format_ids) 
        throws StandardException
    {
        int         num_cols = format_ids.length;
        InstanceGetter[] ret_row  = new InstanceGetter[num_cols];

		int column_listSize = 
            (column_list == null) ? 0 : column_list.getLength();

        for (int i = 0; i < num_cols; i++)
        {
            // does caller want this column?
            if ((column_list != null)   && 
                !((column_listSize > i) && 
                (column_list.isSet(i))))
            {
                // no - column should be skipped.
            }
            else
            {
                // yes - create the column 

                // get empty instance of object identified by the format id.

                ret_row[i] = Monitor.classFromIdentifier(format_ids[i]);
            }
        }

        return(ret_row);
    }


    private static void newRowFromClassInfoTemplateError()
    {
        if (SanityManager.DEBUG)
            SanityManager.THROWASSERT(
                "unexpected error in newRowFromClassInfoTemplate()");
    }

    /**
     * Generate an "empty" row from an array of classInfo objects.
     * <p>
     * Generate an array of new'd objects by using the getNewInstance()
     * method on each of the InstanceGetter objects.  It is more
     * efficient to allocate new objects based on this "cache'd"
     * InstanceGetter object than to call the Monitor to generate a new class
     * from a format id.
     * <p>
     *
	 * @return The new row.
     *
     * @param classinfo_template   An array of InstanceGetter objects each of 
     *                             which can be used to create a new instance 
     *                             of the appropriate type to build a new empty
     *                             template row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static DataValueDescriptor[] newRowFromClassInfoTemplate(
    InstanceGetter[]    classinfo_template) 
        throws StandardException
    {

        DataValueDescriptor[] columns = 
            new DataValueDescriptor[classinfo_template.length];

        try
        {
            for (int column_index = classinfo_template.length; 
                 column_index-- > 0;)
            {
                if (classinfo_template[column_index] != null)
                {
                    // get empty instance of DataValueDescriptor identified by 
                    // the format id.
                    columns[column_index] = (DataValueDescriptor) 
                        classinfo_template[column_index].getNewInstance();
                }
            }
        }
        catch (InstantiationException ie)
        {
            newRowFromClassInfoTemplateError();
        }
        catch (IllegalAccessException iae)
        {
            newRowFromClassInfoTemplateError();
        }
        catch (InvocationTargetException ite)
        {
            newRowFromClassInfoTemplateError();
        }

		return columns;
    }


    /**
     * return string version of row.
     * <p>
     * For debugging only. 
     *
	 * @return The string version of row.
     *
     * @param row The row.
     *
     **/
    public static String toString(Object[] row)
    {
        if (SanityManager.DEBUG)
        {

            String str = new String();

            if (row != null)
            {
                if (row.length == 0)
                {
                    str = "empty row";
                }
                else
                {
                    for (int i = 0; i < row.length; i++)
                        str += "col[" + i + "]=" + row[i];
                }
            }
            else
            {
                str = "row is null";
            }

            return(str);
        }
        else
        {
            return(null);
        }
    }

    /**
     * return string version of a HashTable returned from a FetchSet.
     * <p>
     *
	 * @return The string version of row.
     *
     * @param row The row.
     *
     **/

    // For debugging only. 
    public static String toString(Hashtable hash_table)
    {
        if (SanityManager.DEBUG)
        {
            String str = new String();

            Object  row_or_vector;
            Enumeration enum = hash_table.elements();

            for (Enumeration e = hash_table.elements(); e.hasMoreElements();)
            {
                row_or_vector = e.nextElement();

                if (row_or_vector instanceof Object[])
                {
                    // it's a row
                    str += RowUtil.toString((Object[]) row_or_vector);
                    str += "\n";
                }
                else if (row_or_vector instanceof Vector)
                {
                    // it's a vector
                    Vector vec = (Vector) row_or_vector;

                    for (int i = 0; i < vec.size(); i++)
                    {
                        str += 
                            "vec[" + i + "]:" + 
                            RowUtil.toString((Object[]) vec.elementAt(i));

                        str += "\n";
                    }
                }
                else
                {
                    str += "BAD ENTRY\n";
                }
            }
            return(str);
        }
        else
        {
            return(null);
        }
    }

    /**
     * Process the qualifier list on the row, return true if it qualifies.
     * <p>
     * A two dimensional array is to be used to pass around a AND's and OR's in
     * conjunctive normal form.  The top slot of the 2 dimensional array is 
     * optimized for the more frequent where no OR's are present.  The first 
     * array slot is always a list of AND's to be treated as described above 
     * for single dimensional AND qualifier arrays.  The subsequent slots are 
     * to be treated as AND'd arrays or OR's.  Thus the 2 dimensional array 
     * qual[][] argument is to be treated as the following, note if 
     * qual.length = 1 then only the first array is valid and it is and an 
     * array of and clauses:
     *
     * (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
     * and
     * (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
     * and
     * (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
     * ...
     * and
     * (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])
     *
     * 
	 * @return true if the row qualifies.
     *
     * @param row               The row being qualified.
     * @param qual_list         2 dimensional array representing conjunctive
     *                          normal form of simple qualifiers.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public static final boolean qualifyRow(
    Object[]        row, 
    Qualifier[][]   qual_list)
		 throws StandardException
	{
        boolean     row_qualifies = true;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row != null);
        }

        // First do the qual[0] which is an array of qualifer terms.

        if (SanityManager.DEBUG)
        {
            // routine should not be called if there is no qualifier
            SanityManager.ASSERT(qual_list != null);
            SanityManager.ASSERT(qual_list.length > 0);
        }

        for (int i = 0; i < qual_list[0].length; i++)
        {
            // process each AND clause 

            row_qualifies = false;

            // process each OR clause.

            Qualifier q = qual_list[0][i];

            // Get the column from the possibly partial row, of the 
            // q.getColumnId()'th column in the full row.
            DataValueDescriptor columnValue = 
                    (DataValueDescriptor) row[q.getColumnId()];

            row_qualifies =
                columnValue.compare(
                    q.getOperator(),
                    q.getOrderable(),
                    q.getOrderedNulls(),
                    q.getUnknownRV());

            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;

            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // all the qual[0] and terms passed, now process the OR clauses

        for (int and_idx = 1; and_idx < qual_list.length; and_idx++)
        {
            // loop through each of the "and" clause.

            row_qualifies = false;

            if (SanityManager.DEBUG)
            {
                // Each OR clause must be non-empty.
                SanityManager.ASSERT(qual_list[and_idx].length > 0);
            }

            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++)
            {
                // Apply one qualifier to the row.
                Qualifier q      = qual_list[and_idx][or_idx];
                int       col_id = q.getColumnId();

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        (col_id < row.length),
                        "Qualifier is referencing a column not in the row.");
                }

                // Get the column from the possibly partial row, of the 
                // q.getColumnId()'th column in the full row.
                DataValueDescriptor columnValue = 
                    (DataValueDescriptor) row[q.getColumnId()];

                if (SanityManager.DEBUG)
                {
                    if (columnValue == null)
                        SanityManager.THROWASSERT(
                            "1:row = " + RowUtil.toString(row) +
                            "row.length = " + row.length +
                            ";q.getColumnId() = " + q.getColumnId());
                }

                // do the compare between the column value and value in the
                // qualifier.
                row_qualifies = 
                    columnValue.compare(
                            q.getOperator(),
                            q.getOrderable(),
                            q.getOrderedNulls(),
                            q.getUnknownRV());

                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "processing qual[" + and_idx + "][" + or_idx + "] = " + qual_list[and_idx][or_idx] );

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "value = " + row_qualifies);

                // processing "OR" clauses, so as soon as one is true, break
                // to go and process next AND clause.
                if (row_qualifies)
                    break;

            }

            // The qualifier list represented a set of "AND'd" 
            // qualifications so as soon as one is false processing is done.
            if (!row_qualifies)
                break;
        }

        return(row_qualifies);
    }

}
