/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.TemplateRow

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.services.monitor.Monitor;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.services.io.Storable;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.types.SQLLongint;

import org.apache.derby.iapi.services.io.FormatableBitSet;

public final class TemplateRow
{

	/*
	** Constructors of TemplateRow
	*/

	private TemplateRow() {
	}

    /* Private/Protected methods of This class: */


    /**
     * Allocate new objects to array based on format id's and column_list.
     * <p>
     *
     * @param column_list description of partial set of columns to built as
     *                    described in RowUtil.  If null do all the columns.
     * @param format_ids  An array of format ids representing every column
     *                    in the table.  column_list describes which of these
     *                    columns to populate into the columns array.
     * @param columns     The array to place the newly allocated objects into.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    private static DataValueDescriptor[] allocate_objects(
    int         num_cols_to_allocate,
    FormatableBitSet     column_list,
    int[]       format_ids)
		throws StandardException
    {
        int         dest_pos = 0;

        DataValueDescriptor[] ret_row = 
            new DataValueDescriptor[num_cols_to_allocate];
        int         num_cols = 
            (column_list == null ? format_ids.length : column_list.size());

        for (int i = 0; i < num_cols; i++)
        {
            // does caller want this column?
            if ((column_list != null) && (!column_list.get(i)))
            {
                // no - column should be skipped.
            }
            else
            {
                // yes - create the column 

                // get empty instance of object identified by the format id.
                ret_row[i] = (DataValueDescriptor) 
                    Monitor.newInstanceFromIdentifier(format_ids[i]);

                if (SanityManager.DEBUG)
                {
                    DataValueDescriptor o = ret_row[i];

                    if (o == null)
                    {
                        SanityManager.THROWASSERT(
                        "obj from Monitor.newInstanceFromIdentifier() null." +
                        ";src column position = "  + i              +
                        ";dest column position = " + i  + 
                        ";num_cols = "             + num_cols       +
                        ";format_ids.length = "    + format_ids.length);

                    }

                    if ( ! (o instanceof Storable))
                        SanityManager.THROWASSERT(
                            "object:(" + o.getClass() +"):(" + o + 
                            ") not an instanceof Storable");
                }
            }
        }

        return(ret_row);
    }

    /* Public Methods of This class: */

    /**
    Constuctor for creating a template row which stores n SQLLongint's
    **/
	public static DataValueDescriptor[] newU8Row(int nkeys)
	{
        DataValueDescriptor[] columns = new DataValueDescriptor[nkeys];

        for (int i = 0; i < columns.length; i++)
        {
            columns[i] = new SQLLongint(Long.MIN_VALUE);
        }

		return columns;
	}

    /**
     * Generate an "empty" row to match the format id specification.
     * <p>
     * Generate an array of new'd objects matching the format id specification
     * passed in.  This routine is mostly used by the btree code to generate
     * temporary rows needed during operations like split.  It is more
     * efficient to allocate new objects based on the old object vs. calling
     * the Monitor.
     * <p>
     *
	 * @return The new row.
     *
     * @param column_list   A column list as described in RowUtil. Describes
     *                      which columns to pick out of the template and put
     *                      into returned newRow.  If null just pick all of
     *                      them.
     *
     * @param format_ids an array of format id's, one per column in row.
     *
	 * @exception  StandardException  Standard exception policy.
     *
     * @see RowUtil
     **/
    public static DataValueDescriptor[] newRow(
    DataValueDescriptor[]    template)
        throws StandardException
    {
        DataValueDescriptor[] columns = 
            new DataValueDescriptor[template.length];

        try
        {
            for (int i = template.length; i-- > 0 ;)
            {
                // get empty instance of object identified by the format id.
                columns[i] = 
                    (DataValueDescriptor) template[i].getClass().newInstance();
            }
        }
        catch (Throwable t)
        {
            // RESOLVE - Dan is investigating ways to change the monitor
            // so that it provides the functionality required here, when
            // that happens I will just all the monitor and let any 
            // StandardError that come back just go on up.
            throw(StandardException.newException(
                    SQLState.CONGLOMERATE_TEMPLATE_CREATE_ERROR));
        }

		return columns;
    }

    /**
     * Generate an "empty" row to match the format id specification.
     * <p>
     * Generate an array of new'd objects matching the format id specification
     * passed in.  This routine is mostly used by the btree code to generate
     * temporary rows needed during operations like split.
     * <p>
     *
	 * @return The new row.
     *
     * @param format_ids an array of format id's, one per column in row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static DataValueDescriptor[] newRow(
    FormatableBitSet     column_list,
    int[]        format_ids) 
        throws StandardException
    {
        return(allocate_objects(format_ids.length, column_list, format_ids));
    }

    /**
     * Generate an "empty" row to match the format id + coluumn specification.
     * <p>
     * Generate an array of new'd objects matching the format id specification
     * passed in, and the column passed in.  The new row is first made up of
     * columns matching the format ids, and then followed by one other column
     * matching the column passed in.  This routine is mostly used by the 
     * btree code to generate temporary branch rows needed during operations 
     * like split.
     * <p>
     *
	 * @return The new row.
     *
     * @param format_ids an array of format id's, one per column in row.
     * @param page_ptr   The object to place in the last column of the template.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static DataValueDescriptor[] newBranchRow(
    int[]               format_ids,
    DataValueDescriptor page_ptr) 
        throws StandardException
    {
        // allocate an object array with the number of columns in the template
        // row (ie. number of columns in the leaf row) + one column to hold
        // the page pointer in the branch row.
        DataValueDescriptor[] columns = 
            allocate_objects(
                format_ids.length + 1, (FormatableBitSet) null, format_ids);

        // tack on the page pointer to the extra column allocated onto the 
        // end of the row built from a leafrow template.
        columns[format_ids.length] = page_ptr;

		return(columns);
    }

    /**
     * Check that columns in the row conform to a set of format id's, 
     * both in number and type.
     *
	 * @return boolean indicating if template matches format id's
     *
     * @param format_ids array of format ids which are the types of cols in row
     * @param row        the array of columns that make up the row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	static public boolean checkColumnTypes(
    int[]                   format_ids, 
    DataValueDescriptor[]   row)
		throws StandardException
	{
        boolean ret_val = true;

        while (true)
        {
            int nCols = row.length;
            if (format_ids.length != row.length)
            {
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT(
                        "format_ids[] length (" + format_ids.length +
                        ") expected to be = row length (" + row.length +  ")");
                }
                ret_val = false;
                break;
            }

            if (SanityManager.DEBUG)
            {
                Object  column;
                Object  column_template;

                for (int colid = 0; colid < nCols; colid++)
                {
                    column = row[colid];

                    if (column == null)
                    {
                        SanityManager.THROWASSERT(
                            "column[" + colid + "] is null");
                    }

                    column_template = 
                        Monitor.newInstanceFromIdentifier(format_ids[colid]);


                    // is this the right check?
                    if (column.getClass() != column_template.getClass())
                    {
                        SanityManager.DEBUG_PRINT(
                            "check", "row = " +  RowUtil.toString(row));

                        SanityManager.THROWASSERT(
                            "column["+colid+"] (" + column.getClass() +
                            ") expected to be instanceof column_tempate() (" +
                            column_template.getClass() + ")" +
                            "column = " + column +
                            "row[colid] = " + row[colid]);
                    }
                }
            }
            break;
        }

        return(ret_val);
	}

    /**
     * Check that columns in the row conform to a set of format id's, 
     * both in number and type.
     *
	 * @return boolean indicating if template matches format id's
     *
     * @param format_ids array of format ids which are the types of cols in row
     * @param row        the array of columns that make up the row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	static public boolean checkPartialColumnTypes(
    int[]                   format_ids, 
    FormatableBitSet                 validColumns,
    int[]                   fieldStates,
    DataValueDescriptor[]   row)
		throws StandardException
	{
        boolean ret_val = true;

        return(ret_val);
	}
}
