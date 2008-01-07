/*

   Derby - Class org.apache.derby.impl.store.access.conglomerate.OpenConglomerateScratchSpace

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

package org.apache.derby.impl.store.access.conglomerate;

import org.apache.derby.iapi.error.StandardException; 

import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.RowUtil;

import org.apache.derby.iapi.store.raw.Transaction;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.services.io.FormatableBitSet;

/**

A utility class to store and use temporary scratch space associated with
a conglomerate.

**/

public class OpenConglomerateScratchSpace 
    implements DynamicCompiledOpenConglomInfo
{

    /**************************************************************************
     * Fields of the class
     **************************************************************************
     */

    /**
     * A template of info about the classes in the returned row.
     * <p>
     * This template is allocated on demand, and is used to efficiently
     * create new rows for export from this class.  This variable is for
     * use by get_row_for_export().
     **/
    private DataValueDescriptor[]   row_for_export_template;

    /**
     * A Scratch template used for searching and qualifying rows in the 
     * conglomerate.  This is a full template, independent of the 
     * FormatableBitSet used for access.
     **/
    private DataValueDescriptor[]   scratch_template;

    /**
     * A Scratch row used for qualifying rows in the 
     * conglomerate.  This is a row which matches the FormatableBitSet of rows
     * being returned.
     **/
    private DataValueDescriptor[]   scratch_row;

    /**
     * A complete array of format id's and collation_ids for this conglomerate.
     **/
    private int[]                   format_ids;
    private int[]                   collation_ids;


    /**
     * Scratch space used by <code>ConglomerateController</code>.
     * 
     * @see org.apache.derby.iapi.store.access.ConglomerateController#delete
     * @see org.apache.derby.iapi.store.access.ConglomerateController#replace
     */
    private RowPosition             scratch_row_position;

    /**************************************************************************
     * Constructors for This class:
     **************************************************************************
     */

    /**
     * Creates a new scratch space.
     *
     * @param format_ids format identifiers for columns in the row
     * @param collation_ids collation identifiers for the columns in the row
     */
    public OpenConglomerateScratchSpace(
    int[]   format_ids,
    int[]   collation_ids)
    {
        this.format_ids     = format_ids;
        this.collation_ids  = collation_ids;
    }

    /**************************************************************************
     * Private/Protected methods of This class:
     **************************************************************************
     */

    /**************************************************************************
     * Public Methods of This class:
     **************************************************************************
     */

    /**
     * Return an empty template (possibly partial) row to be given back to 
     * a client.
     * <p>
     * The main use of this is for fetchSet() and fetchNextGroup() which
     * allocate rows and then give them back entirely to the caller.
     * <p>
     *
	 * @return The row to use.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DataValueDescriptor[] get_row_for_export(Transaction rawtran)
        throws StandardException
    {
        // Create a partial row class template template from the initial scan
        // parameters.
        if (row_for_export_template == null)
        {
            row_for_export_template = 
                RowUtil.newTemplate(
                    rawtran.getDataValueFactory(), 
                    null, format_ids, collation_ids);
        }

        // Allocate a new row based on the class template.
        return(RowUtil.newRowFromTemplate(row_for_export_template));
    }

    /**
     * Return an empty template (possibly partial) row to be used and 
     * reused internally for processing.
     * <p>
     * The main use of this is for qualifying rows where a row has not been
     * provided by the client.  This routine cache's a single row for reuse
     * by the caller, if the caller needs 2 concurrent scratch rows, some other
     * mechanism must be used.
     * <p>
     *
	 * @return The row to use.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DataValueDescriptor[] get_scratch_row(Transaction    rawtran)
        throws StandardException
    {
        // Create a partial row class template template from the initial scan
        // parameters.
        if (scratch_row == null)
        {
            scratch_row = get_row_for_export(rawtran);
        }

        // Allocate a new row based on the class template.
        return(scratch_row);
    }

    /**
     * Return a complete empty row.  
     * <p>
     * The main use of this is for searching a tree where a complete copy of
     * the row is needed for searching.
     * <p>
     *
	 * @return The template to use.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public DataValueDescriptor[] get_template(Transaction rawtran)
        throws StandardException
    {
        // Create a partial row class template from the initial scan parameters.
        if (scratch_template == null)
        {
            scratch_template = 
                TemplateRow.newRow(
                    rawtran, 
                    (FormatableBitSet) null, format_ids, collation_ids);
        }

        return(scratch_template);
    }

    /**
     * Return a scratch RowPosition.
     * <p>
     * Used by GenericConglomerateController.delete() and 
     * GenericConglomerateController.replace().  It may be reused so callers
     * must insure that object no longer needed before next possible call
     * to get it again.
     * <p>
     *
	 * @return a scratch RowPosition.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public RowPosition get_scratch_row_position()
    {
        if (scratch_row_position == null)
        {
            scratch_row_position = new RowPosition();
        }

        return(scratch_row_position);
    }
}
