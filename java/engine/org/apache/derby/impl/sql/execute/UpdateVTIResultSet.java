/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 2003, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.types.DataValueDescriptor;

import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import java.util.Properties;

/**
 * Update the rows from the source into the specified
 * base table.
 */
public class UpdateVTIResultSet extends DMLVTIResultSet
	/**
		IBM Copyright &copy notice.
	*/

{ private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2003_2004;

	private java.sql.ResultSet		rs;

	private	TemporaryRowHolderImpl	rowHolder;

    /**
	 *
	 * @exception StandardException		Thrown on error
     */
    public UpdateVTIResultSet(NoPutResultSet source, 
						   Activation activation)
		throws StandardException
    {
		super(source, activation);
	}
	
	
	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	protected void openCore() throws StandardException
	{
        int rowLocationColumn = -1;
        boolean firstRow = true;
        
        rs = activation.getTargetVTI();
		row = getNextRowCore(sourceResultSet);

        if( null != row)
            rowLocationColumn = row.nColumns();
		if (!firstExecute)
			lcc.getStatementContext().setTopResultSet(this, subqueryTrackingArray);

		/* The source does not know whether or not we are doing a
		 * deferred mode insert.  If we are, then we must clear the
		 * index scan info from the activation so that the row changer
		 * does not re-use that information (which won't be valid for
		 * a deferred mode insert).
		 */
		if (constants.deferred)
		{
			activation.clearIndexScanInfo();
		}

		if (null == rowHolder && constants.deferred)
		{
			Properties properties = new Properties();

			/*
			** If deferred we save a copy of the entire row.
			*/
			rowHolder = new TemporaryRowHolderImpl(tc, properties, resultDescription);
		}

        try
        {
            while ( row != null )
            {
                if (constants.deferred)
                {
                    // Add the row number to the row.
                    if( firstRow)
                    {
                        row.getColumn( rowLocationColumn).setValue( rs.getRow());
                        firstRow = false;
                    }
                    else
                    {
                        DataValueDescriptor rowLocation = row.cloneColumn( rowLocationColumn);
                        rowLocation.setValue( rs.getRow());
                        row.setColumn( rowLocationColumn, rowLocation);
                    }
                    rowHolder.insert(row);
                }
                else
                    updateVTI( rs);
                rowCount++;

                // No need to do a next on a single row source
                if (constants.singleRowSource)
                {
                    row = null;
                }
                else
                {
                    row = getNextRowCore(sourceResultSet);
                }
            }
        }
        catch (StandardException se)
        {
            throw se;
        }
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}

		/*
		** If it's a deferred update, scan the temporary conglomerate and
		** insert the rows into the VTI using rowChanger.
		*/
		if (constants.deferred)
		{
			CursorResultSet tempRS = rowHolder.getResultSet();
			try
			{
                ExecRow	deferredRowBuffer = null;

				tempRS.open();
				while ((deferredRowBuffer = tempRS.getNextRow()) != null)
				{
					row = deferredRowBuffer;
                    int rowNumber = row.getColumn( rowLocationColumn).getInt();
                    rs.absolute( rowNumber);
					updateVTI(rs);
				}
			}
            catch (Throwable t)
            {
                throw StandardException.unexpectedUserException(t);
            }
            finally
			{
				sourceResultSet.clearCurrentRow();
				tempRS.close();
			}
		}

		if (rowHolder != null)
		{
			rowHolder.close();
			// rowHolder kept across opens
		}
	} // end of openCore

	private void updateVTI(ResultSet target)
		throws StandardException
	{
        int[] changedColumnIds = constants.changedColumnIds;
		try
		{
            for( int i = 0; i < changedColumnIds.length; i++)
            {
                int columnId = changedColumnIds[i];
                DataValueDescriptor newValue = row.getColumn( i + 1);
                if( newValue.isNull())
                    target.updateNull( columnId);
                else
                    newValue.setInto( target, columnId);
            }
            target.updateRow();
        }
		catch (Throwable t)
		{
			throw StandardException.unexpectedUserException(t);
		}
	} // end of updateVTI
}
