/*

   Derby - Class org.apache.derby.impl.sql.execute.DeleteVTIResultSet

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.types.SQLInteger;

import java.util.Properties;

/**
 * Delete the rows from the specified
 * base table. This will cause constraints to be checked
 * and triggers to be executed based on the c's and t's
 * compiled into the insert plan.
 */
public class DeleteVTIResultSet extends DMLVTIResultSet
{

	private java.sql.ResultSet		rs;
    private TemporaryRowHolderImpl rowHolder;
    /* If the delete is deferred use a row holder to keep the list of IDs of the rows to be deleted.
     * A RowHolder is used instead of a simple list because a RowHolder will spill to disk when it becomes
     * too large. The row will consist of just one column -- an integer.
     */

    /*
     * class interface
     *
     */
    /**
     *
	 * @exception StandardException		Thrown on error
     */
    public DeleteVTIResultSet
	(
		NoPutResultSet		source,
		Activation			activation
	)
		throws StandardException
    {
		super(source, activation);
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	protected void openCore() throws StandardException
	{
		lcc.getStatementContext().setTopResultSet(this, subqueryTrackingArray);

		row = getNextRowCore(sourceResultSet);

		if (row != null)
		{
			rs = activation.getTargetVTI();

			if (SanityManager.DEBUG)
			{
				SanityManager.ASSERT(rs != null,
					"rs expected to be non-null");
			}
		}


		/* The source does not know whether or not we are doing a
		 * deferred mode delete.  If we are, then we must clear the
		 * index scan info from the activation so that the row changer
		 * does not re-use that information (which won't be valid for
		 * a deferred mode delete).
		 */
		if (constants.deferred)
		{
			activation.clearIndexScanInfo();
            if( null == rowHolder)
                rowHolder = new TemporaryRowHolderImpl( tc, new Properties(), (ResultDescription) null);
		}

        try
        {
            while ( row != null )
            {
                if( !constants.deferred)
                    rs.deleteRow();
                else
                {
                    ExecRow rowId = new ValueRow(1);
                    rowId.setColumn( 1, new SQLInteger( rs.getRow()));
                    rowHolder.insert( rowId);
                }

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

		if (constants.deferred)
		{
			CursorResultSet tempRS = rowHolder.getResultSet();
			try
			{
                ExecRow	deferredRowBuffer = null;

				tempRS.open();
				while ((deferredRowBuffer = tempRS.getNextRow()) != null)
				{
                    int rowNumber = deferredRowBuffer.getColumn( 1).getInt();
                    rs.absolute( rowNumber);
					rs.deleteRow();
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
}
