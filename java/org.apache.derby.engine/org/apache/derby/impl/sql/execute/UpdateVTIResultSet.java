/*

   Derby - Class org.apache.derby.impl.sql.execute.UpdateVTIResultSet

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.shared.common.sanity.SanityManager;

import org.apache.derby.shared.common.error.StandardException;

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
//IC see: https://issues.apache.org/jira/browse/DERBY-467
class UpdateVTIResultSet extends DMLVTIResultSet
{
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
		@exception StandardException Standard Derby error policy
	*/
	protected void openCore() throws StandardException
	{
        int rowLocationColumn = -1;
        boolean firstRow = true;
        
        rs = activation.getTargetVTI();
		ExecRow row = getNextRowCore(sourceResultSet);
//IC see: https://issues.apache.org/jira/browse/DERBY-467

        if( null != row)
            rowLocationColumn = row.nColumns();

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
//IC see: https://issues.apache.org/jira/browse/DERBY-1112
			rowHolder =
//IC see: https://issues.apache.org/jira/browse/DERBY-4610
//IC see: https://issues.apache.org/jira/browse/DERBY-3049
				new TemporaryRowHolderImpl(activation, properties,
										   resultDescription);
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
//IC see: https://issues.apache.org/jira/browse/DERBY-467
                    updateVTI(rs, row);
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
				tempRS.open();
//IC see: https://issues.apache.org/jira/browse/DERBY-467
				while ((row = tempRS.getNextRow()) != null)
				{
                    int rowNumber = row.getColumn( rowLocationColumn).getInt();
                    rs.absolute( rowNumber);
					updateVTI(rs, row);
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

    @Override
    public void close() throws StandardException
    {
        close( false );
    }

	private void updateVTI(ResultSet target, ExecRow row)
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
