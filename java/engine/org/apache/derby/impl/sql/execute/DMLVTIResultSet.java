/*

   Derby - Class org.apache.derby.impl.sql.execute.DMLVTIResultSet

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;

import org.apache.derby.iapi.store.access.TransactionController;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Base class for Insert, Delete & UpdateVTIResultSet
 */
abstract class DMLVTIResultSet extends DMLWriteResultSet
{

	// passed in at construction time

	protected NoPutResultSet sourceResultSet;
	public  NoPutResultSet savedSource;
	public	UpdatableVTIConstantAction	constants;
	public	TransactionController 	tc;
	public	LanguageConnectionContext			lcc;

    public	ResultDescription 		resultDescription;
	private int						numOpens;
	protected boolean				firstExecute;

	public	ExecRow					row;

	/**
     * Returns the description of the inserted rows.
     * REVISIT: Do we want this to return NULL instead?
	 */
	public ResultDescription getResultDescription()
	{
	    return resultDescription;
	}

    /**
	 *
	 * @exception StandardException		Thrown on error
     */
    public DMLVTIResultSet(NoPutResultSet source, 
						   Activation activation)
		throws StandardException
    {
		super(activation);
		sourceResultSet = source;
		constants = (UpdatableVTIConstantAction) constantAction;

		lcc = activation.getLanguageConnectionContext();
        tc = activation.getTransactionController();

        resultDescription = sourceResultSet.getResultDescription();
	}
	
	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void open() throws StandardException
	{
		// Remember if this is the 1st execution
		firstExecute = (numOpens == 0);

		rowCount = 0;

		if (numOpens++ == 0)
		{
			sourceResultSet.openCore();
		}
		else
		{
			sourceResultSet.reopenCore();
		}

        openCore();

        row = null;
        
		/* Cache query plan text for source, before it gets blown away */
		if (lcc.getRunTimeStatisticsMode())
		{
			/* savedSource nulled after run time statistics generation */
			savedSource = sourceResultSet;
		}

		cleanUp();

		endTime = getCurrentTimeMillis();
	} // end of open()

    protected abstract void openCore() throws StandardException;

	/**
	 * @see org.apache.derby.iapi.sql.ResultSet#cleanUp
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void	cleanUp() throws StandardException
	{
		/* Close down the source ResultSet tree */
        if( null != sourceResultSet)
            sourceResultSet.close();
		numOpens = 0;
		super.close();
	} // end of cleanUp

	public void finish() throws StandardException
    {

		sourceResultSet.finish();
		super.finish();
	} // end of finish
}
