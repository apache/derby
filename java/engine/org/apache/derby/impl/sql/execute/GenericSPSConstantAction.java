/*

   Derby - Class org.apache.derby.impl.sql.execute.GenericSPSConstantAction

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

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.services.context.ContextService;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.reference.SQLState;

import org.apache.derby.iapi.sql.execute.ExecRow;

import org.apache.derby.iapi.store.access.TransactionController;

import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.PreparedStatement;

/**
 * Utility class for Stored prepared statement constant
 * actions.
 *
 * @author jamie
 */
public abstract class GenericSPSConstantAction extends DDLConstantAction
{
	/**
	 * Public niladic constructor. 
	 */
	public GenericSPSConstantAction() {}


	protected Object[] getUsingResults(String usingText) 
		throws StandardException
	{
		/*
		** Get the results the easy way: create
		** a statement from the text and execute
		** it.
		*/
		LanguageConnectionContext lcc = (LanguageConnectionContext)
			ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);

		PreparedStatement ps = lcc.prepareInternalStatement(usingText);

		ResultSet rs = ps.execute(lcc, false);
		ExecRow row = null;
		row = rs.getNextRow();
		if (row == null)
		{
			throw StandardException.newException(SQLState.LANG_NO_ROWS_FROM_USING_DURING_EXECUTION);
		}

		Object[] rowArray = row.getRowArray();

		/*
		** If there are any other rows, then throw an
		** exception
		*/
		if (rs.getNextRow() != null)
		{
			StandardException se = StandardException.newException(SQLState.LANG_USING_CARDINALITY_VIOLATION_DURING_EXECUTION);
			throw se;
		}

		return rowArray;
	}
}
