/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

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
