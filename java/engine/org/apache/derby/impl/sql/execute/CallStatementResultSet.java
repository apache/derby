/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.sql.execute.ExecutionContext;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.iapi.services.loader.GeneratedMethod;

import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.store.access.ConglomerateController;

import org.apache.derby.iapi.services.sanity.SanityManager;

/**
 * Call the specified expression, ignoring the return, if any.
 *
 * @author jerry
 */
class CallStatementResultSet extends NoRowsResultSetImpl
{

	private final GeneratedMethod methodCall;

    /*
     * class interface
     *
     */
    CallStatementResultSet(
				GeneratedMethod methodCall,
				Activation a) 
			throws StandardException
    {
		super(a);
		this.methodCall = methodCall;
	}

	/**
		@exception StandardException Standard Cloudscape error policy
	*/
	public void open() throws StandardException
	{
		methodCall.invoke(activation);
		close();
    }

	/**
	 * @see ResultSet#cleanUp
	 */
	public void	cleanUp() 
	{
		/* Nothing to do */
	}
}
