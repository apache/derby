/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.impl.sql.execute
   (C) Copyright IBM Corp. 1997, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.impl.sql.execute;

import org.apache.derby.iapi.sql.execute.ExecutionContext;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.ResultSetFactory;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;

import org.apache.derby.iapi.sql.ResultSet;

import org.apache.derby.iapi.services.sanity.SanityManager;

import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;

import org.apache.derby.iapi.services.monitor.Monitor;

import org.apache.derby.iapi.error.StandardException;

import java.util.Properties;
import org.apache.derby.iapi.error.ExceptionSeverity;
/**
 * ExecutionContext stores the result set factory to be used by
 * the current connection, and manages execution-level connection
 * activities.
 * <p>
 * An execution context is expected to be on the stack for the
 * duration of the connection.
 *
 * @author ames
 */
public class GenericExecutionContext
	extends ContextImpl 
	implements ExecutionContext {
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1997_2004;

	private ResultSet sourceRS;

	//
	// class implementation
	//
	protected ResultSetFactory rsFactory;
	protected ResultSetStatisticsFactory rssFactory;
	protected ExecutionFactory execFactory;

	//
	// ExecutionContext interface
	//
	/**
	 * Get the ResultSetFactory from this ExecutionContext.
	 *
	 * @return	The result set factory associated with this
	 *		ExecutionContext
	 */
	public ResultSetFactory getResultSetFactory() 
	{
		/* null rsFactory may have been passed to
		 * constructor in order to speed up boot time.
		 */
		if (rsFactory == null)
		{
			rsFactory = execFactory.getResultSetFactory();
		}
		return rsFactory;
	}

	/**
	 * Get the ResultSetStatisticsFactory from this ExecutionContext.
	 *
	 * @return	The result set statistics factory associated with this
	 *		ExecutionContext
	 *
	 * @exception StandardException		Thrown on error
	 */
	public ResultSetStatisticsFactory getResultSetStatisticsFactory()
					throws StandardException {
		if (rssFactory == null) {
			rssFactory = (ResultSetStatisticsFactory)
				Monitor.bootServiceModule(
									false,
									execFactory,
									ResultSetStatisticsFactory.MODULE,
									(Properties) null);
		}

		return rssFactory;
	}

	public ExecutionFactory getExecutionFactory() {
		return execFactory;
	}

	/**
	 * @see ExecutionContext#beginStatement
	 * @exception StandardException Thrown on error
	 */
	public void beginStatement(ResultSet sourceRS) throws StandardException {
		this.sourceRS = sourceRS;
	}

	/**
	 * @see ExecutionContext#endStatement
	 * @exception StandardException Thrown on error
	 */
	public void endStatement() throws StandardException {
		sourceRS = null;
	}

	/**
	 * @see ExecutionContext#siftForeignKeys
	 * @exception StandardException Thrown on error
	 */
	public	Object[]	siftForeignKeys( Object[] fullList ) throws StandardException
	{
		// for the Core Language, this routine is a NOP. The interesting
		// cases occur during REFRESH and the initial boot of a Target
		// database. See RepExecutionContext for the interesting cases.

		return	fullList;
	}

	/**
	 * @see ExecutionContext#siftTriggers
	 * @exception StandardException Thrown on error
	 */
	public Object siftTriggers(Object triggerInfo) throws StandardException
	{
		// for the Core Language, this routine is a NOP. The interesting
		// cases occur during REFRESH and the initial boot of a Target
		// database. See RepExecutionContext for the interesting cases.
		return	triggerInfo;
	}

	//
	// Context interface
	//

	/**
	 * @exception StandardException Thrown on error
	 */
	public void cleanupOnError(Throwable error) throws StandardException {
		if (error instanceof StandardException) {

			StandardException se = (StandardException) error;
			if (se.getSeverity() > ExceptionSeverity.STATEMENT_SEVERITY)
				return;


			if (sourceRS != null)
			{
				sourceRS.close();
				sourceRS = null;
			}

			endStatement();
			return;
		}
	}

	//
	// class interface
	//
	public	GenericExecutionContext(
		    ResultSetFactory rsf,
			ContextManager cm,
			ExecutionFactory ef)
	{

		super(cm, ExecutionContext.CONTEXT_ID);
		rsFactory = rsf;
		execFactory = ef;
	}

}
