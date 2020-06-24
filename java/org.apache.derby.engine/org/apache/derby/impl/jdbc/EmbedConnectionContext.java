/*

   Derby - Class org.apache.derby.impl.jdbc.EmbedConnectionContext

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

//depot/main/java/org.apache.derby.impl.jdbc/EmbedConnectionContext.java#24 - edit change 16899 (text)
package org.apache.derby.impl.jdbc;

// This is the recommended super-class for all contexts.
import org.apache.derby.iapi.services.context.ContextImpl;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.jdbc.ConnectionContext;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;

import org.apache.derby.shared.common.error.ExceptionSeverity;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.SQLException;
import java.util.Vector;
import java.util.Enumeration;
/**
 */
//IC see: https://issues.apache.org/jira/browse/DERBY-467
class EmbedConnectionContext extends ContextImpl 
		implements ConnectionContext
{

	/**
		We hold a soft reference to the connection so that when the application
		releases its reference to the Connection without closing it, its finalize
		method will be called, which will then close the connection. If a direct
		reference is used here, such a Connection will never be closed or garbage
		collected as modules hold onto the ContextManager and thus there would
		be a direct reference through this object.
	*/
	private java.lang.ref.SoftReference<EmbedConnection>	connRef;


	EmbedConnectionContext(ContextManager cm, EmbedConnection conn) {
		super(cm, ConnectionContext.CONTEXT_ID);

//IC see: https://issues.apache.org/jira/browse/DERBY-6213
		connRef = new java.lang.ref.SoftReference<EmbedConnection>(conn);
	}

	public void cleanupOnError(Throwable error) {

		if (connRef == null)
			return;

		EmbedConnection conn = connRef.get();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		if (error instanceof StandardException) {

			StandardException se = (StandardException) error;
			if (se.getSeverity() < ExceptionSeverity.SESSION_SEVERITY) {

				// any error in auto commit mode that does not close the
				// session will cause a rollback, thus remvoing the need
				// for any commit. We could check this flag under autoCommit
				// being true but the flag is ignored when autoCommit is false
				// so why add the extra check
				if (conn != null) {
					conn.needCommit = false;
				}
				return;
			}
		}

		// This may be a transaction without connection.
		if (conn != null)
			conn.setInactive(); // make the connection inactive & empty

		connRef = null;
		popMe();
	}

	//public java.sql.Connection getEmbedConnection()
	//{
	///	return conn;
	//}

	/**
		Get a connection equivalent to the call
		<PRE>
		DriverManager.getConnection("jdbc:default:connection");
		</PRE>
	*/
	public java.sql.Connection getNestedConnection(boolean internal) throws SQLException {

		EmbedConnection conn = connRef.get();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		if ((conn == null) || conn.isClosed())
			throw Util.noCurrentConnection();

		if (!internal) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
			StatementContext sc = privilegedGetLCC( conn ).getStatementContext();
			if ((sc == null) || (sc.getSQLAllowed() < org.apache.derby.catalog.types.RoutineAliasInfo.MODIFIES_SQL_DATA))
				throw Util.noCurrentConnection();
		}

		return conn.getLocalDriver().getNewNestedConnection(conn);
	}

	/**
	 * Get a jdbc ResultSet based on the execution ResultSet.
	 *
	 * @param executionResultSet	a result set as gotten from execution
	 *	
	 */	
	public java.sql.ResultSet getResultSet
	(
		ResultSet 				executionResultSet
//IC see: https://issues.apache.org/jira/browse/DERBY-98
	) throws SQLException
	{
		EmbedConnection conn = connRef.get();
//IC see: https://issues.apache.org/jira/browse/DERBY-6213

		EmbedResultSet rs = conn.getLocalDriver().newEmbedResultSet(conn, executionResultSet, 
							false, (EmbedStatement) null, true);
		return rs;
	}

    /**
     * Process a ResultSet from a procedure to be a dynamic result,
     * but one that will be closed due to it being inaccessible. We cannot simply
     * close the ResultSet as it the nested connection that created
     * it might be closed, leading to its close method being a no-op.
     * This performs all the conversion (linking the ResultSet
     * to a valid connection) required but does not close
     * the ResultSet.
     * 
     *   @see EmbedStatement#processDynamicResult(EmbedConnection, java.sql.ResultSet, EmbedStatement)
     */
    public boolean processInaccessibleDynamicResult(java.sql.ResultSet resultSet) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        EmbedConnection conn = connRef.get();
//IC see: https://issues.apache.org/jira/browse/DERBY-1585
        if (conn == null)
            return false;
        
        // Pass in null as the Statement to own the ResultSet since
        // we don't have one since the dynamic result will be inaccessible.
        return EmbedStatement.processDynamicResult(conn, resultSet, null) != null;
    }
    
    /**
     * Private, privileged lookup of the lcc..
     */
    private LanguageConnectionContext privilegedGetLCC( final EmbedConnection conn )
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6751
        return AccessController.doPrivileged
            (
             new PrivilegedAction<LanguageConnectionContext>()
             {
                 public LanguageConnectionContext run()
                 {
                     return conn.getLanguageConnection();
                 }
             }
             );
    }
}
