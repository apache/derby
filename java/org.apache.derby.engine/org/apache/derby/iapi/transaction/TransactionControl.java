/*

   Derby - Class org.apache.derby.iapi.transaction.TransactionControl

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derby.iapi.transaction;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.derby.shared.common.error.ExceptionSeverity;
import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.util.ArrayUtil;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * Provide support to transactions to manage sets of
 * actions to perform at transaction boundaries.
 *
 * <P> Add rollback of savepoints?
 * TODO: A
 */
public final class TransactionControl {
    
	/* Constants for scan isolation levels. */
	public static final int UNSPECIFIED_ISOLATION_LEVEL = 0;
	public static final int READ_UNCOMMITTED_ISOLATION_LEVEL = 1;
	public static final int READ_COMMITTED_ISOLATION_LEVEL = 2;
	public static final int REPEATABLE_READ_ISOLATION_LEVEL = 3;
	public static final int SERIALIZABLE_ISOLATION_LEVEL = 4;

    /**
     * Map from Derby transaction isolation constants to
     * JDBC constants.
     */
	private static final int[] CS_TO_JDBC_ISOLATION_LEVEL_MAP = {
//IC see: https://issues.apache.org/jira/browse/DERBY-6206
		java.sql.Connection.TRANSACTION_NONE,				// UNSPECIFIED_ISOLATION_LEVEL
		java.sql.Connection.TRANSACTION_READ_UNCOMMITTED,	// READ_UNCOMMITTED_ISOLATION_LEVEL
		java.sql.Connection.TRANSACTION_READ_COMMITTED,		// READ_COMMITTED_ISOLATION_LEVEL
		java.sql.Connection.TRANSACTION_REPEATABLE_READ,	// REPEATABLE_READ_ISOLATION_LEVEL		
		java.sql.Connection.TRANSACTION_SERIALIZABLE		// SERIALIZABLE_ISOLATION_LEVEL
	};

    /**
     * Map from Derby transaction isolation constants to
     * text values used in SQL. Note that the text
     * "REPEATABLE READ" or "RR" maps to SERIALIZABLE_ISOLATION_LEVEL
     * as a hang over from DB2 compatibility and now to preserve
     * backwards compatability.
     */
	private static final String[][] CS_TO_SQL_ISOLATION_MAP = {
		{ "  "},					// UNSPECIFIED_ISOLATION_LEVEL
		{ "UR", "DIRTY READ", "READ UNCOMMITTED"},
		{ "CS", "CURSOR STABILITY", "READ COMMITTED"},
		{ "RS"},		// read stability	
		{ "RR", "REPEATABLE READ", "SERIALIZABLE"}
	};

    private final ArrayList<TransactionListener> listeners;

    /** Map a Derby isolation level to the corresponding JDBC level */
    public  static  int jdbcIsolationLevel( int derbyIsolationLevel )
    {
        return CS_TO_JDBC_ISOLATION_LEVEL_MAP[ derbyIsolationLevel ];
    }

    /** Map Derby isolation level to SQL text values */
    public  static  String[]    isolationTextNames( int derbyIsolationLevel )
    {
        return ArrayUtil.copy( CS_TO_SQL_ISOLATION_MAP[ derbyIsolationLevel ] );
    }

    /** Get number of isolation string mappings */
    public  static  int     isolationMapCount() { return CS_TO_SQL_ISOLATION_MAP.length; }
    
    public TransactionControl()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        listeners = new ArrayList<TransactionListener>();
    }
    
    /**
     * Add a listener to the curent transaction.
     * 
     * A listener may be added multiple times and it will
     * receive multiple callbacks.
     * 
     */
    public void addListener(TransactionListener listener)
    {
        listeners.add(listener);
    }
    
    /**
     * Remove a listener from the current transaction.
     * 
     */
    public void removeListener(TransactionListener listener)
    {
        listeners.remove(listener);
    }
    
    /**
     * Notify all listeners that a commit is about to occur.
     * If a listener throws an exception then no
     * further listeners will be notified and a
     * StandardException with rollback severity will be thrown.
     * @throws StandardException
     */
    public void preCommitNotify() throws StandardException
    {
        if (listeners.isEmpty())
            return;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        for (Iterator<TransactionListener> i = listeners.iterator(); i.hasNext(); )
        {
            TransactionListener listener =
                i.next();
            
            try {
                if(listener.preCommit())
                   i.remove();
            }
            catch (StandardException se) 
            {               
                // This catches any exceptions that have Transaction severity
                // or less (e.g. Statement exception).
                // If we received any lesser
                // error then we abort the transaction anyway.
                
                if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
                    throw StandardException.newException(
                            SQLState.XACT_COMMIT_EXCEPTION, se);
                }
                
                throw se;
                
            }
            
        }
    }
    
    /**
     * Notify all listeners that a rollback is about to occur.
     * If a listener throws an exception then no
     * further listeners will be notified and a
     * StandardException with shutdown database(?) severity will be thrown.
     * @throws StandardException
     */
    public void preRollbackNotify() throws StandardException
    { 
        if (listeners.isEmpty())
            return;
        
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        for (Iterator<TransactionListener> i = listeners.iterator(); i.hasNext(); )
        {
            TransactionListener listener =
                i.next();
            
            try {
                listener.preRollback();
                i.remove();
            } catch (StandardException se) {
                // TODO: Define behaviour on exception during rollback.

                if (se.getSeverity() < ExceptionSeverity.TRANSACTION_SEVERITY)
                {
                    
                }
                throw se;
            }
        }
    }
}
