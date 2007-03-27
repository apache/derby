/*
 
   Derby - Class org.apache.derby.impl.jdbc.SQLExceptionFactory40
 
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

package org.apache.derby.impl.jdbc;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLFeatureNotSupportedException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * SQLExceptionFactory40 overwrites getSQLException method
 * to return SQLException or one of its sub class
 */

public class SQLExceptionFactory40 extends SQLExceptionFactory {
    
    /**
     * overwrites super class method to create JDBC4 exceptions      
     * SQLSTATE CLASS (prefix)     Exception
     * 0A                          java.sql.SQLFeatureNotSupportedException
     * 08                          java.sql.SQLTransientConnectionException
     * 22                          java.sql.SQLDataException
     * 28                          java.sql.SQLInvalidAuthorizationSpecException
     * 40                          java.sql.SQLTransactionRollbackException
     * 42                          java.sql.SQLSyntaxErrorException
     * 
     * Note the following divergence from JDBC3 behavior: When running
     * a JDBC3 client, we return EmbedSQLException. That exception class
     * overrides Throwable.toString() and strips off the Throwable's class name.
     * In contrast, the following JDBC4 implementation returns
     * subclasses of java.sql.Exception. These subclasses inherit the behavior 
     * of Throwable.toString(). That is, their toString() output includes
     * their class name. This will break code which relies on the
     * stripping behavior of EmbedSQLSxception.toString(). 
     */
    
    public SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable t, Object[] args) {
        String sqlState = StandardException.getSQLStateFromIdentifier(messageId);

		//
		// Create dummy exception which ferries arguments needed to serialize
		// SQLExceptions across the DRDA network layer.
		//
		t = wrapArgsForTransportAcrossDRDA( message, messageId, next, severity, t, args );

        final SQLException ex;
        if (sqlState.startsWith(SQLState.CONNECTIVITY_PREFIX)) {
            //none of the sqlstate supported by derby belongs to
            //NonTransientConnectionException
            ex = new SQLTransientConnectionException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.SQL_DATA_PREFIX)) {
            ex = new SQLDataException(message, sqlState, severity, t);
        } else if (sqlState.startsWith(SQLState.INTEGRITY_VIOLATION_PREFIX)) {
            ex = new SQLIntegrityConstraintViolationException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.AUTHORIZATION_PREFIX)) {
            ex = new SQLInvalidAuthorizationSpecException(message, sqlState,
                    severity, t);
        }        
        else if (sqlState.startsWith(SQLState.TRANSACTION_PREFIX)) {
            ex = new SQLTransactionRollbackException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.LSE_COMPILATION_PREFIX)) {
            ex = new SQLSyntaxErrorException(message, sqlState, severity, t);
        } else if (sqlState.startsWith(SQLState.UNSUPPORTED_PREFIX)) {
            ex = new SQLFeatureNotSupportedException(message, sqlState, severity, t);
        } else {
            ex = new SQLException(message, sqlState, severity, t);
        }
        
        if (next != null) {
            ex.setNextException(next);
        }
        return ex;
    }        

	/**
	 * Unpack the exception, looking for an EmbedSQLException which carries
	 * the Derby messageID and args which we will serialize across DRDA so
	 * that the client can reconstitute a SQLException with appropriate text.
	 * If we are running JDBC4, then the
	 * passed-in exception will hopefully wrap an informative EmbedSQLException.
	 * See wrapArgsForTransportAcrossDRDA() below.
	 */
	public	SQLException	getArgumentFerry(SQLException se)
	{
		Throwable	cause = se.getCause();

		if ( (cause == null) || !(cause instanceof EmbedSQLException ))	{ return se; }
		else	{ return (SQLException) cause; }
	}

	/**
	 * <p>
	 * The following method helps handle DERBY-1178. The problem is that we may
	 * need to serialize our final SQLException across the DRDA network layer.
	 * That serialization involves some clever encoding of the Derby messageID and
	 * arguments. Unfortunately, once we create one of the
	 * JDBC4-specific subclasses of SQLException, we lose the messageID and
	 * args. This method creates a dummy EmbedSQLException which preserves that
	 * information. We return the dummy exception.
	 * </p>
	 */
	private	SQLException	wrapArgsForTransportAcrossDRDA
	( String message, String messageId, SQLException next, int severity, Throwable t, Object[] args )
	{
        // Generate an EmbedSQLException
        SQLException e =
            super.getSQLException(message, messageId, next, severity, t, args);
        return e;
	}
	
}
