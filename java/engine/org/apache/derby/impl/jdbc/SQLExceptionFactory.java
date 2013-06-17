/*
 
   Derby - Class org.apache.derby.impl.jdbc.SQLExceptionFactory
 
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
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransactionRollbackException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.jdbc.ExceptionFactory;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.shared.common.reference.SQLState;

/**
 *Class to create SQLException
 *
 */
public class SQLExceptionFactory extends ExceptionFactory {
    /**
     * <p>
     * method to construct SQLException
     * version specific drivers can overload this method to create
     * version specific exceptions
     * </p>
     *
     * <p>
     * This implementation creates JDBC 4 exceptions.
     * </p>
     *
     * <pre>
     * SQLSTATE CLASS (prefix)     Exception
     * 0A                          java.sql.SQLFeatureNotSupportedException
     * 08                          java.sql.SQLNonTransientConnectionException
     * 22                          java.sql.SQLDataException
     * 28                          java.sql.SQLInvalidAuthorizationSpecException
     * 40                          java.sql.SQLTransactionRollbackException
     * 42                          java.sql.SQLSyntaxErrorException
     * </pre>
     */
    @Override
    public SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable t, Object... args) {
        String sqlState = StandardException.getSQLStateFromIdentifier(messageId);

        //
        // Create dummy exception which ferries arguments needed to serialize
        // SQLExceptions across the DRDA network layer.
        //
        t = wrapArgsForTransportAcrossDRDA( message, messageId, next, severity, t, args );

        final SQLException ex;
        if (sqlState.startsWith(SQLState.CONNECTIVITY_PREFIX)) {
            //no derby sqlstate belongs to
            //TransientConnectionException DERBY-3074
            ex = new SQLNonTransientConnectionException(message, sqlState, severity, t);
        } else if (sqlState.startsWith(SQLState.SQL_DATA_PREFIX)) {
            ex = new SQLDataException(message, sqlState, severity, t);
        } else if (sqlState.startsWith(SQLState.INTEGRITY_VIOLATION_PREFIX)) {
            ex = new SQLIntegrityConstraintViolationException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith(SQLState.AUTHORIZATION_SPEC_PREFIX)) {
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
        } else if
                (
                 sqlState.equals(SQLState.LANG_STATEMENT_CANCELLED_OR_TIMED_OUT.substring(0, 5)) ||
                 sqlState.equals(SQLState.LOGIN_TIMEOUT.substring(0, 5))
                 ) {
            ex = new SQLTimeoutException(message, sqlState, severity, t);
        } else {
            ex = new SQLException(message, sqlState, severity, t);
        }

        if (next != null) {
            ex.setNextException(next);
        }
        return ex;
    }

    /**
     * Construct an SQLException whose message and severity are derived from
     * the message id.
     */
    @Override
    public final SQLException getSQLException(String messageId,
            SQLException next, Throwable cause, Object... args) {
        String message = MessageService.getCompleteMessage(messageId, args);
        int severity = StandardException.getSeverityFromIdentifier(messageId);
        return getSQLException(message, messageId, next, severity, cause, args);
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
    private SQLException wrapArgsForTransportAcrossDRDA(
            String message, String messageId, SQLException next,
            int severity, Throwable t, Object[] args) {
        return new EmbedSQLException(
                message, messageId,
                (next == null ?
                    null : StandardException.getArgumentFerry(next)),
                severity, t, args);
    }
}
