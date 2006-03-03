/*
 
   Derby - Class org.apache.derby.impl.jdbc.SQLExceptionFactory40
 
   Copyright 2006 The Apache Software Foundation or its licensors, as applicable.
 
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

package org.apache.derby.impl.jdbc;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import org.apache.derby.iapi.error.StandardException;

/**
 * SQLExceptionFactory40 overwrites getSQLException method
 * to return SQLException or one of its sub class
 */

public class SQLExceptionFactory40 extends SQLExceptionFactory {
    
    /**
     * overwrites super class method to create JDBC4 exceptions      
     * SQLSTATE CLASS (prefix)     Exception
     * 08                          java.sql.SQLTransientConnectionException
     * 22                          java.sql.SQLDataException
     * 28                          java.sql.SQLInvalidAuthorizationSpecException
     * 40                          java.sql.SQLTransactionRollbackException
     * 42                          java.sql.SQLSyntaxErrorException
     * 
     * This method sets the stack trace of the newly created exception to the
     * root cause of the original Throwable.
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
        SQLException ex = new SQLException(message, sqlState, severity, t);
        if (sqlState.startsWith("08")) {
            //none of the sqlstate supported by derby belongs to
            //NonTransientConnectionException
            ex = new SQLTransientConnectionException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith("22")) {
            ex = new SQLDataException(message, sqlState, severity, t);
        } else if (sqlState.startsWith("23")) {
            ex = new SQLIntegrityConstraintViolationException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith("28")) {
            ex = new SQLInvalidAuthorizationSpecException(message, sqlState,
                    severity, t);
        }        
        else if (sqlState.startsWith("40")) {
            ex = new SQLTransactionRollbackException(message, sqlState,
                    severity, t);
        } else if (sqlState.startsWith("42")) {
            ex = new SQLSyntaxErrorException(message, sqlState, severity, t);
        }
        
        if (next != null) {
            ex.setNextException(next);
        }
        if (t != null) {
            ex.setStackTrace (t.getStackTrace ());
        }
        return ex;
    }        
}
