/*

   Derby - Class org.apache.derby.client.am.SQLExceptionFactory40

   Copyright (c) 2006 The Apache Software Foundation or its licensors, where applicable.

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
package org.apache.derby.client.am;

import org.apache.derby.shared.common.reference.SQLState;

import java.sql.SQLDataException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;

/**
 * SQLException factory class to create jdbc 40 exception classes
 */

public class SQLExceptionFactory40 extends SQLExceptionFactory {
    
    /**
     * creates jdbc4.0 SQLException and its subclass based on sql state
     * 0A                          java.sql.SQLFeatureNotSupportedException
     * 08                          java.sql.SQLTransientConnectionException
     * 22                          java.sql.SQLDataException
     * 28                          java.sql.SQLInvalidAuthorizationSpecException
     * 40                          java.sql.SQLTransactionRollbackException
     * 42                          java.sql.SQLSyntaxErrorException
     * 
     * @param message description of the 
     * @param sqlState 
     * @param errCode derby error code
     */
    public SQLException getSQLException (String message, String sqlState, 
                                                            int errCode) {        
        SQLException ex = null;
        if (sqlState == null) {
            ex = new SQLException(message, sqlState, errCode); 
        } else if (sqlState.startsWith(SQLState.CONNECTIVITY_PREFIX)) {
            //none of the sqlstate supported by derby belongs to
            //NonTransientConnectionException
            ex = new SQLTransientConnectionException(message, sqlState, errCode);
        } else if (sqlState.startsWith(SQLState.SQL_DATA_PREFIX)) {
            ex = new SQLDataException(message, sqlState, errCode);
        } else if (sqlState.startsWith(SQLState.INTEGRITY_VIOLATION_PREFIX)) {
            ex = new SQLIntegrityConstraintViolationException(message, sqlState,
                    errCode);
        } else if (sqlState.startsWith(SQLState.AUTHORIZATION_PREFIX)) {
            ex = new SQLInvalidAuthorizationSpecException(message, sqlState,
                    errCode);
        } else if (sqlState.startsWith(SQLState.TRANSACTION_PREFIX)) {
            ex = new SQLTransactionRollbackException(message, sqlState,
                    errCode);
        } else if (sqlState.startsWith(SQLState.LSE_COMPILATION_PREFIX)) {
            ex = new SQLSyntaxErrorException(message, sqlState, errCode);
        } else if (sqlState.startsWith (SQLState.UNSUPPORTED_PREFIX)) {
            ex = new SQLFeatureNotSupportedException(message, sqlState, 
                    errCode);
        } else {
            ex = new SQLException(message, sqlState, errCode); 
        }
        return ex;
    }
}
