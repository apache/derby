/*

   Derby - Class org.apache.derby.iapi.jdbc.ExceptionFactory

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

package org.apache.derby.iapi.jdbc;

import java.sql.SQLException;

/**
 * An exception factory is used to create SQLExceptions of the correct type.
 */
public interface ExceptionFactory {

    /**
     * Unpack a SQL exception, looking for an EmbedSQLException which carries
     * the Derby messageID and args which we will serialize across DRDA so
     * that the client can reconstitute a SQLException with appropriate text.
     * If we are running JDBC 3, then we hope that the passed-in
     * exception is already an EmbedSQLException, which carries all the
     * information we need.
     *
     * @param se the exception to unpack
     * @return the argument ferry for the exception
     */
    SQLException getArgumentFerry(SQLException se);

    /**
     * Construct an SQLException whose message and severity are specified
     * explicitly.
     *
     * @param message the exception message
     * @param messageId the message id
     * @param next the next SQLException
     * @param severity the severity of the exception
     * @param cause the cause of the exception
     * @param args the message arguments
     * @return an SQLException
     */
    SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable cause, Object[] args);

    /**
     * Construct an SQLException whose message and severity are derived from
     * the message id.
     *
     * @param messageId the message id
     * @param next the next SQLException
     * @param cause the cause of the exception
     * @param args the message arguments
     * @return an SQLException
     */
    SQLException getSQLException(String messageId, SQLException next,
            Throwable cause, Object[] args);

}
