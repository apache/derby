/*

   Derby - Class org.apache.derby.shared.common.error.ExceptionFactory

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

package org.apache.derby.shared.common.error;

import java.sql.SQLException;

/**
 * An exception factory is used to create SQLExceptions of the correct type.
 */
public abstract class ExceptionFactory {

    /** The singleton ExceptionFactory instance. */
    private static final ExceptionFactory INSTANCE;
    static {
        // Initialize the singleton instance. Use reflection so that there
        // is no compile-time dependency on implementation classes from iapi.
        // Currently, there is only one implementation. There used to be two;
        // one for JDBC 3.0 and lower, and one for JDBC 4.0 and higher. If
        // the need for more than one implementation ever arises again, the
        // code below should be changed to load the correct factory for the
        // run-time platform.
//IC see: https://issues.apache.org/jira/browse/DERBY-6253
        String impl = "org.apache.derby.impl.jdbc.SQLExceptionFactory";
        ExceptionFactory factory = null;
        try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            Class<?> clazz = Class.forName(impl);
            factory = (ExceptionFactory) clazz.getConstructor().newInstance();
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
        INSTANCE = factory;
    }

    /**
     * Get the singleton exception factory instance.
     * @return an {@code ExceptionFactory} instance
     */
    public static ExceptionFactory getInstance() {
        return INSTANCE;
    }

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
    public abstract SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable cause, Object... args);
//IC see: https://issues.apache.org/jira/browse/DERBY-6253

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
    public abstract SQLException getSQLException(String messageId,
//IC see: https://issues.apache.org/jira/browse/DERBY-6253
            SQLException next, Throwable cause, Object... args);
}
