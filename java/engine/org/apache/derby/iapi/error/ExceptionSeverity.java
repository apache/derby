/*

   Derby - Class org.apache.derby.iapi.error.ExceptionSeverity

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.iapi.error;

/**
 * Severity constants for SQLExceptions.
 * 
 * These constants are used in the ErrorCode available on a SQLException
 * to provide information about the severity of the error.
 *
 * @see java.sql.SQLException
 */
public interface ExceptionSeverity
{
	/*
	 * Use NO_APPLICABLE_SEVERITY for internal errors and unit
	 * tests that don't need to report or worry about severities.
	 */
	/**
	 * NO_APPLICABLE_SEVERITY occurs only when the system was
	 * unable to determine the severity.
	 */
	public static final int NO_APPLICABLE_SEVERITY = 0;
	/**
	 * WARNING_SEVERITY is associated with SQLWarnings.
	 */
	public static final int WARNING_SEVERITY = 10000;
	/**
	 * STATEMENT_SEVERITY is associated with errors which
	 * cause only the current statement to be aborted.
	 */
	public static final int STATEMENT_SEVERITY = 20000;
	/**
	 * TRANSACTION_SEVERITY is associated with those errors which
	 * cause the current transaction to be aborted.
	 */
	public static final int TRANSACTION_SEVERITY = 30000;
	/**
	 * SESSION_SEVERITY is associated with errors which
	 * cause the current connection to be closed.
	 */
	public static final int SESSION_SEVERITY = 40000;
	/**
	 * DATABASE_SEVERITY is associated with errors which
	 * cause the current database to be closed.
	 */
	public static final int DATABASE_SEVERITY = 45000;
	/**
	 * SYSTEM_SEVERITY is associated with internal errors which
	 * cause the system to shut down.
	 */
	public static final int SYSTEM_SEVERITY = 50000;
}

