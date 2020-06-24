/*

   Derby - Class org.apache.derby.shared.common.error.PublicAPI

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
	Class that wraps StandardExceptions in a SQLException.
	This is used to make any public API methods always
	throw SQLException rather than a random collection.
 */
public class PublicAPI
{
	/**
		Generates a SQLException for signalling that the
		operation failed due to a database error.

        @param se The exception to wrap inside a SQLException

        @return a SQLException wrapped around the original exception
	 */
	public static SQLException wrapStandardException(StandardException se) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6488
        se.markAsPublicAPI();
        return ExceptionFactory.getInstance().getSQLException(
                se.getMessage(), se.getMessageId(), (SQLException) null,
                se.getSeverity(), se, se.getArguments());
	}
}
