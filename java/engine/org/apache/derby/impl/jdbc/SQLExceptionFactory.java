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

import java.io.IOException;
import org.apache.derby.iapi.error.ExceptionSeverity;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.MessageId;

import java.sql.SQLException;

/**
 *Class to create SQLException
 *
 */
public class SQLExceptionFactory {
    /**
     * method to construct SQLException
     * version specific drivers can overload this method to create
     * version specific exceptions
     */
    public SQLException getSQLException(String message, String messageId,
            SQLException next, int severity, Throwable t, Object[] args) {
        return new EmbedSQLException(message, messageId, next, severity,
                t, args);
    }

	/**
	 * Unpack a SQL exception, looking for an EmbedSQLException which carries
	 * the Derby messageID and args which we will serialize across DRDA so
	 * that the client can reconstitute a SQLException with appropriate text.
	 * If we are running JDBC3 or JDBC2, then we hope that the passed-in
	 * exception is already an EmbedSQLException, which carries all the
	 * information we need.
	 */
	public	SQLException	getArgumentFerry(SQLException se)
	{
		return StandardException.getArgumentFerry(se);
	}

}
