/*

   Derby - Class org.apache.derby.iapi.error.SQLWarningFactory

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

package org.apache.derby.iapi.error;

import java.sql.SQLWarning;

import org.apache.derby.iapi.services.i18n.MessageService;


// for javadoc 
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This class generates SQLWarning instances. It has an understanding of Derby's
 * internal error/warning message Ids, and transforms these to localised error
 * messages and appropriate SQLState.
 */
public class SQLWarningFactory {

	/**
	 * Generates a SQLWarning instance based on the supplied messageId and
	 * arguments. It looks up the messageId to generate a localised warning
	 * message. Also, SQLState is set correctly based on the messageId.
	 * 
	 * @param messageId A Derby messageId as defined in {@link SQLState org.apache.derby.shared.common.reference.SQLState}.
	 * @param args Arguments for the warning message
	 * @return Properly initialized SQLWarning instance.
	 * @see org.apache.derby.shared.common.reference.SQLState
	 */
    public static SQLWarning newSQLWarning(String messageId, Object... args)
    {
		return new SQLWarning
            (
             MessageService.getTextMessage( messageId, args ),
             StandardException.getSQLStateFromIdentifier(messageId),
             ExceptionSeverity.WARNING_SEVERITY
             );
	}

}
