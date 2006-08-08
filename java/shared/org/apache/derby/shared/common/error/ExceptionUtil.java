/*
   Derby - Class org.apache.derby.common.error.ExceptionUtil
 
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

import org.apache.derby.shared.common.error.ExceptionSeverity;

/**
 * This class provides utility routines for exceptions 
 */
public class ExceptionUtil
{
   	/**
	 *  Convert a message identifer from 
     *  org.apache.derby.shared.common.reference.SQLState to
	 *  a SQLState five character string.
     *
	 *	@param messageID - the sql state id of the message from cloudscape
	 *	@return String 	 - the 5 character code of the SQLState ID to returned to the user 
	*/
	public static String getSQLStateFromIdentifier(String messageID) {

		if (messageID.length() == 5)
			return messageID;
		return messageID.substring(0, 5);
	}
    
   	/**
	* Get the severity given a message identifier from SQLState.
	*/
	public static int getSeverityFromIdentifier(String messageID) {

		int lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;

		switch (messageID.length()) {
		case 5:
			switch (messageID.charAt(0)) {
			case '0':
				switch (messageID.charAt(1)) {
				case '1':
					lseverity = ExceptionSeverity.WARNING_SEVERITY;
					break;
				case 'A':
				case '7':
					lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
					break;
				case '8':
					lseverity = ExceptionSeverity.SESSION_SEVERITY;
					break;
				}
				break;	
			case '2':
			case '3':
				lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
				break;
			case '4':
				switch (messageID.charAt(1)) {
				case '0':
					lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
					break;
				case '2':
					lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
					break;
				}
				break;	
			}
			break;

		default:
			switch (messageID.charAt(6)) {
			case 'M':
				lseverity = ExceptionSeverity.SYSTEM_SEVERITY;
				break;
			case 'D':
				lseverity = ExceptionSeverity.DATABASE_SEVERITY;
				break;
			case 'C':
				lseverity = ExceptionSeverity.SESSION_SEVERITY;
				break;
			case 'T':
				lseverity = ExceptionSeverity.TRANSACTION_SEVERITY;
				break;
			case 'S':
				lseverity = ExceptionSeverity.STATEMENT_SEVERITY;
				break;
			case 'U':
				lseverity = ExceptionSeverity.NO_APPLICABLE_SEVERITY;
				break;
			}
			break;
		}

		return lseverity;
	}

}
