/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql
   (C) Copyright IBM Corp. 2000, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;

/**
 * Utilities for dealing with statements.
 *
 * @author jeff
 */
public class StatementUtil
{
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_2000_2004;
	private StatementUtil(){};	// Do not instantiate

	public static String typeName(int typeNumber)
	{
		String retval;

		switch (typeNumber)
		{
		  case StatementType.INSERT:
		  case StatementType.BULK_INSERT_REPLACE:
		  case StatementType.UPDATE:
		  case StatementType.DELETE:
		  case StatementType.ENABLED:
		  case StatementType.DISABLED:
			retval = TypeNames[typeNumber];
			break;

		  default:
			retval = MessageService.getTextMessage(SQLState.LANG_UNKNOWN);
			break;
		}

		return retval;
	}

	private static final String[] TypeNames = 
				{ 
					"",
					"INSERT",
					"INSERT",
					"UPDATE",
					"DELETE",
					"ENABLED",
					"DISABLED"
				};
}
