/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.conn
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.conn;

import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.MessageService;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.error.ExceptionSeverity;

import java.sql.SQLException;

public class ConnectionUtil {

	/**
		Get the current LanguageConnectionContext.
		Used by public api code that needs to ensure it
		is in the context of a SQL connection.

		@exception SQLException Caller is not in the context of a connection.
	*/
	public static LanguageConnectionContext getCurrentLCC()
		throws SQLException {

			LanguageConnectionContext lcc = (LanguageConnectionContext)
				ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);

			if (lcc == null)
				throw new SQLException(
							// No current connection
							MessageService.getTextMessage(
											SQLState.NO_CURRENT_CONNECTION),
							SQLState.NO_CURRENT_CONNECTION,
							ExceptionSeverity.SESSION_SEVERITY);

			return lcc;
	}
}
