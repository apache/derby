/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.error
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.error;

import java.sql.SQLException;

import org.apache.derby.impl.jdbc.EmbedSQLException;


/**
	Class that wraps StandardExceptions in a SQLException.
	This is used to make any public API methods always
	throw SQLException rather than a random collection.
	This wrapping is also special cased by TypeStatementException
	to avoid double wrapping of some errors.
	<P>
	This will get cleaned up in main.
 */
public class PublicAPI
{
	/**
		Generates a SQLException for signalling that the
		operation failed due to a database error.
	 */
	public static SQLException wrapStandardException(StandardException se) {
		return EmbedSQLException.wrapStandardException(se.getMessage(),
			se.getMessageId(), se.getSeverity(), se);
	}
}
