/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import java.io.Serializable;
/**
    A bread for the internalUtil class
*/
public abstract class PropertyUtil extends org.apache.derby.iapi.util.PropertyUtil
{
	public static Serializable getDatabasePropertyDefault(String k) throws Exception
	{
        LanguageConnectionContext lcc =
			(LanguageConnectionContext) ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
		if (lcc == null) throw new Exception("getPropertyDefault only works in a connection");
		return lcc.getTransactionExecute().getPropertyDefault(k);
	}
	public static void setDatabasePropertyDefault(String k,Serializable v) throws Exception
	{
        LanguageConnectionContext lcc =
			(LanguageConnectionContext) ContextService.getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
		if (lcc == null) throw new Exception("getPropertyDefault only works in a connection");
		lcc.getTransactionExecute().setPropertyDefault(k,v);
	}
}
