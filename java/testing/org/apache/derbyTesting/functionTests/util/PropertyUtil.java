/*

   Derby - Class org.apache.derbyTesting.functionTests.util.PropertyUtil

   Copyright 1999, 2004 The Apache Software Foundation or its licensors, as applicable.

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
