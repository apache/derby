/*

   Derby - Class org.apache.derbyTesting.functionTests.util.PropertyUtil

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.apache.derbyTesting.functionTests.util;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derby.iapi.services.context.Context;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-6648
			(LanguageConnectionContext) getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
		if (lcc == null) throw new Exception("getPropertyDefault only works in a connection");
		return lcc.getTransactionExecute().getPropertyDefault(k);
	}
	public static void setDatabasePropertyDefault(String k,Serializable v) throws Exception
	{
        LanguageConnectionContext lcc =
			(LanguageConnectionContext) getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
		if (lcc == null) throw new Exception("getPropertyDefault only works in a connection");
		lcc.getTransactionExecute().setPropertyDefault(k,v);
	}
    
    /**
     * Privileged lookup of a Context. Must be private so that user code
     * can't call this entry point.
     */
    private  static  Context    getContextOrNull( final String contextID )
    {
        if ( System.getSecurityManager() == null )
        {
            return ContextService.getContextOrNull( contextID );
        }
        else
        {
            return AccessController.doPrivileged
                (
                 new PrivilegedAction<Context>()
                 {
                     public Context run()
                     {
                         return ContextService.getContextOrNull( contextID );
                     }
                 }
                 );
        }
    }

}
