/*

   Derby - Class org.apache.derby.diag.DiagUtil

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

package org.apache.derby.diag;

import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.derby.shared.common.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.iapi.services.context.Context;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;

/**
 * Utility methods for the package of diagnostic vtis.
 */
abstract    class   DiagUtil
{
    /**
     * Raise an exception if we are running with SQL authorization turned on
     * but the current user isn't the database owner. This method is used
     * to restrict access to VTIs which disclose sensitive information.
     * See DERBY-5395.
     */
    static void    checkAccess()   throws StandardException
    {
        LanguageConnectionContext lcc = (LanguageConnectionContext)
            getContextOrNull(LanguageConnectionContext.CONTEXT_ID);
        DataDictionary  dd = lcc.getDataDictionary();

        if ( dd.usesSqlAuthorization() )
        {
            String  databaseOwner = dd.getAuthorizationDatabaseOwner();
            String  currentUser = lcc.getStatementContext().getSQLSessionContext().getCurrentUser();

            if ( !databaseOwner.equals( currentUser ) )
            {
                throw StandardException.newException( SQLState.DBO_ONLY );
            }
        }
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


