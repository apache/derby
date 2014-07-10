/*

   Derby - Class org.apache.derby.iapi.db.PropertyInfo

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

package org.apache.derby.iapi.db;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;

import org.apache.derby.iapi.security.Securable;
import org.apache.derby.iapi.security.SecurityUtil;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.Authorizer;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;

import org.apache.derby.iapi.store.access.TransactionController;
import java.sql.SQLException;

/**
  *	PropertyInfo is a class with static methods that  set  properties associated with a database.
  * 
  * 
  * <P>
  This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
  <p>This class can be accessed using the class alias <code> PROPERTYINFO </code> in SQL-J statements.
  */

public final class PropertyInfo
{


	/**
		Set or delete the value of a property of the database on the current connection.
        For security reasons (see DERBY-6616), this code is duplicated in SystemProcedures.

		@param key the property key
		@param value the new value, if null the property is deleted.

		@exception SQLException on error
	*/
	
	public static void setDatabaseProperty(String key, String value) throws SQLException
	{
		LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();

		try {
            SecurityUtil.authorize( Securable.SET_DATABASE_PROPERTY );
            
		Authorizer a = lcc.getAuthorizer();
		a.authorize((Activation) null, Authorizer.PROPERTY_WRITE_OP);

        // Get the current transaction controller
        TransactionController tc = lcc.getTransactionExecute();

		tc.setProperty(key, value, false);
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	
    private	PropertyInfo() {}
   
}

