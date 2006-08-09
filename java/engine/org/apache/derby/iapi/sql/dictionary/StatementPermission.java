/*

   Derby - Class org.apache.derby.iapi.sql.dictionary.StatementPermission

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

package org.apache.derby.iapi.sql.dictionary;

import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.error.StandardException;

/**
 * This class describes a permission require by a statement.
 */

public abstract class StatementPermission
{
	/**
	 * Restrict implementations to this package to reduce
	 * risk of external code spoofing the GRANT/REVOKE system
	 * by providing its own fake implementations.
	 *
	 */
	StatementPermission()
	{
	}
	/**
	 * @param lcc				LanguageConnectionContext
	 * @param authorizationId	AuthorizationId
	 * @param forGrant
	 *
	 * @exception StandardException if the permission has not been granted
	 */
	public abstract void check( LanguageConnectionContext lcc,
								String authorizationId,
								boolean forGrant) throws StandardException;

	/**
	 * 
	 * Get the PermissionDescriptor for the passed authorization id for this
	 * object. This method gets called during the execution phase of create 
	 * view/constraint/trigger. The return value of this method is saved in
	 * dependency system to keep track of views/constraints/triggers 
	 * dependencies on required permissions. This happens in execution phase 
	 * after it has been established that passed authorization id has all the 
	 * permissions it needs to create that view/constraint/trigger. Which means 
	 * that we can only get to writing into dependency system once all the required 
	 * privileges are confirmed. 
	 *   
	 * @param authid	AuthorizationId
	 * @param dd	DataDictionary
	 * 
	 * @return PermissionsDescriptor	The PermissionDescriptor for the passed
	 *  authorization id on this object
	 * 
	 * @exception StandardException
	 */
	public abstract PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException;
}
