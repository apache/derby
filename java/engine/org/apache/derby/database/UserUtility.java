/*

   Derby - Class org.apache.derby.database.UserUtility

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

package org.apache.derby.database;
import org.apache.derby.iapi.db.PropertyInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.util.IdUtil;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.ConnectionUtil;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.reference.Property;

import java.sql.SQLException;
import org.apache.derby.iapi.error.PublicAPI;

/**
  This utility class provides static methods for managing user authorization in a Cloudscape database.
  
   <p>This class can only be used within an SQL-J statement, a Java procedure or a server side Java method.
   <p>This class can be accessed using the class alias <code> USERUTILITY </code> in SQL-J statements.
  <P>
  <i>
  IBM Corp. reserves the right 
  to change, rename, or remove this interface
  at any time. </i>
  */
public abstract class UserUtility
{
	/** Enumeration value for read access permission ("READ_ACCESS_PERMISSION"). */
	public final static String READ_ACCESS_PERMISSION = "READ_ACCESS_PERMISSION";
	/** Enumeration value for full access permission ("FULL_ACCESS_PERMISSION"). */
	public final static String FULL_ACCESS_PERMISSION = "FULL_ACCESS_PERMISSION";

	/** Prevent users from creating UserUtility Objects. */
	private UserUtility() {}

	/**
	  Add a user's authorization permission to the database.

	  <P>
 	  Only users with FULL_ACCESS_PERMISSION may use this.
	  
	  @param userName the user's name. A valid possibly delimited
	  SQL identifier.
	  @param permission READ_ACCESS_PERMISSION or FULL_ACCESS_PERMISSION.
	  @exception SQLException thrown if this fails.
	  */
	public static final void add(String userName, String permission)
		 throws SQLException
	{
		String pv;
		TransactionController tc = ConnectionUtil.getCurrentLCC().getTransactionExecute();
		try {
		normalizeIdParam("userName",userName); //Validate
		if (permission==null)
			throw StandardException.newException(SQLState.UU_INVALID_PARAMETER, "permission","null");			
		if (permission.equals(READ_ACCESS_PERMISSION))
		{
			pv = (String)tc.getProperty(Property.READ_ONLY_ACCESS_USERS_PROPERTY);
			pv = IdUtil.appendId(userName,pv);
			PropertyInfo.setDatabaseProperty(Property.READ_ONLY_ACCESS_USERS_PROPERTY,pv);
		}
		else if (permission.equals(FULL_ACCESS_PERMISSION))
		{
			pv = (String)tc.getProperty(Property.FULL_ACCESS_USERS_PROPERTY);
			pv = IdUtil.appendId(userName,pv);
			PropertyInfo.setDatabaseProperty(Property.FULL_ACCESS_USERS_PROPERTY,pv);
		}
		else
			throw StandardException.newException(SQLState.UU_UNKNOWN_PERMISSION, permission);
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
	  Set the authorization permission for a user in the database.

	  <P>
	  Only users with FULL_ACCESS_PERMISSION may use this.

	  @param userName the user's name. A valid possibly delimited
	  SQL identifier.
	  @param permission READ_ACCESS_PERMISSION or FULL_ACCESS_PERMISSION.
	  @exception SQLException thrown if this fails.
	  */
	public static final void set(String userName, String permission)
		 throws SQLException
	{
		drop(userName);
		add(userName,permission);
	}

	/**
	  Drop a user's authorization permission from the database.

	  <P>
	  Only users with FULL_ACCESS_PERMISSION may use this.

	  @param userName the user's name. A valid possibly delimited
	  SQL identifier.
	  @return if the user existed in the database and was
	  dropped return true. If the user did not exist
	  in the database to start with return false.
	  @exception SQLException thrown if this fails or the user
	  being dropped does not exist.
	  */
	public static final void drop(String userName) throws
	SQLException
	{
		TransactionController tc = ConnectionUtil.getCurrentLCC().getTransactionExecute();

		try {
		String userId = normalizeIdParam("userName",userName); 

		String access = getPermission(userName);
		if (access != null && access.equals(READ_ACCESS_PERMISSION))
		{
			String pv = (String)tc.getProperty(Property.READ_ONLY_ACCESS_USERS_PROPERTY);
			String newList = IdUtil.deleteId(userId,pv);
			PropertyInfo.setDatabaseProperty(Property.READ_ONLY_ACCESS_USERS_PROPERTY,newList);
		}
		else if (access != null && access.equals(FULL_ACCESS_PERMISSION))
		{
			String pv = (String)tc.getProperty(Property.FULL_ACCESS_USERS_PROPERTY);
			String newList = IdUtil.deleteId(userId,pv);
			PropertyInfo.setDatabaseProperty(Property.FULL_ACCESS_USERS_PROPERTY,newList);
		}
		else
		{
			throw StandardException.newException(SQLState.UU_UNKNOWN_USER, userName);
		}
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	/**
	  Return a user's authorization permission in a database.

	  <P>
	  Users with FULL_ACCESS_PERMISSION or READ_ACCESS_PERMISSION
	  may use this.
	  
	  @param userName the user's name. A valid possibly delimited
	  SQL identifier.
	  @return FULL_ACCESS_PERMISSION if the user is in "derby.database.fullAccessUsers",
	          READ_ACCESS_PERMISSION if the user is in "derby.database.readOnlyAccessUsers",
			  or null if the user is not in either list.
	  @exception SQLException thrown if this fails.
	  */
	public static final String getPermission(String userName)
         throws SQLException
	{
		TransactionController tc = ConnectionUtil.getCurrentLCC().getTransactionExecute();

		try {

		String pv = (String)
			tc.getProperty(Property.READ_ONLY_ACCESS_USERS_PROPERTY);
		String userId = normalizeIdParam("userName",userName); 
		if (IdUtil.idOnList(userId,pv)) return READ_ACCESS_PERMISSION;
		pv = (String)tc.getProperty(Property.FULL_ACCESS_USERS_PROPERTY);
		if (IdUtil.idOnList(userId,pv)) return FULL_ACCESS_PERMISSION;
		return null;
		} catch (StandardException se) {
			throw PublicAPI.wrapStandardException(se);
		}
	}

	private static String normalizeIdParam(String pName, String pValue)
		 throws StandardException
	{
		if (pValue==null)
			throw StandardException.newException(SQLState.UU_INVALID_PARAMETER, pName,"null");
			
		try {
			return IdUtil.parseId(pValue);
		}
		catch (StandardException se) {
			throw StandardException.newException(SQLState.UU_INVALID_PARAMETER, se, pName,pValue);
		}
	}
}
