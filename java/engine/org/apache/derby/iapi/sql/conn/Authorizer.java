/*

   Derby - Class org.apache.derby.iapi.sql.conn.Authorizer

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

package org.apache.derby.iapi.sql.conn;

import java.util.List;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.dictionary.StatementPermission;
import org.apache.derby.iapi.error.StandardException;

/**
  The Authorizer verifies a connected user has the authorization 
  to perform a requested database operation using the current
  connection.

  <P>
  Today no object based authorization is supported.
  */
public interface Authorizer
{
	/** SQL write (insert,update,delete) operation */
	public static final int SQL_WRITE_OP = 0;
	/** SQL SELECT  operation */
	public static final int	SQL_SELECT_OP = 1;
	/** Any other SQL operation	*/
	public static final int	SQL_ARBITARY_OP = 2;
	/** SQL CALL/VALUE  operation */
	public static final int	SQL_CALL_OP = 3;
	/** SQL DDL operation */
	public static final int SQL_DDL_OP   = 4;
	/** database property write operation */
	public static final int PROPERTY_WRITE_OP = 5;
	/**  database jar write operation */	
	public static final int JAR_WRITE_OP = 6;
	
	/* Privilege types for SQL standard (grant/revoke) permissions checking. */
	public static final int NULL_PRIV = -1;
	public static final int SELECT_PRIV = 0;
	public static final int UPDATE_PRIV = 1;
	public static final int REFERENCES_PRIV = 2;
	public static final int INSERT_PRIV = 3;
	public static final int DELETE_PRIV = 4;
	public static final int TRIGGER_PRIV = 5;
	public static final int EXECUTE_PRIV = 6;
	public static final int USAGE_PRIV = 7;
    /* 
     * DERBY-4191
     * Used to check if user has a table level select privilege/any column 
     * level select privilege to fulfill the requirements for following kind 
     * of queries
     * select count(*) from t1
     * select count(1) from t1
     * select 1 from t1
     * select t1.c1 from t1, t2
     * DERBY-4191 was added for Derby bug where for first 3 queries above,
     * we were not requiring any select privilege on t1. And for the 4th
     * query, we were not requiring any select privilege on t2 since no
     * column was selected from t2
     */
	public static final int MIN_SELECT_PRIV = 8;
    public static final int PRIV_TYPE_COUNT = 9;
    
	/* Used to check who can create schemas or who can modify objects in schema */
	public static final int CREATE_SCHEMA_PRIV = 16;
	public static final int MODIFY_SCHEMA_PRIV = 17;
	public static final int DROP_SCHEMA_PRIV = 18;

    /* Check who can create and drop roles */
	public static final int CREATE_ROLE_PRIV = 19;
	public static final int DROP_ROLE_PRIV = 20;

	/**
	 * The system authorization ID is defined by the SQL2003 spec as the grantor
	 * of privileges to object owners.
	 */
	public static final String SYSTEM_AUTHORIZATION_ID = "_SYSTEM";

	/**
	 * The public authorization ID is defined by the SQL2003 spec as implying all users.
	 */
	public static final String PUBLIC_AUTHORIZATION_ID = "PUBLIC";

	/**
	  Verify the connected user is authorized to perform the requested
	  operation.

	  This variation should only be used with operations that do not use tables
	  or routines. If the operation involves tables or routines then use the
	  variation of the authorize method that takes an Activation parameter. The
	  activation holds the table, column, and routine lists.

	  @param operation the enumeration code for the requsted operation.

	  @exception StandardException Thrown if the operation is not allowed
	 */
	public void authorize( int operation) throws StandardException;
    
	/**
	  Verify the connected user is authorized to perform the requested
	  operation.

	  @param activation holds the list of tables, columns, and routines used.
	  @param operation the enumeration code for the requsted operation.

	  @exception StandardException Thrown if the operation is not allowed
	*/
	public void authorize(Activation activation, int operation)
				throws StandardException;

	/**
	  Verify the connected user possesses the indicated permissions

	  @param requiredPermissionsList    the required permissions
	  @param activation holds the execution logic

	  @exception StandardException Thrown if the operation is not allowed
	*/
	public void authorize
        (
         List<StatementPermission> requiredPermissionsList,
         Activation activation
         )
        throws StandardException;

   /**
	 Get the readOnly status for this authorizer's connection.
	 */
   public boolean isReadOnlyConnection();

   /**
	 Set the readOnly status for this authorizer's connection.
	 @param on true means set the connection to read only mode,
	           false means set the connection to read wrte mode.
	 @param authorize true means to verify the caller has authority
	        to set the connection and false means do not check. 
	 @exception StandardException Oops not allowed.
	 */
   public void setReadOnlyConnection(boolean on, boolean authorize)
		 throws StandardException;

   /**
	 Refresh this authorizer to reflect a change in the database
	 permissions.
	 
	 @exception StandardException Oops.
	 */
   public void refresh() throws StandardException;  
}
