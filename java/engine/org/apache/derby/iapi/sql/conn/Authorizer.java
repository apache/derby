/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.iapi.sql.conn
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.iapi.sql.conn;

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
	/**
		IBM Copyright &copy notice.
	*/
	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;
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
	
	/**
	  Verify the connected user is authorized to perform the requested
	  operation.

	  @param operation the enumeration code for the requsted operation.

	  @exception StandardException Thrown if the operation is not allowed
	 */
	public void authorize(int operation) throws StandardException;

    /**
	  Get the Authorization ID for this Authorizer.
	  */
   public String getAuthorizationId();

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
	 
	 @exception AuthorizerSessionException Connect permission gone.
	 @exception StandardException Oops.
	 */
   public void refresh() throws StandardException;  
}
