/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derby.vti
   (C) Copyright IBM Corp. 1999, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derby.vti;

/**
  * 
  *	VTIEnvironment is an interface used in costing VTIs.
  * 
  * The interface is
  * passed as a parameter to various methods in the Virtual Table interface.
  * <I>IBM Corp. reserves the right to change, rename, or
  * remove this interface at any time.</I>
  * @see org.apache.derby.vti.VTICosting
  */
public interface VTIEnvironment
{
	/**
		IBM Copyright &copy notice.
	*/

	public static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1999_2004;

	/**
		Return true if this instance of the VTI has been created for compilation,
		false if it is for runtime execution.
	*/
	public boolean isCompileTime();

	/**
		Return the SQL text of the original SQL statement.
	*/
	public String getOriginalSQL();

	/**
		Get the  specific JDBC isolation of the statement. If it returns Connection.TRANSACTION_NONE
		then no isolation was specified and the connection's isolation level is implied.
	*/
	public int getStatementIsolationLevel();

	/**
		Saves an object associated with a key that will be maintained
		for the lifetime of the statement plan.
		Any previous value associated with the key is discarded.
		Any saved object can be seen by any JDBC Connection that has a Statement object
		that references the same statement plan.
	*/
	public void setSharedState(String key, java.io.Serializable value);

	/**
		Get an an object associated with a key from set of objects maintained with the statement plan.
	*/
	public Object getSharedState(String key);
}
