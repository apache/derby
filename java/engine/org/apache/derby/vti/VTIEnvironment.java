/*

   Derby - Class org.apache.derby.vti.VTIEnvironment

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
