/*

   Derby - Class org.apache.derby.vti.VTIEnvironment

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

package org.apache.derby.vti;

/**
  *
  * <P>
  *	VTIEnvironment is the state variable created by the optimizer to help it
  *	place a Table Function in the join order.
  *	The methods of <a href="./VTICosting.html">VTICosting</a> use this state variable in
  *	order to pass information to each other and learn other details of the
  *	operating environment.
  * </P>
  *
  * @see org.apache.derby.vti.VTICosting
  */
public interface VTIEnvironment
{

	/**
		Return true if this instance of the Table Function has been created for compilation,
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
		Get an object associated with a key from set of objects maintained with the statement plan.
	*/
	public Object getSharedState(String key);
}
