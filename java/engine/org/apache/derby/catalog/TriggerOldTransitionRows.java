/*

   Derby - Class org.apache.derby.catalog.TriggerOldTransitionRows

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

package org.apache.derby.catalog;

import org.apache.derby.iapi.db.Factory;
import org.apache.derby.iapi.db.TriggerExecutionContext;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.math.BigDecimal;

/**
 * Provides information about a set of rows before a trigger action
 * changed them.
 * 
 *
 * <p>
 * This class implements only JDBC 1.2, not JDBC 2.0.  You cannot
 * compile this class with JDK1.2, since it implements only the
 * JDBC 1.2 ResultSet interface and not the JDBC 2.0 ResultSet
 * interface.  You can only use this class in a JDK 1.2 runtime 
 * environment if no JDBC 2.0 calls are made against it.
 *
 */
public class TriggerOldTransitionRows extends org.apache.derby.vti.UpdatableVTITemplate
{

	private ResultSet resultSet;

	/**
	 * Construct a VTI on the trigger's old row set.
	 * The old row set is the before image of the rows
	 * that are changed by the trigger.  For a trigger
	 * on a delete, this is all the rows that are deleted.	
	 * For a trigger on an update, this is the rows before
	 * they are updated.  For an insert, this throws an 
	 * exception.
	 *
	 * @exception SQLException thrown if no trigger active
	 */
	public TriggerOldTransitionRows() throws SQLException
	{
		initializeResultSet();
	}

	private ResultSet initializeResultSet() throws SQLException {
		if (resultSet != null)
			resultSet.close();
		
		TriggerExecutionContext tec = Factory.getTriggerExecutionContext();
		if (tec == null)
		{
			throw new SQLException("There are no active triggers", "38000");
		}
		resultSet = tec.getOldRowSet();

		if (resultSet == null)
		{
			throw new SQLException("There is no old transition rows result set for this trigger", "38000");
		}
		return resultSet;
	}  

       public ResultSet executeQuery() throws SQLException {
    	   //DERBY-4095. Need to reinititialize ResultSet on 
           //executeQuery, in case it was closed in a NESTEDLOOP join.
           return initializeResultSet();
       }
        
       public int getResultSetConcurrency() {
            return ResultSet.CONCUR_READ_ONLY;
       }
       public void close() throws SQLException {
           resultSet.close();
       }
}
