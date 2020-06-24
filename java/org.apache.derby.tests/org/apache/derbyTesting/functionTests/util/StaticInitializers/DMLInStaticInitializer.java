/*

   Derby - Class org.apache.derbyTesting.functionTests.util.StaticInitializers.DMLInStaticInitializer

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

package org.apache.derbyTesting.functionTests.util.StaticInitializers;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test DML statement called from within static initializer */
public class DMLInStaticInitializer
{

	/* This is the method that is invoked from the outer query */
	public static int getANumber()
	{
		return 1;
	}

	static
	{
		/* Execute a DML statement from within the static initializer */
		doADMLStatement();
	}

	private static void doADMLStatement()
	{
		ResultSet rs = null;

		try
		{
			int	value;

			/* Connect to the database */
			Statement s = DriverManager.getConnection(
						"jdbc:default:connection").createStatement();

			/* Execute a DML statement.  This depends on t1 existing. */
			rs = s.executeQuery("SELECT s FROM t1");

			if (rs.next())
			{
				System.out.println("Value of t1.s is " + rs.getShort(1));
			}
		}
		catch (SQLException se)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-2518
			if (!se.getSQLState().equals("38001")){
				throw new ExceptionInInitializerError(se);
			}
		}
		finally
		{
			try
			{
				if (rs != null)
					rs.close();
			}
			catch (SQLException se)
			{
//IC see: https://issues.apache.org/jira/browse/DERBY-2518
				if (!se.getSQLState().equals("38001")) {
					throw new ExceptionInInitializerError(se);
				}
			}
		}
	}
}
