/*

   Derby - Class org.apache.derbyTesting.functionTests.util.StaticInitializers.InsertInStaticInitializer

   Copyright 2000, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.util.StaticInitializers;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test Insert statement called from within static initializer holds
 * onto locks it should hold onto and doesn't hold onto locks it shouldn't
 * hold onto.
*/
public class InsertInStaticInitializer
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
			boolean b = s.execute("INSERT into t1 values (1)");

			//if (rs.next())
			//{
			//	System.out.println("Value of t1.s is " + rs.getShort(1));
			//}
		}
		catch (SQLException se)
		{
			System.out.println("Caught exception " + se);
			se.printStackTrace(System.out);
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
				System.out.println("Caught exception " + se);
				se.printStackTrace(System.out);
			}
		}
	}
}
