/*

   Licensed Materials - Property of IBM
   Cloudscape - Package org.apache.derbyTesting.functionTests.util.StaticInitializers
   (C) Copyright IBM Corp. 1998, 2004. All Rights Reserved.
   US Government Users Restricted Rights - Use, duplication or
   disclosure restricted by GSA ADP Schedule Contract with IBM Corp.

 */

package org.apache.derbyTesting.functionTests.util.StaticInitializers;

import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;

/** Test DML statement called from within static initializer */
public class DMLInStaticInitializer
{ 
	/**
		IBM Copyright &copy notice.
	*/
	private static final String copyrightNotice = org.apache.derby.iapi.reference.Copyright.SHORT_1998_2004;

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
