/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.outparams

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

package org.apache.derbyTesting.functionTests.tests.lang;
import java.sql.*;

import org.apache.derby.tools.ij;
import org.apache.derby.iapi.reference.JDBC30Translation;
import java.io.PrintStream;
import java.math.BigInteger;
import java.math.BigDecimal;

public class outparams
{
 
	static final String outputMethods[] =
	{
		"takesNothing",

		null, 
	
		null,
		null,
	
		"takesShortPrimitive",
		null,
	
		"takesIntegerPrimitive",
		null,
	
		"takesLongPrimitive",
		null,
	
		"takesFloatPrimitive",
		null,
	
		"takesDoublePrimitive",
		null,
	
		"takesBigDecimal",
	
		"takesByteArray",
		
		"takesString",
		
		"takesDate",
		
		"takesTimestamp",
	
		"takesTime",
	
		null
	};

	// parameter types for outputMethods.
	private static final String[] outputProcParam =
	{
		null, // "takesNothing",

		null, 
	
		null,
		null,
	
		"SMALLINT", // "takesShortPrimitive",
		null,
	
		"INT", // "takesIntegerPrimitive",
		null,
	
		"BIGINT", // "takesLongPrimitive",
		null,
	
		"REAL", // "takesFloatPrimitive",
		null,
	
		"DOUBLE", // "takesDoublePrimitive",
		null,
	
		"DECIMAL(10,4)", // "takesBigDecimal",
	
		"VARCHAR(40) FOR BIT DATA", // "takesByteArray",
		
		"VARCHAR(40)", // "takesString",
		
		"DATE", // "takesDate",
		
		"TIMESTAMP", // "takesTimestamp",
	
		"TIME", // "takesTime",
	
		null
	};

	
	static final String returnMethods[] =
	{
		"returnsNothing",

		null,
		null,
	
		"returnsShortP",
		null,
	
		"returnsIntegerP",
		null,
	
		"returnsLongP",
		null,
	
		"returnsFloatP",
		null,
	
		"returnsDoubleP",
		null,
	
		"returnsBigDecimal",
	
		"returnsByteArray",
		
		"returnsString",
		
		"returnsDate",
		
		"returnsTimestamp",
	
		"returnsTime",
	
		null
	};

	static final String[] returnMethodType =
	{
		null, // "returnsNothing",

		null, // "returnsBytePrimitive",
		null, // "returnsByte",
	
		"SMALLINT", // "returnsShortPrimitive",
		null, // "returnsShort",
	
		"INT", // "returnsIntegerPrimitive",
		null, // "returnsInteger",
	
		"BIGINT", // "returnsLongPrimitive",
		null, // "returnsLong",
	
		"REAL", // "returnsFloatPrimitive",
		null, // "returnsFloat",
	
		"DOUBLE", // "returnsDoublePrimitive",
		null, // "returnsDouble",
	
		"DECIMAL(10,2)", // "returnsBigDecimal",
	
		"VARCHAR(40) FOR BIT DATA", // "returnsByteArray",
		
		"VARCHAR(40)", // "returnsString",
		
		"DATE", // "returnsDate",
		
		"TIMESTAMP", // "returnsTimestamp",
	
		"TIME", // "returnsTime",
	
		null, // "returnsBigInteger"
	};
	
	static final int types[] =
	{
		Types.BIT,
		JDBC30Translation.SQL_TYPES_BOOLEAN,
		Types.TINYINT,
		Types.SMALLINT,
		Types.INTEGER,
		Types.BIGINT,
		Types.FLOAT,
		Types.REAL,
		Types.DOUBLE,
		Types.NUMERIC,
		Types.DECIMAL,
		Types.CHAR,
		Types.VARCHAR,
		Types.LONGVARCHAR,
		Types.DATE,
		Types.TIME, 
		Types.TIMESTAMP,
		Types.BINARY,
		Types.VARBINARY,
		Types.LONGVARBINARY,
		Types.OTHER
	};
	
	static final String typeNames[] =
	{
		"BIT",
		"BOOLEAN",
		"TINYINT",
		"SMALLINT",
		"INTEGER",
		"BIGINT",
		"FLOAT",
		"REAL",
		"DOUBLE",
		"NUMERIC",
		"DECIMAL",
		"CHAR",
		"VARCHAR",
		"LONGVARCHAR",
		"DATE",
		"TIME",
		"TIMESTAMP",
		"BINARY",
		"VARBINARY",
		"LONGVARBINARY",
		"OTHER"
	};

	//public static Connection conn;

	public static void main (String[] argv) throws Throwable
	{
   		ij.getPropertyArg(argv); 
        Connection conn = ij.startJBMS();

        runTests( conn);
    }

    public static void runTests( Connection conn) throws Throwable
    {        
		conn.setAutoCommit(false);	

		testMisc(conn);
		testNull(conn);
		testUpdate(conn);
		testEachOutputType(conn);
		testReturnTypes(conn);
		testOtherOutputType(conn);
		testManyOut(conn);
		test5116(conn);
	}

	private static void testMisc(Connection conn) throws Throwable
	{
		System.out.println("==============================================");
		System.out.println("TESTING BOUNDARY CONDITIONS");
		System.out.println("==============================================\n");

		Statement scp = conn.createStatement();

		scp.execute("CREATE PROCEDURE takesString(OUT P1 VARCHAR(40), IN P2 INT) " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.takesString'" +
						" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");

		CallableStatement cs = conn.prepareCall("call takesString(?,?)");

		// register a normal int as an output param, should fail
		boolean failed = false;
		try
		{
			cs.registerOutParameter(2, Types.INTEGER);
		}
		catch (SQLException se)
		{
			failed = true;
			System.out.println("Expected exception "+se);
		}
		if (!failed)
		{
			System.out.println("registerOutParameter on non-output didn't fail");
		}

		// invalid param number
		failed = false;
		try
		{
			cs.registerOutParameter(9, Types.INTEGER);
		}
		catch (SQLException se)
		{
			failed = true;
			System.out.println("Expected exception "+se);
		}
		if (!failed)
		{
			System.out.println("registerOutParameter on bad value didn't fail");
		}

		// invalid param number
		failed = false;
		try
		{
			cs.registerOutParameter(0, Types.INTEGER);
		}
		catch (SQLException se)
		{
			failed = true;
			System.out.println("Expected exception "+se);
		}
		if (!failed)
		{
			System.out.println("registerOutParameter on bad value didn't fail");
		}

		// set before register, bad type, should fail as is output parameter.	
		try
		{
			cs.setDouble(1, 1);
			System.out.println("FAIL setDouble() on takesString() accepted");
		}
		catch (SQLException se)
		{
			System.out.println("Expected exception "+se);
		}

		// set before register, should fail as is output parameter.
		try
		{
			cs.setString(1, "hello");
			System.out.println("FAIL setString() on takesString() accepted");
		}
		catch (SQLException se)
		{
			System.out.println("Expected exception "+se);
		}

		cs.registerOutParameter(1, Types.CHAR);
		cs.setInt(2, Types.INTEGER);
		try
		{
			cs.execute();
		}
		catch (SQLException se)
		{
			System.out.println("cs.execute() got unexpected exception: "+se);
		}

		// shouldn't have to reregister the type, and shouldn't
		// need to set the output parameters
		cs.clearParameters();
		cs.setInt(2, Types.INTEGER);
		try
		{
			cs.execute();
		}
		catch (SQLException se)
		{
			System.out.println("cs.execute() got unexpected exception: "+se);
		}
		cs.close();
		scp.execute("DROP PROCEDURE takesString");

		scp.execute("CREATE FUNCTION returnsBigDecimal(P2 INT) RETURNS DECIMAL(10,2) " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.returnsBigDecimal'" +
						" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
		// return output params -- cannot do set on return output param
		cs = conn.prepareCall("? = call returnsBigDecimal(?)");
		try
		{
			cs.setBigDecimal(1, new BigDecimal(1d));
			System.out.println("ERROR: setBigDecimal() on return output parameter succeeded");
		}
		catch (SQLException se)
		{
			System.out.println("Expected exception on setBigDecimal() on a return output param: "+se);
		}
		cs.close();
		scp.execute("DROP FUNCTION returnsBigDecimal");

		// lets try ? = call syntax on a call that doesn't return anything
		
		scp.execute("CREATE PROCEDURE returnsNothing() " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.returnsNothing'" +
						" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
		try
		{
			cs = conn.prepareCall("? = call returnsNothing()");
			System.out.println("ERROR: no exception on prepare of '? = call returnsNothing()");
		}
		catch (SQLException se)
		{
			System.out.println("Expected exception on prepare of '? = call returnsNothing()': "+se);
		}
		scp.execute("DROP PROCEDURE returnsNothing");
	}

	
	private static void testNull(Connection conn) throws Throwable
	{
		System.out.println("==============================================");
		System.out.println("TESTING NULLS");
		System.out.println("==============================================\n");
		System.out.println("Test for bug 4317, passing null value for a parameter");

		Statement scp = conn.createStatement();

		scp.execute("CREATE PROCEDURE testNullBug4317(IN P1 VARCHAR(10)) " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.testNullBug4317'" +
						" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


		CallableStatement cs0 = conn.prepareCall("call testNullBug4317(?)");
		try
		{
			cs0.setString(1, null);		// passing in null
			cs0.execute();
		}
		catch (SQLException se)
		{
			System.out.println("cs0.execute() got unexpected exception: "+se);
		}

		try
		{
			// BUG 5928 - setNull throws an exception - fixed.
			cs0.setNull(1, java.sql.Types.VARCHAR);		// passing in null
			cs0.execute();
		}
		catch (SQLException se)
		{
			System.out.println("cs0.execute() got unexpected exception: "+se);
		}
		cs0.close();
		scp.execute("DROP PROCEDURE testNullBug4317");


	}

	// test: do we get an appropriate update count when using ?=call?
	private static void testUpdate(Connection conn) throws Throwable
	{
		System.out.println("==============================================");
		System.out.println("TESTING UPDATE COUNT");
		System.out.println("==============================================\n");

		Statement scp = conn.createStatement();

		scp.execute("CREATE FUNCTION returnsIntegerP(P1 INT) RETURNS INTEGER " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.returnsIntegerP'" +
						" NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");



		CallableStatement cs = conn.prepareCall("? = call returnsIntegerP(0)");
		cs.registerOutParameter(1, Types.INTEGER);
		try
		{
			int updCount = cs.executeUpdate();
			System.out.println("executeUpdate on ? = call returnsIntegerP returned "+updCount);
			System.out.println("getString(1) returned "+cs.getString(1));
		}
		catch (SQLException se)
		{
			System.out.println("cs.execute() got unexpected exception: "+se);
		}

		cs.close();
		scp.execute("DROP FUNCTION returnsIntegerP");
		scp.close();
	}

	// should do get setString() to use a string that is appropriate for
	//	the target type
	private static void testEachOutputType(Connection conn) throws Throwable
	{
		System.out.println("==============================================");
		System.out.println("TESTING NORMAL OUTPUT PARAMETERS");
		System.out.println("==============================================\n");
		CallableStatement cs = null;

		for (int doSetObject = 0; doSetObject < 3; doSetObject++)
		{
			switch (doSetObject)
			{
				case 0:
					System.out.println("...starting doing setXXX for each type xxx");
					break;
				case 1:
					System.out.println("...now doing setObject on each type xxx");
					break;
				case 2:
					System.out.println("...not doing any setXXX, just OUT parameters, not IN/OUT");
					break;
			}

			for (int method = 0; method < outputMethods.length; method++)
			{
				String methodName = outputMethods[method];
				if (methodName == null)
					continue;

				System.out.println("\n------------------------------------");

				Statement scp = conn.createStatement();
				String str;
				if (methodName.indexOf("Nothing") == -1)
				{

					scp.execute("CREATE PROCEDURE " + methodName + "(INOUT P1 " + outputProcParam[method] + ", IN P2 INT) " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams." + methodName +
						"' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


					if (method%2 == 0)
						str = "call "+methodName+"(?,?)";
					else
						str = "{call "+methodName+"(?,?)}";
				}
				else
				{
					scp.execute("CREATE PROCEDURE " + methodName + "() " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams." + methodName +
						"' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
					str = "{call "+methodName+"()}";
				}


	
				System.out.println(str);
				try 
				{
					cs = conn.prepareCall(str);
				}
				catch (SQLException se)
				{
					System.out.println("ERROR: unexpected exception "+se);
					throw se;
				}
	
				for (int type = 0; type < types.length; type++)
				{
					cs.clearParameters();
					System.out.println();
					try
					{
						System.out.println("\n\tcs.registerOutParameter(1, "+typeNames[type]+")");
						cs.registerOutParameter(1, types[type]);
					} 
					catch (SQLException se)
					{
						System.out.println("\tException "+se);
						continue;
					}
	
					StringBuffer buf = new StringBuffer();
					try
					{
						if (doSetObject == 0) 
						{
							callSetMethod(cs, 1, types[type], buf);
						}
						else if (doSetObject == 1)
						{
							callSetObject(cs, 1, types[type], buf);
						}
						else
						{
							// only try this once
							type = types.length-1;
							buf.append("...no setXXX(1) at all");
						}
					}
					catch (SQLException se)
					{
						System.out.println("\t"+buf.toString());
						System.out.println("\tException "+se);
						continue;
					}
					System.out.println("\t"+buf.toString());
					cs.setInt(2, types[type]);
	
					try
					{
						System.out.println("\tcs.execute()");
						boolean hasResultSet = cs.execute();
						if (hasResultSet)
							System.out.println("testEachOutputType HAS RESULT SET cs.execute() returned true");
						}
					catch (SQLException se)
					{
						System.out.println("\tException "+se);
						continue;
					}
					for (int getType = 0; getType < types.length; getType++)
					{
						StringBuffer getbuf = new StringBuffer();
						try
						{
							callGetMethod(cs, 1, types[getType], getbuf);
						}
						catch (SQLException se)
						{
							getbuf.append(se);
						}
						System.out.println("\t\t\t"+getbuf.toString());
					}
	
				}

				cs.close();

				scp.execute("DROP PROCEDURE " + methodName);
				scp.close();
			}
		}

		System.out.println("------------------------------------\n");

	}

	// test that everything works ok when we regsiter the param as type OTHER.
	// should be able to get/setXXX of the appropriate type
	private static void testOtherOutputType(Connection conn) throws Throwable
	{
		System.out.println("==============================================");
		System.out.println("TESTING OUTPUT PARAMETERS WITH register(OTHER)");
		System.out.println("==============================================\n");
		CallableStatement cs = null;

		for (int method = 0; method < outputMethods.length; method++)
		{
			String methodName = outputMethods[method];
			if (methodName == null)
				continue;
			System.out.println("\n------------------------------------");


			Statement scp = conn.createStatement();
			String str;
			if (methodName.indexOf("Nothing") == -1)
			{

				scp.execute("CREATE PROCEDURE " + methodName + "(INOUT P1 " + outputProcParam[method] + ", IN P2 INT) " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams." + methodName +
					"' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


				if (method%2 == 0)
					str = "call "+methodName+"(?,?)";
				else
					str = "{call "+methodName+"(?,?)}";
			}
			else
			{
				scp.execute("CREATE PROCEDURE " + methodName + "() " +
					"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams." + methodName +
					"' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
				str = "{call "+methodName+"()}";
			}

			System.out.println(str);
			try 
			{
				cs = conn.prepareCall(str);
			}
			catch (SQLException se)
			{
				System.out.println("ERROR: unexpected exception "+se);
				throw se;
			}
	
			for (int type = 0; type < types.length; type++)
			{
				cs.clearParameters();
				System.out.println();
				try
				{
					System.out.println("\n\tcs.registerOutParameter(1, Types.OTHER)");
					cs.registerOutParameter(1, Types.OTHER);
				} 
				catch (SQLException se)
				{
					System.out.println("\tException "+se);
					continue;
				}

				StringBuffer buf = new StringBuffer();
				try
				{
					callSetMethod(cs, 1, types[type], buf);
				}
				catch (SQLException se)
				{
					System.out.println("\t"+buf.toString());
					System.out.println("\tException "+se);
					continue;
				}
				System.out.println("\t"+buf.toString());
				cs.setInt(2, types[type]);

				try
				{
					System.out.println("\tcs.execute()");
					cs.execute();
				}
				catch (SQLException se)
				{
					System.out.println("\tException "+se);
					continue;
				}
				for (int getType = 0; getType < types.length; getType++)
				{
					StringBuffer getbuf = new StringBuffer();
					try
					{
						callGetMethod(cs, 1, types[getType], getbuf);
					}
					catch (SQLException se)
					{
						getbuf.append(se);
					}
					System.out.println("\t\t\t"+getbuf.toString());
				}

			}

			cs.close();

			scp.execute("DROP PROCEDURE " + methodName);
			scp.close();
		}

		System.out.println("------------------------------------\n");
	}

	private static void testReturnTypes(Connection conn) throws Throwable
	{
		System.out.println("==============================================\n");
		System.out.println("TESTING RETURN OUTPUT PARAMETERS");
		System.out.println("==============================================\n");
		CallableStatement cs = null;
		for (int method = 0; method < returnMethods.length; method++)
		{
			String methodName = returnMethods[method];
			if (methodName == null)
				continue;

			Statement scf = conn.createStatement();
			String str;
			String dropRoutine;
			if (methodName.indexOf("Nothing") != -1)
			{

				scf.execute("CREATE PROCEDURE " + methodName + "()" +
					" EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams." + methodName +
					"' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


				dropRoutine = "DROP PROCEDURE " + methodName;

				str = "{call "+returnMethods[method]+"()}";
			}
			else
			{

				scf.execute("CREATE FUNCTION " + methodName + "(P1 INT) RETURNS " + returnMethodType[method] +
					" EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams." + methodName +
					"' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");
				dropRoutine = "DROP FUNCTION " + methodName;

				str = "{? = call "+returnMethods[method]+"(?)}";
			}


				
			System.out.println("\n------------------------------------");
				

			System.out.println(str);
			try 
			{
				cs = conn.prepareCall(str);
			}
			catch (SQLException se)
			{
				System.out.println("ERROR: unexpected exception "+se);
				throw se;
			}

			for (int type = 0; type < types.length; type++)
			{
				cs.clearParameters();
				System.out.println();
				try
				{
					System.out.println("\n\tcs.registerOutParameter(1, "+typeNames[type]+")");
					cs.registerOutParameter(1, types[type]);
				} 
				catch (SQLException se)
				{
					System.out.println("\tException "+se);
					continue;
				}
				try
				{
					cs.setInt(2, types[type]);
				}
				catch (SQLException se)
				{
					System.out.println("\tUnexpected exception on cs.setInt(2, "+types[type]+"): "+se);
					continue;
				}

				try
				{
					System.out.println("\tcs.execute()");
					boolean hasResultSet = cs.execute();
					if (hasResultSet)
						System.out.println("testReturnTypes HAS RESULT SET cs.execute() returned true");

				}
				catch (SQLException se)
				{
					System.out.println("\tException "+se);
					continue;
				}
				for (int getType = 0; getType < types.length; getType++)
				{
					StringBuffer getbuf = new StringBuffer();
					try
					{
						callGetMethod(cs, 1, types[getType], getbuf);
					}
					catch (SQLException se)
					{
						getbuf.append(se);
					}
					System.out.println("\t\t\t"+getbuf.toString());
				}

			}

			cs.close();
			scf.execute(dropRoutine);
			scf.close();
		}

		System.out.println("------------------------------------\n");

	}

	private static void callSetObject(CallableStatement cs, int arg, int type, StringBuffer strbuf) throws Throwable
	{

		switch (type)	
		{
			case Types.BIT:
			case JDBC30Translation.SQL_TYPES_BOOLEAN:
				strbuf.append("setObject("+arg+", true)");
				cs.setObject(arg, new Boolean(true));
				break;

			case Types.TINYINT:
				strbuf.append("setObject("+arg+", 6)");
				cs.setObject(arg, new Integer((byte)6));
				break;

			case Types.SMALLINT:
				strbuf.append("setObject("+arg+", 66)");
				cs.setObject(arg, new Integer((short)66));
				break;

			case Types.INTEGER:
				strbuf.append("setObject("+arg+", 666)");
				cs.setObject(arg, new Integer(666));
				break;

			case Types.BIGINT:
				strbuf.append("setObject("+arg+", 666)");
				cs.setObject(arg, new Long(666));
				break;

			case Types.FLOAT:
			case Types.REAL:
				strbuf.append("setObject("+arg+", 666)");
				cs.setObject(arg, new Float(666));
				break;

			case Types.DOUBLE:
				strbuf.append("setObject("+arg+", 666)");
				cs.setObject(arg, new Double(666));
				break;

			case Types.DECIMAL:
			case Types.NUMERIC:
				strbuf.append("setObject("+arg+", 666.666)");
				BigDecimal bd = new BigDecimal("666.666");
				cs.setObject(arg, bd);
				break;

			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				strbuf.append("setObject("+arg+", \"Set via setString()\")");
				cs.setObject(arg, "Set via setString()");
				break;

			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				strbuf.append("setObject("+arg+", byte[])");
				byte[] myarray = new byte[16];
				myarray[0] = (byte)255;
				cs.setObject(arg, myarray);
				break;

			case Types.DATE:
				strbuf.append("setObject("+arg+", Date.valueOf(1999-09-09))");
				cs.setObject(arg, Date.valueOf("1999-09-09"));
				break;

			case Types.TIME:
				strbuf.append("setObject("+arg+", Time.valueOf(09:09:09))");
				cs.setObject(arg, Time.valueOf("09:09:09"));
				break;

			case Types.TIMESTAMP:
				strbuf.append("setObject("+arg+", Timestamp.valueOf(1999-09-09 09:09:09.999))");
				cs.setObject(arg, Timestamp.valueOf("1999-09-09 09:09:09.999"));
				break;

			case Types.OTHER:
				strbuf.append("setObject("+arg+", new BigInteger(666))");
				cs.setObject(arg, new BigInteger("666"));
				break;

			default:
				throw new Throwable("TEST ERROR: unexpected type "+type);
		}	
	}
	private static void callSetMethod(CallableStatement cs, int arg, int type, StringBuffer strbuf) throws Throwable
	{
		switch (type)	
		{
			case Types.BIT:
			case JDBC30Translation.SQL_TYPES_BOOLEAN:
				strbuf.append("setBoolean("+arg+", true)");
				cs.setBoolean(arg, true);
				break;

			case Types.TINYINT:
				strbuf.append("setByte("+arg+", 6)");
				cs.setByte(arg, (byte)6);
				break;

			case Types.SMALLINT:
				strbuf.append("setShort("+arg+", 66)");
				cs.setShort(arg, (short)66);
				break;

			case Types.INTEGER:
				strbuf.append("setInt("+arg+", 666)");
				cs.setInt(arg, 666);
				break;

			case Types.BIGINT:
				strbuf.append("setLong("+arg+", 666)");
				cs.setLong(arg, 666);
				break;

			case Types.FLOAT:
			case Types.REAL:
				strbuf.append("setFLoat("+arg+", 666)");
				cs.setFloat(arg, 666);
				break;

			case Types.DOUBLE:
				strbuf.append("setDouble("+arg+", 666)");
				cs.setDouble(arg, 666);
				break;

			case Types.DECIMAL:
			case Types.NUMERIC:
				strbuf.append("setBigDecimal("+arg+", 666.666)");
				BigDecimal bd = new BigDecimal("666.666");
				cs.setBigDecimal(arg, bd);
				break;

			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				strbuf.append("setString("+arg+", \"Set via setString()\")");
				cs.setString(arg, "Set via setString()");
				break;

			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				strbuf.append("setBytes("+arg+", byte[])");
				byte[] myarray = new byte[16];
				myarray[0] = (byte)255;
				cs.setBytes(arg, myarray);
				break;

			case Types.DATE:
				strbuf.append("setDate("+arg+", Date.valueOf(1999-09-09))");
				cs.setDate(arg, Date.valueOf("1999-09-09"));
				break;

			case Types.TIME:
				strbuf.append("setTime("+arg+", Time.valueOf(09:09:09))");
				cs.setTime(arg, Time.valueOf("09:09:09"));
				break;

			case Types.TIMESTAMP:
				strbuf.append("setTimestamp("+arg+", Timestamp.valueOf(1999-09-09 09:09:09.999))");
				cs.setTimestamp(arg, Timestamp.valueOf("1999-09-09 09:09:09.999"));
				break;

			case Types.OTHER:
				strbuf.append("setObject("+arg+", new BigInteger(666))");
				cs.setObject(arg, new BigInteger("666"));
				break;

			default:
				throw new Throwable("TEST ERROR: unexpected type "+type);
		}	
	}

	private static void callGetMethod(CallableStatement cs, int arg, int type, StringBuffer strbuf) throws Throwable
	{
		switch (type)	
		{
			case Types.BIT:
			case JDBC30Translation.SQL_TYPES_BOOLEAN:
				strbuf.append("getBoolean("+arg+") = ");
				strbuf.append(cs.getBoolean(arg));
				break;

			case Types.TINYINT:
				strbuf.append("getByte("+arg+") = ");
				strbuf.append(Byte.toString(cs.getByte(arg)));
				break;

			case Types.SMALLINT:
				strbuf.append("getShort("+arg+") = ");
				strbuf.append(Short.toString(cs.getShort(arg)));
				break;

			case Types.INTEGER:
				strbuf.append("getInt("+arg+") = ");
				strbuf.append(Integer.toString(cs.getInt(arg)));
				break;

			case Types.BIGINT:
				strbuf.append("getLong("+arg+") = ");
				strbuf.append(Long.toString(cs.getLong(arg)));
				break;

			case Types.FLOAT:
			case Types.REAL:
				strbuf.append("getFloat("+arg+") = ");
				strbuf.append(Float.toString(cs.getFloat(arg)));
				break;

			case Types.DOUBLE:
				strbuf.append("getDouble("+arg+") = ");
				strbuf.append(Double.toString(cs.getDouble(arg)));
				break;

			case Types.DECIMAL:
			case Types.NUMERIC:
				strbuf.append("getBigDecimal("+arg+") = ");
				BigDecimal bd = cs.getBigDecimal(arg, 2);
				strbuf.append(bd == null ? "null" : bd.toString());
				break;

			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				strbuf.append("getString("+arg+") = ");
				String s = cs.getString(arg);
				if (s.startsWith("[B@"))
					s = "byte[] reference";
				strbuf.append(s);
				break;

			case Types.BINARY:
			case Types.VARBINARY:
			case Types.LONGVARBINARY:
				strbuf.append("getBytes("+arg+") = ");
				byteArrayToString(cs.getBytes(arg), strbuf);
				break;

			case Types.DATE:
				strbuf.append("getDate("+arg+") = ");
				Date date = cs.getDate(arg);
				strbuf.append(date == null ? "null" : date.toString());
				break;

			case Types.TIME:
				strbuf.append("getTime("+arg+") = ");
				Time time = cs.getTime(arg);
				strbuf.append(time == null ? "null" : time.toString());
				break;

			case Types.TIMESTAMP:
				strbuf.append("getTimestamp("+arg+") = ");
				Timestamp timestamp = cs.getTimestamp(arg);
				strbuf.append(timestamp == null ? "null" : timestamp.toString());
				break;

			case Types.OTHER:
				strbuf.append("getObject("+arg+") = ");
				Object o = cs.getObject(arg);
				if (o == null)
				{
					strbuf.append("null");
				}
				else if (o instanceof byte[])
				{
					byteArrayToString((byte[])o, strbuf);
				}
				else
				{
					strbuf.append(o.toString());
				}

				break;

			default:
				throw new Throwable("TEST ERROR: unexpected type "+type);
		}	
	}

	static private void byteArrayToString(byte[] barray, StringBuffer strbuf)
	{
		if (barray == null)
		{
			strbuf.append("null");
		}
		else
		{
			for (int i = 0; i<barray.length; i++)
			{
				strbuf.append(barray[i]);
			}
		}
	}

	private static String getStringOfType(int type) throws Throwable
	{
		switch (type)
		{
			case Types.CHAR:
			case Types.VARCHAR:
			case Types.LONGVARCHAR:
				return "I am a string";

			case Types.TINYINT:
			case Types.SMALLINT:
			case Types.INTEGER:
			case Types.BIGINT:
			case Types.OTHER:		// other is bigInt
				return "3";

			case Types.FLOAT:
			case Types.REAL:
			case Types.DECIMAL:
			case Types.NUMERIC:
				return  "3.33";

			case Types.DATE:		
				return "1933-03-03";

			case Types.TIME:		
				return "03:03:03";

			case Types.TIMESTAMP:		
				return "1933-03-03 03:03:03.333";

			case Types.BINARY:		
			case Types.VARBINARY:		
			case Types.LONGVARBINARY:
				return "00680065006c006c006f";

			case Types.BIT:
			case JDBC30Translation.SQL_TYPES_BOOLEAN:
				return "true";

			default:
				throw new Throwable("bad type "+type);
		}	
	}

	/////////////////////////////////////////////////////////////
	//	
	// OUTPUT PARAMETER METHODS
	//
	/////////////////////////////////////////////////////////////
	public static void testNull(Boolean passedInNull, Boolean setToNull, Integer[] retval) throws Throwable
	{
		if (passedInNull.booleanValue())
		{
			if (retval[0] != null)
			{
				throw new Throwable("testNull() got a non-null param when it should have been null");
			}
		}

		retval[0] = (setToNull.booleanValue()) ? null : new Integer((short)66);
	}

	public static void testNullBug4317(String passedInNull) throws Throwable
	{
	}

	public static void takesNothing()
	{
	}
	public static void takesBytePrimitive(byte[] outparam, int type)
	{
		outparam[0]+=outparam[0];
	}
	public static void takesByte(Byte[] outparam, int type)
	{
		outparam[0] = new Byte((byte)(outparam[0] == null ? 33 : outparam[0].byteValue()*2));
	}

	public static void takesShortPrimitive(short[] outparam, int type)
	{
		outparam[0]+=outparam[0];
	}
	public static void takesShort(Short[] outparam, int type)
	{
		outparam[0] = new Short((byte)(outparam[0] == null ? 33 : outparam[0].shortValue()*2));
	}

	public static void takesIntegerPrimitive(int[] outparam, int type)
	{
		outparam[0]+=outparam[0];
	}
	public static void takesInteger(Integer[] outparam, int type)
	{
		outparam[0] = new Integer(outparam[0] == null ? 33 : outparam[0].intValue()*2);
	}

	public static void takesLongPrimitive(long[] outparam, int type)
	{
		outparam[0]+=outparam[0];
	}
	public static void takesLong(Long[] outparam, int type)
	{
		outparam[0] = new Long(outparam[0] == null ? 33 : outparam[0].longValue()*2);
	}

	public static void takesDoublePrimitive(double[] outparam, int type)
	{
		outparam[0]+=outparam[0];
	}
	public static void takesDouble(Double[] outparam, int type)
	{
		outparam[0] = new Double(outparam[0] == null ? 33 : outparam[0].doubleValue()*2);
	}

	public static void takesFloatPrimitive(float[] outparam, int type)
	{
		outparam[0]+=outparam[0];
	}
	public static void takesFloat(Float[] outparam, int type)
	{
		outparam[0] = new Float(outparam[0] == null ? 33 : outparam[0].floatValue()*2);
	}

	public static void takesBooleanPrimitive(boolean[] outparam, int type)
	{
		outparam[0] = true;
	}
	public static void takesBoolean(Boolean[] outparam, int type)
	{
		outparam[0] = new Boolean(true);
	}

	public static void takesBigDecimal(BigDecimal[] outparam, int type)
	{
		outparam[0] = (outparam[0] == null ? new BigDecimal("33") : outparam[0].add(outparam[0]));
		outparam[0].setScale(4, BigDecimal.ROUND_DOWN);
	}

	public static void takesByteArray(byte[][] outparam, int type)
	{
		byte[] myarray = new byte[16];
		myarray[0] = (byte)255;
		outparam[0] = myarray;
	}

	public static void takesDate(Date[] outparam, int type)
	{
		outparam[0] = Date.valueOf("1966-06-06");
	}

	public static void takesTime(Time[] outparam, int type)
	{
		outparam[0] = Time.valueOf("06:06:06");
	}

	public static void takesTimestamp(Timestamp[] outparam, int type)
	{
		outparam[0] = Timestamp.valueOf("1966-06-06 06:06:06.666");
	}

	public static void takesString(String[] outparam, int type) throws Throwable
	{
		outparam[0] = getStringOfType(type);
	}

	public static void takesBigInteger(BigInteger[] outparam, int type)
	{
		outparam[0] = (outparam[0] == null ? new BigInteger("33") : outparam[0].add(outparam[0]));
	}
	
	
	/////////////////////////////////////////////////////////////
	//	
	// RETURN PARAMETER METHODS
	//
	/////////////////////////////////////////////////////////////
	public static void returnsNothing()
	{
	}

	public static byte returnsByteP(int type)
	{
		return 66;
	}
	public static Byte returnsByte(int type)
	{
		return new Byte((byte)66);
	}

	public static short returnsShortP(int type)
	{
		return 666;
	}
	public static Short returnsShort(int type)
	{
		return new Short((short)666);
	}

	public static int returnsIntegerP(int type)
	{
		return 666;
	}
	public static Integer returnsInteger(int type)
	{
		return new Integer(666);
	}

	public static long returnsLongP(int type)
	{
		return 666;
	}
	public static Long returnsLong(int type)
	{
		return new Long(666);
	}

	public static float returnsFloatP(int type)
	{
		return 666;
	}
	public static Float returnsFloat(int type)
	{
		return new Float(666);
	}

	public static double returnsDoubleP(int type)
	{
		return 666;
	}
	public static Double returnsDouble(int type)
	{
		return new Double(666);
	}


	public static BigDecimal returnsBigDecimal(int type)
	{
		return new BigDecimal(666d);
	}

	public static byte[] returnsByteArray(int type)
	{
		byte[] myarray = new byte[16];
		myarray[0] = (byte)255;
		return myarray;
	}

	public static String returnsString(int type) throws Throwable
	{
		return getStringOfType(type);
	}

	public static Date returnsDate(int type)
	{
		return Date.valueOf("1966-06-06");
	}

	public static Time returnsTime(int type)
	{
		return Time.valueOf("06:06:06");
	}

	public static Timestamp returnsTimestamp(int type)
	{
		return Timestamp.valueOf("1966-06-06 06:06:06.666");
	}

	public static BigInteger returnsBigInteger(int type)
	{
		return new BigInteger("666");
	}


	// these come from the performance test JDBC.Parameters that was failing
	private static void testManyOut(Connection conn) throws SQLException {

		System.out.println("start testManyOut");

		Statement scp = conn.createStatement();

		scp.execute("CREATE PROCEDURE OP_OUT " +
			"(OUT I1 INT, OUT I2 INT, OUT I3 INT, OUT I4 INT, OUT I5 INT, "+
			"OUT V1 VARCHAR(40), OUT V2 VARCHAR(40), OUT V3 VARCHAR(40), OUT V4 VARCHAR(40), OUT V5 VARCHAR(40)) "+

			"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.output' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


		scp.execute("CREATE PROCEDURE OP_INOUT " +
			"(INOUT I1 INT, INOUT I2 INT, INOUT I3 INT, INOUT I4 INT, INOUT I5 INT, " +
			"INOUT V1 VARCHAR(40), INOUT V2 VARCHAR(40), INOUT V3 VARCHAR(40), INOUT V4 VARCHAR(40), INOUT V5 VARCHAR(40)) " +

			"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.output' NO SQL LANGUAGE JAVA PARAMETER STYLE JAVA");


		CallableStatement csOut_cs = conn.prepareCall("CALL OP_OUT(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
		CallableStatement csInOut_cs = conn.prepareCall("CALL OP_INOUT(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

		System.out.println("Ten OUT parameters");

		executeOutput(csOut_cs);
		executeOutput(csOut_cs);

		csOut_cs.close();

		System.out.println("Ten INOUT parameters");


		setupInput(csInOut_cs);
		executeOutput(csInOut_cs);

		setupInput(csInOut_cs);
		executeOutput(csInOut_cs);

		csInOut_cs.close();

		scp.execute("DROP PROCEDURE OP_OUT");
		scp.execute("DROP PROCEDURE OP_INOUT");
		scp.close();


		System.out.println("end testManyOut");


	}


	private static void setupInput(PreparedStatement ps) throws SQLException {

		ps.setInt(1, 0);
		ps.setInt(2, 0);
		ps.setInt(3, 99);
		ps.setInt(4, 103);
		ps.setInt(5, 1456);

		ps.setNull(6, Types.CHAR);
		ps.setString(7, null);
		ps.setString(8, "hello");
		ps.setString(9, "goodbye");
		ps.setString(10, "welcome");
	}
	private static void executeOutput(CallableStatement cs) throws SQLException {

		for (int p = 1; p <= 5; p++)
			cs.registerOutParameter(p, Types.INTEGER);

		for (int p = 6; p <= 10; p++)
			cs.registerOutParameter(p, Types.VARCHAR);

		cs.execute();

		for (int p = 1; p <= 5; p++) {
			System.out.println("  " + p + " = " + cs.getInt(p) + " was null " + cs.wasNull());
	
		}
		for (int p = 6; p <= 10; p++) {
			System.out.println("  " + p + " = " + cs.getString(p) + " was null " + cs.wasNull());
		}
	}


	public static void output(int[] a1, int[] a2, int[] a3, int[] a4, int[] a5,
		String[] s1, String[] s2, String[] s3, String[] s4, String[] s5) {

		System.out.println("  a1 = " + a1[0]);
		System.out.println("  a2 = " + a2[0]);
		System.out.println("  a3 = " + a3[0]);
		System.out.println("  a4 = " + a4[0]);
		System.out.println("  a5 = " + a5[0]);

		System.out.println("  s1 = " + s1[0]);
		System.out.println("  s2 = " + s2[0]);
		System.out.println("  s3 = " + s3[0]);
		System.out.println("  s4 = " + s4[0]);
		System.out.println("  s5 = " + s5[0]);

		a1[0] = 0;
		a2[0] = 0;
		a3[0] = 77;
		a4[0] = 4;
		a5[0] = 2003;

		s1[0] = null;
		s2[0] = null;
		s3[0] = "cloudscape";
		s4[0] = "jbms";
		s5[0] = "IBM CS";
	}

	private static void test5116(Connection conn) throws Throwable
	{
		System.out.println("==============================================");
		System.out.println("TESTING FIX OF 5116 -- VAR BIT VARYING INPUT");
		System.out.println("==============================================\n");

		Statement stmt = conn.createStatement();
		stmt.executeUpdate("CREATE TABLE ACTIVITY_INSTANCE_T (" +
    "AIID                               char(16) for bit data              NOT NULL ," +
    "KIND                               INTEGER                            NOT NULL ," +
    "PIID                               char(16) for bit data              NOT NULL ," +
    "PTID                               char(16) for bit data              NOT NULL ," +
    "ATID                               char(16) for bit data              NOT NULL ," +
    "RUN_MODE                           INTEGER                            NOT NULL ," +
    "FINISHED                           TIMESTAMP                                   ," +
    "ACTIVATED                          TIMESTAMP                                   ," +
    "STARTED                            TIMESTAMP                                   ," +
    "LAST_MODIFIED                      TIMESTAMP                                   ," +
    "LAST_STATE_CHANGE                  TIMESTAMP                                   ," +
    "STATE                              INTEGER                            NOT NULL ," +
    "TRANS_COND_VALUES                  VARCHAR(66) FOR BIT DATA           NOT NULL ," +
    "NUM_CONN_ACT_EVA                   INTEGER                            NOT NULL ," +
    "NUMBER_OF_ITERATIONS               INTEGER                            NOT NULL ," +
    "NUMBER_OF_RETRIES                  INTEGER                            NOT NULL ," +
    "HAS_CUSTOM_ATTRIBUTES              SMALLINT                           NOT NULL ," +
    "NON_BLOCK_PTID                     char(16) for bit data              NOT NULL ," +
    "NON_BLOCK_PIID                     char(16) for bit data              NOT NULL ," +
    "EXPIRES                            TIMESTAMP                                   ," +
    "TASK_ID                            VARCHAR(254)                                ," +
    "UNHANDLED_EXCEPTION                BLOB(3993600)                       ," +
    "SUB_PROCESS_PIID                   char(16) for bit data                                    ," +
    "OWNER                              VARCHAR(32)                                 ," +
    "USER_INPUT                         VARCHAR(130) FOR BIT DATA                   ," +
    "DESCRIPTION                        VARCHAR(254)                                ," +
    "VERSION_ID                         SMALLINT                           NOT NULL ," +
    "PRIMARY KEY ( AIID ) )");

		stmt.execute("CREATE PROCEDURE doInsertion(IN P1 VARCHAR(2) FOR BIT DATA) " +
						"EXTERNAL NAME 'org.apache.derbyTesting.functionTests.tests.lang.outparams.doInsertion'" +
						" MODIFIES SQL DATA LANGUAGE JAVA PARAMETER STYLE JAVA");

		CallableStatement cs = conn.prepareCall("call doInsertion (?)");
		cs.setNull(1, java.sql.Types.VARBINARY);
		cs.execute();
        byte [] b = new byte[2];
        b[0]=1; b[1] = 2;
        cs.setBytes( 1, b );
		cs.execute();
		cs.close();
		stmt.executeUpdate("DROP PROCEDURE doInsertion");
		stmt.close();
	}

	public static void doInsertion (byte[] p25) throws Throwable
	{
		Connection connNested = DriverManager.getConnection("jdbc:default:connection");
		Statement stmt = connNested.createStatement();
		stmt.executeUpdate("delete from ACTIVITY_INSTANCE_T");

        String strStmt = "INSERT INTO ACTIVITY_INSTANCE_T VALUES( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,? )";
        PreparedStatement pstmt = connNested.prepareStatement( strStmt );

        byte [] b = new byte[2];
        b[0]=1;
		byte[] b2 = new byte[1];
		b2[0] = 0;

         pstmt.setBytes( 1, b ); //ids
         pstmt.setInt( 2, 0);
         pstmt.setBytes( 3, b );
         pstmt.setBytes( 4, b );
         pstmt.setBytes( 5, b );
         pstmt.setInt( 6, 0);
         pstmt.setNull( 7, java.sql.Types.TIMESTAMP);
         pstmt.setNull( 8, java.sql.Types.TIMESTAMP);
         pstmt.setNull( 9, java.sql.Types.TIMESTAMP);
         pstmt.setNull( 10, java.sql.Types.TIMESTAMP);
         pstmt.setNull( 11, java.sql.Types.TIMESTAMP);
         pstmt.setInt( 12, 0);
         pstmt.setBytes( 13, b );

         pstmt.setInt( 14, 0);
         pstmt.setInt( 15, 0);
         pstmt.setInt( 16, 0);
         pstmt.setBoolean( 17, false);
         pstmt.setBytes( 18, b );
         pstmt.setBytes( 19, b );
         pstmt.setNull( 20, java.sql.Types.TIMESTAMP);
         pstmt.setNull( 21, java.sql.Types.VARCHAR);
         pstmt.setNull( 22, java.sql.Types.BLOB );
         pstmt.setNull( 23, java.sql.Types.VARBINARY );
         pstmt.setNull( 24, java.sql.Types.VARCHAR);
		if (p25 == null)
             pstmt.setNull( 25, java.sql.Types.VARBINARY);
		else
			 pstmt.setBytes(25, p25);
         pstmt.setNull( 26, java.sql.Types.VARCHAR);
         pstmt.setShort( 27, (short) 0);
         pstmt.executeUpdate();
         pstmt.close();

         pstmt = connNested.prepareStatement( "SELECT version_id, user_input FROM activity_instance_t");
         ResultSet resultSet = pstmt.executeQuery();
         System.out.println("Executed query");
         while( resultSet.next() )
         {
            System.out.println("i= " + resultSet.getInt(1) );
            byte [] userInput = resultSet.getBytes(2);
            if( userInput == null || resultSet.wasNull() )
            {
				if( userInput == null)
               		System.out.println("UserInput = null");
				if (resultSet.wasNull())
               		System.out.println("resultSet wasNull");
            }
            else
            {
               System.out.println("UserInput length  = " + userInput.length + " bytes");
               for( int i=0; i<userInput.length; i++ )
               {
                  System.out.println( i + ") = " + userInput[i] );
               }
            }
         }
         System.out.println("Close result set.");
         resultSet.close();
         pstmt.close();
         stmt.close();
		 connNested.close();
   }
}

