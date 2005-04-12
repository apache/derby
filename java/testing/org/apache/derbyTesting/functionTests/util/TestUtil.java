/*

   Derby - Class org.apache.derbyTesting.functionTests.util.TestUtil

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


package org.apache.derbyTesting.functionTests.util;

import java.sql.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Properties;
import org.apache.derby.iapi.reference.JDBC30Translation;


/**
	Utility methods for tests, in order to bring some consistency to test
	output and handle testing framework differences

*/
public class TestUtil {

	public static final int UNKNOWN_FRAMEWORK = -1;

	/**
	   framework = embedded (or null) jdbc:derby:
	*/
	public static final int EMBEDDED_FRAMEWORK = 0;
	
	/**
	   framework = DerbyNet for JCC  jdbc:derby:net:
	*/
	public static final int DERBY_NET_FRAMEWORK = 1;

	/**
	   framework = DB2JCC  for testing JCC against DB2 for 
	   debugging jcc problems jdbc:db2://
	*/

	public  static final int DB2JCC_FRAMEWORK = 2; // jdbc:db2//
	
	/**
	   framework = DerbyNetClient  for Derby cient  jdbc:derby://
	*/
	public static final int DERBY_NET_CLIENT_FRAMEWORK = 3; // jdbc:derby://


	/**
	   framework = DB2jNet 
	   OLD_NET_FRAMEWORK is for tests that have not yet been contributed.
	   it can be removed once all tests are at apache
	*/
	public  static final int OLD_NET_FRAMEWORK = 4;          // jdbc:derby:net:


	private static int framework = UNKNOWN_FRAMEWORK;


	// DataSource Type strings used to build up datasource names.
	// e.g. "Embed" + XA_DATASOURCE_STRING + "DataSource
	private static String XA_DATASOURCE_STRING = "XA";
	private static String CONNECTION_POOL_DATASOURCE_STRING = "ConnectionPool";
	private static String REGULAR_DATASOURCE_STRING = "";
	
	// Methods for making framework dependent decisions in tests.

	/**
	 * Is this a network testingframework? 
	 * return true if the System Property framework is set to Derby Network
	 * client or JCC
	 *
	 * @return true if this is a Network Server test
	 */
	public static boolean isNetFramework()
	{
		framework = getFramework();
		switch (framework)
		{
			case DERBY_NET_FRAMEWORK:
			case DERBY_NET_CLIENT_FRAMEWORK:
			case DB2JCC_FRAMEWORK:
			case OLD_NET_FRAMEWORK:
				return true;
			default:
				return false;
		}
	}
			
	/** 
		Is the JCC driver being used
	  
		@return true for JCC driver
	*/
	public static boolean isJCCFramework()
	{
		int framework = getFramework();
		switch (framework)
		{
			case DERBY_NET_FRAMEWORK:
			case DB2JCC_FRAMEWORK:
			case OLD_NET_FRAMEWORK:
				return true;
		}
		return false;
	}

	public static boolean isDerbyNetClientFramework()
	{
		return (getFramework() == DERBY_NET_CLIENT_FRAMEWORK);
	}

	public static boolean isEmbeddedFramework()
	{
		return (getFramework() == EMBEDDED_FRAMEWORK);
	}

	/**
	   Get the framework from the System Property framework
	   @return  constant for framework being used
	       TestUtil.EMBEDDED_FRAMEWORK  for embedded
		   TestUtil.DERBY_NET_CLIENT_FRAMEWORK  for Derby Network Client 
		   TestUtil.DERBY_NET_FRAMEWORK for JCC to Network Server
		   TestUtil.DB2JCC_FRAMEWORK for JCC to DB2
	*/
	private static int getFramework()
	{
		if (framework != UNKNOWN_FRAMEWORK)
			return framework;
		String frameworkString = System.getProperty("framework");
		if (frameworkString == null || 
		   frameworkString.toUpperCase(Locale.ENGLISH).equals("EMBEDDED"))
			framework = EMBEDDED_FRAMEWORK;
		else if (frameworkString.toUpperCase(Locale.ENGLISH).equals("DERBYNETCLIENT"))
			framework = DERBY_NET_CLIENT_FRAMEWORK;
		else if (frameworkString.toUpperCase(Locale.ENGLISH).equals("DERBYNET"))
			framework = DERBY_NET_FRAMEWORK;
		else if (frameworkString.toUpperCase(Locale.ENGLISH).indexOf("DB2JNET") != -1)
			framework = OLD_NET_FRAMEWORK;

		return framework;

	}

	/**
	    Get URL prefix for current framework.
		
		@return url, assume localhost and port 1527 for Network Tests
		@see getJdbcUrlPrefix(String server, int port)
		
	*/
	public static String getJdbcUrlPrefix()
	{
		return getJdbcUrlPrefix("localhost", 1527);
	}

	/** 
		Get URL prefix for current framework		
		
		@param server  host to connect to with client driver 
		               ignored for embedded driver
		@param port    port to connect to with client driver
		               ignored with embedded driver
		@return URL prefix
		        EMBEDDED_FRAMEWORK returns "jdbc:derby"
				DERBY_NET_FRAMEWORK = "jdbc:derby:net://<server>:port/"
				DERBY_NET_CLIENT_FRAMEWORK = "jdbc:derby://<server>:port/"
				DB2_JCC_FRAMEWORK = "jdbc:db2://<server>:port/"
	*/
	public static String getJdbcUrlPrefix(String server, int port)
	{
		int framework = getFramework();
		switch (framework)
		{
			case EMBEDDED_FRAMEWORK:
				return "jdbc:derby:";
			case DERBY_NET_FRAMEWORK:
			case OLD_NET_FRAMEWORK:								
				return "jdbc:derby:net://" + server + ":" + port + "/";
			case DERBY_NET_CLIENT_FRAMEWORK:
				return "jdbc:derby://" + server + ":" + port + "/";
			case DB2JCC_FRAMEWORK:				
				return "jdbc:db2://" + server + ":" + port + "/";
		}
		// Unknown framework
		return null;
		
	}

	/**
	   Load the appropriate driver for the current framework
	*/
	public static void loadDriver() throws Exception
	{
		String driverName = null;
		framework = getFramework();
		switch (framework)
		{
			case EMBEDDED_FRAMEWORK:
				driverName =  "org.apache.derby.jdbc.EmbeddedDriver";
				break;
			case DERBY_NET_FRAMEWORK:
			case OLD_NET_FRAMEWORK:				
			case DB2JCC_FRAMEWORK:				
				driverName = "com.ibm.db2.jcc.DB2Driver";
				break;
			case DERBY_NET_CLIENT_FRAMEWORK:
				driverName = "org.apache.derby.jdbc.ClientDriver";
				break;
		}
		Class.forName(driverName).newInstance();
	}


	/**
	 * Get a data source for the appropriate framework
	 * @param attrs  A set of attribute values to set on the datasource.
	 *                The appropriate setter method wil b
	 *                For example the property databaseName with value wombat,
	 *                will mean ds.setDatabaseName("wombat") will be called
	 *  @return datasource for current framework
	 */
	public static javax.sql.DataSource getDataSource(Properties attrs)
	{
		
		String classname = getDataSourcePrefix() + REGULAR_DATASOURCE_STRING + "DataSource";
		return (javax.sql.DataSource) getDataSourceWithReflection(classname, attrs);
	}

	/**
	 * Get an xa  data source for the appropriate framework
	 * @param attrs  A set of attribute values to set on the datasource.
	 *                The appropriate setter method wil b
	 *                For example the property databaseName with value wombat,
	 *                will mean ds.setDatabaseName("wombat") will be called
	 *  @return datasource for current framework
	 */
	public static javax.sql.XADataSource getXADataSource(Properties attrs)
	{
		
		String classname = getDataSourcePrefix() + XA_DATASOURCE_STRING + "DataSource";
		return (javax.sql.XADataSource) getDataSourceWithReflection(classname, attrs);
	}

	
	/**
	 * Get a ConnectionPoolDataSource  for the appropriate framework
	 * @param attrs  A set of attribute values to set on the datasource.
	 *                The appropriate setter method wil b
	 *                For example the property databaseName with value wombat,
	 *                will mean ds.setDatabaseName("wombat") will be called
	 *  @return datasource for current framework
	 */
	public static javax.sql.ConnectionPoolDataSource getConnectionPoolDataSource(Properties attrs)
	{
		String classname = getDataSourcePrefix() + CONNECTION_POOL_DATASOURCE_STRING + "DataSource";
		return (javax.sql.ConnectionPoolDataSource) getDataSourceWithReflection(classname, attrs);
	}

	public static String getDataSourcePrefix()
		{
			framework = getFramework();
			switch(framework)
			{
				case OLD_NET_FRAMEWORK:
				case DERBY_NET_FRAMEWORK:
				case DB2JCC_FRAMEWORK:
					return "com.ibm.db2.jcc.DB2";
				case DERBY_NET_CLIENT_FRAMEWORK:
					return "org.apache.derby.jdbc.Client";
				case EMBEDDED_FRAMEWORK:
					return "org.apache.derby.jdbc.Embedded";
				default:
					Exception e = new Exception("FAIL: No DataSource Prefix for framework: " + framework);
					e.printStackTrace();
			}
			return null;
		}



	static private Class[] STRING_ARG_TYPE = {String.class};
	static private Class[] INT_ARG_TYPE = {Integer.TYPE};
	static private Class[] BOOLEAN_ARG_TYPE = { Boolean.TYPE };
	// A hashtable of special non-string attributes.
	private static Hashtable specialAttributes = null;
	

	private static Object getDataSourceWithReflection(String classname, Properties attrs)
	{
		Object[] args = null;
		Object ds = null;
		Method sh = null;
		String methodName = null;
		
		if (specialAttributes == null)
		{
			specialAttributes = new Hashtable();
			specialAttributes.put("portNumber",INT_ARG_TYPE);
			specialAttributes.put("driverType",INT_ARG_TYPE);
			specialAttributes.put("retrieveMessagesFromServerOnGetMessage",
								  BOOLEAN_ARG_TYPE);
			specialAttributes.put("retrieveMessageText",
								  BOOLEAN_ARG_TYPE);
		}
		
		try {
		ds  = Class.forName(classname).newInstance();

		for (Enumeration propNames = attrs.propertyNames(); 
			 propNames.hasMoreElements();)
		{
			String key = (String) propNames.nextElement();
			Class[] argType = (Class[]) specialAttributes.get(key);
			if (argType == null) 
				argType = STRING_ARG_TYPE;
			String value = attrs.getProperty(key);
			if (argType  == INT_ARG_TYPE)
			{
				args = new Integer[] 
				{ new Integer(Integer.parseInt(value)) };	
			}
			else if (argType  == BOOLEAN_ARG_TYPE)
			{
				args = new Boolean[] { new Boolean(value) };	
			}
			else if (argType == STRING_ARG_TYPE)
			{
				args = new String[] { value };
			}
			else  // No other property types supported right now
			{
				throw new Exception("FAIL: getDataSourceWithReflection: Argument type " + argType[0].getName() +  " not supportted for attribute: " +
									" key:" + key + " value:" +value);
			   
			}
			methodName = getSetterName(key);

			
			// Need to use reflection to load indirectly
			// setDatabaseName
			sh = ds.getClass().getMethod(methodName, argType);
			sh.invoke(ds, args);
		}

		} catch (Exception e)
		{
			System.out.println("Error accessing method " + methodName);
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		return ds;
	}

	
	public static String  getSetterName(String attribute)
	{
		return "set" + Character.toUpperCase(attribute.charAt(0)) + attribute.substring(1);
	}

	
	public static String  getGetterName(String attribute)
	{
		return "get" + Character.toUpperCase(attribute.charAt(0)) + attribute.substring(1);
	}

	// Some methods for test output.
	public static void dumpSQLExceptions(SQLException sqle) {
		TestUtil.dumpSQLExceptions(sqle, false);
	}

	public static void dumpSQLExceptions(SQLException sqle, boolean expected) {
		String prefix = "";
		if (!expected) {
			System.out.println("FAIL -- unexpected exception ****************");
		}
		else
		{
			prefix = "EXPECTED ";
		}

		do
		{
			System.out.println(prefix + "SQLSTATE("+sqle.getSQLState()+"): " + sqle.getMessage());
			sqle = sqle.getNextException();
		} while (sqle != null);
	}


	  public static String sqlNameFromJdbc(int jdbcType) {
		switch (jdbcType) {
			case Types.BIT 		:  return "Types.BIT";
			case JDBC30Translation.SQL_TYPES_BOOLEAN  : return "Types.BOOLEAN";
			case Types.TINYINT 	:  return "Types.TINYINT";
			case Types.SMALLINT	:  return "SMALLINT";
			case Types.INTEGER 	:  return "INTEGER";
			case Types.BIGINT 	:  return "BIGINT";

			case Types.FLOAT 	:  return "Types.FLOAT";
			case Types.REAL 	:  return "REAL";
			case Types.DOUBLE 	:  return "DOUBLE";

			case Types.NUMERIC 	:  return "Types.NUMERIC";
			case Types.DECIMAL	:  return "DECIMAL";

			case Types.CHAR		:  return "CHAR";
			case Types.VARCHAR 	:  return "VARCHAR";
			case Types.LONGVARCHAR 	:  return "LONG VARCHAR";
            case Types.CLOB     :  return "CLOB";

			case Types.DATE 		:  return "DATE";
			case Types.TIME 		:  return "TIME";
			case Types.TIMESTAMP 	:  return "TIMESTAMP";

			case Types.BINARY			:  return "CHAR () FOR BIT DATA";
			case Types.VARBINARY	 	:  return "VARCHAR () FOR BIT DATA";
			case Types.LONGVARBINARY 	:  return "LONG VARCHAR FOR BIT DATA";
            case Types.BLOB             :  return "BLOB";

			case Types.OTHER		:  return "Types.OTHER";
			case Types.NULL		:  return "Types.NULL";
			default : return String.valueOf(jdbcType);
		}
	}
	  public static String jdbcNameFromJdbc(int jdbcType) {
		switch (jdbcType) {
			case Types.BIT 		:  return "Types.BIT";
			case JDBC30Translation.SQL_TYPES_BOOLEAN  : return "Types.BOOLEAN";
			case Types.TINYINT 	:  return "Types.TINYINT";
			case Types.SMALLINT	:  return "Types.SMALLINT";
			case Types.INTEGER 	:  return "Types.INTEGER";
			case Types.BIGINT 	:  return "Types.BIGINT";

			case Types.FLOAT 	:  return "Types.FLOAT";
			case Types.REAL 	:  return "Types.REAL";
			case Types.DOUBLE 	:  return "Types.DOUBLE";

			case Types.NUMERIC 	:  return "Types.NUMERIC";
			case Types.DECIMAL	:  return "Types.DECIMAL";

			case Types.CHAR		:  return "Types.CHAR";
			case Types.VARCHAR 	:  return "Types.VARCHAR";
			case Types.LONGVARCHAR 	:  return "Types.LONGVARCHAR";
            case Types.CLOB     :  return "Types.CLOB";

			case Types.DATE 		:  return "Types.DATE";
			case Types.TIME 		:  return "Types.TIME";
			case Types.TIMESTAMP 	:  return "Types.TIMESTAMP";

			case Types.BINARY			:  return "Types.BINARY";
			case Types.VARBINARY	 	:  return "Types.VARBINARY";
			case Types.LONGVARBINARY 	:  return "Types.LONGVARBINARY";
            case Types.BLOB             :  return "Types.BLOB";

			case Types.OTHER		:  return "Types.OTHER";
			case Types.NULL		:  return "Types.NULL";
			default : return String.valueOf(jdbcType);
		}
	}

	/*** Some routines for printing test information to html  **/

	public static String TABLE_START_TAG =  "<TABLE border=1 cellspacing=1 cellpadding=1  bgcolor=white  style='width:100%'>";
	public static String TABLE_END_TAG = "</TABLE>";
	public static String TD_INVERSE =
		"<td  valign=bottom align=center style=background:#DADADA;  padding:.75pt .75pt .75pt .75pt'> <p class=MsoNormal style='margin-top:6.0pt;margin-right:0in;margin-bottom:  6.0pt;margin-left:0in'><b><span style='font-size:8.5pt;font-family:Arial;  color:black'>";

	public static String TD_CENTER = "<TD valign=center align=center> <p class=MsoNormal style='margin-top:6.0pt;margin-right:0in;margin-bottom:6.0pt;margin-left:0in'><b><span style='font-size:8.5pt;font-family:Arial;  color:black'>";

	public static String TD_LEFT = "<TD valign=center align=left> <p class=MsoNormal style='margin-top:6.0pt;margin-right:0in;margin-bottom:6.0pt;margin-left:0in'><b><span style='font-size:8.5pt;font-family:Arial;  color:black'>";

	
	public static String TD_END = "</SPAN></TD>";

	public static String END_HTML_PAGE="</BODY> </HTML>";
	

	public static void startHTMLPage(String title, String author)
	{
		System.out.println("<HTML> \n <HEAD>");
		System.out.println(" <meta http-equiv=\"Content-Type\"content=\"text/html; charset=iso-8859-1\">");
		System.out.println("<meta name=\"Author\" content=\"" + author +  "\">");
		System.out.println("<title>" + title + "</title>");
		System.out.println("</HEAD> <BODY>");
		System.out.println("<H1>" + title + "</H1>");
	}

	public static void endHTMLPage()
	{
		System.out.println(END_HTML_PAGE);
	}

/*	public static void main(String[] argv)
	{
		testBoolArrayToHTMLTable();
	}
*/

	/** 
	 * Converts 2 dimensional boolean array into an HTML table.
	 * used by casting.java to print out casting doc
	 *
	 * @param rowLabels   - Row labels
	 * @param colLabels   - Column labels
	 **/
	public static void printBoolArrayHTMLTable(String rowDescription,
											   String columnDescription,
											   String [] rowLabels, 
											   String [] colLabels,
											   boolean[][] array,
											   String tableInfo)
	{

		System.out.println("<H2>" + tableInfo + "</H2>");

		System.out.println(TABLE_START_TAG);
		System.out.println("<TR>");
		// Print corner with labels
		System.out.println(TD_INVERSE +columnDescription + "---><BR><BR><BR><BR><BR>");
		System.out.println("<---" +rowDescription);
		System.out.println(TD_END);

		
		// Print column headers
		for (int i = 0; i < colLabels.length; i++)
		{
			System.out.println(TD_INVERSE);
			for (int c = 0; c < colLabels[i].length() && c < 20; c++)
			{
				System.out.println(colLabels[i].charAt(c) + "<BR>");
			}
			System.out.println(TD_END);
		}

		System.out.println("</TR>");

		// Print the Row Labels and Data
		for (int i = 0; i < rowLabels.length; i ++)
		{
			System.out.println("<TR>");
			System.out.println(TD_LEFT);
			System.out.println("<C> " +  rowLabels[i] + "</C>");
			System.out.println(TD_END);

			for (int j = 0; j < colLabels.length; j ++)
			{
				System.out.println(TD_CENTER);
				System.out.println((array[i][j]) ? "Y" : "-");
				System.out.println(TD_END);
			}
			System.out.println("</TR>");
		}


		System.out.println(TABLE_END_TAG);
		System.out.println("<P><P>");

	}

	/**
	 * Just converts a string to a hex literal to assist in converting test
	 * cases that used to insert strings into bit data tables
	 * Converts using UTF-16BE just like the old casts used to.
	 *
	 * @ param s  String to convert  (e.g
	 * @ resturns hex literal that can be inserted into a bit column.
	 */
	public static String stringToHexLiteral(String s)
	{
		byte[] bytes;
		String hexLiteral = null;
		try {
			bytes = s.getBytes("UTF-16BE");
			hexLiteral = convertToHexString(bytes);
		}
		catch (UnsupportedEncodingException ue)
		{
			System.out.println("This shouldn't happen as UTF-16BE should be supported");
			ue.printStackTrace();
		}

		return hexLiteral;
	}

	private static String convertToHexString(byte [] buf)
	{
		StringBuffer str = new StringBuffer();
		str.append("X'");
		String val;
		int byteVal;
		for (int i = 0; i < buf.length; i++)
		{
			byteVal = buf[i] & 0xff;
			val = Integer.toHexString(byteVal);
			if (val.length() < 2)
				str.append("0");
			str.append(val);
		}
		return str.toString() +"'";
	}



	/**
		Get the JDBC version, inferring it from the driver.
		We cannot use the JDBC DatabaseMetaData method
		as it is not present in JDBC 2.0.
	*/

	public static int getJDBCMajorVersion(Connection conn)
	{
		try {
			conn.getClass().getMethod("setSavepoint", null);
			return 3;
		} catch (NoSuchMethodException e) {
			return 2;
		} catch (NoClassDefFoundError e2) {
			return 2;
		}

	}


}



