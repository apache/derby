/*

   Derby - Class org.apache.derbyTesting.functionTests.util.TestUtil

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


package org.apache.derbyTesting.functionTests.util;

import java.sql.*;
import java.io.*;
import java.lang.reflect.*;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Locale;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.NoSuchElementException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;
import java.security.PrivilegedActionException;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;

import org.apache.derbyTesting.functionTests.harness.JavaVersionHolder;
import org.apache.derbyTesting.functionTests.harness.RunTest;




/**
	Utility methods for tests, in order to bring some consistency to test
	output and handle testing framework differences

*/
public class TestUtil {
	
	//Used for JSR169
	public static boolean HAVE_DRIVER_CLASS;
	static{
//IC see: https://issues.apache.org/jira/browse/DERBY-398
//IC see: https://issues.apache.org/jira/browse/DERBY-597
		try{
			Class.forName("java.sql.Driver");
			HAVE_DRIVER_CLASS = true;
		}
		catch(ClassNotFoundException e){
			//Used for JSR169
			HAVE_DRIVER_CLASS = false;
		}
	}

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
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
              String frameworkString = AccessController.doPrivileged
                  (new PrivilegedAction<String>() {
                          public String run() {
                              return System.getProperty("framework");
                          }
                      }
                   );              
		// last attempt to get useprocess to do networkserver stuff.
		// If a suite has useprocess, it's possible there was no property set.
//IC see: https://issues.apache.org/jira/browse/DERBY-1141
		if (frameworkString == null)
		{
		   String useprocessFramework = RunTest.framework;
		   if (useprocessFramework != null)
			frameworkString = useprocessFramework;
		}
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
		
		@return url, assume localhost - unless set differently in System property - 
//IC see: https://issues.apache.org/jira/browse/DERBY-413
		             and assume port 1527 for Network Tests
		@see #getJdbcUrlPrefix(String server, int port)
		
	*/
    public static String getJdbcUrlPrefix()
    {
        String hostName=getHostName();
        return getJdbcUrlPrefix(hostName, 1527);
    }

    /** Get hostName as passed in - if not, set it to "localhost" 
        @return hostName, as passed into system properties, or "localhost"
    */
    public static String getHostName()
    {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
        String hostName = AccessController.doPrivileged
            (new PrivilegedAction<String>() {
                    public String run() {
                        return System.getProperty("hostName");
                    }
                }
             );    
        if (hostName == null)
            hostName="localhost";
        return hostName;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-918
//IC see: https://issues.apache.org/jira/browse/DERBY-934
//IC see: https://issues.apache.org/jira/browse/DERBY-797
              final String driverName;
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
//IC see: https://issues.apache.org/jira/browse/DERBY-918
//IC see: https://issues.apache.org/jira/browse/DERBY-934
//IC see: https://issues.apache.org/jira/browse/DERBY-797
                      default: 
                            driverName=  "org.apache.derby.jdbc.EmbeddedDriver";
                            break;
		}
                                
              try {
                  AccessController.doPrivileged
                      (new PrivilegedExceptionAction<Void>() {
                              public Void run() throws Exception {
                                  Class<?> clazz = Class.forName(driverName);
                                  clazz.getConstructor().newInstance();
                                  return null;
                              }
                          }
                       );
              } catch (PrivilegedActionException e) {
                  throw e.getException();
              }
        }


	/**
	 * Get a data source for the appropriate framework
	 * @param attrs  A set of attribute values to set on the datasource.
	 *                The appropriate setter method wil b
	 *                For example the property databaseName with value wombat,
	 *                will mean ds.setDatabaseName("wombat") will be called
	 *  @return datasource for current framework
	 */
    public static DataSource getDataSource(Properties attrs)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        String classname =
            getDataSourcePrefix() + REGULAR_DATASOURCE_STRING + "DataSource40";
        return (DataSource) getDataSourceWithReflection(classname, attrs);
	}

	/**
	 * Get an xa  data source for the appropriate framework
	 * @param attrs  A set of attribute values to set on the datasource.
	 *                The appropriate setter method wil b
	 *                For example the property databaseName with value wombat,
	 *                will mean ds.setDatabaseName("wombat") will be called
	 *  @return datasource for current framework
	 */
    public static XADataSource getXADataSource(Properties attrs)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
        String classname =
            getDataSourcePrefix() + XA_DATASOURCE_STRING + "DataSource40";
        return (XADataSource) getDataSourceWithReflection(classname, attrs);
	}

	
	/**
	 * Get a ConnectionPoolDataSource  for the appropriate framework
	 * @param attrs  A set of attribute values to set on the datasource.
	 *                The appropriate setter method wil b
	 *                For example the property databaseName with value wombat,
	 *                will mean ds.setDatabaseName("wombat") will be called
	 *  @return datasource for current framework
	 */
    public static ConnectionPoolDataSource
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            getConnectionPoolDataSource(Properties attrs)
	{
        String classname = getDataSourcePrefix() +
                CONNECTION_POOL_DATASOURCE_STRING + "DataSource40";
        return (ConnectionPoolDataSource)
                getDataSourceWithReflection(classname, attrs);
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
	private static Hashtable<String, Class[]> specialAttributes = null;
	

	private static Object getDataSourceWithReflection(String classname, Properties attrs)
	{
		Object[] args = null;
		Object ds = null;
		Method sh = null;
		String methodName = null;
		
		if (specialAttributes == null)
		{
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
			specialAttributes = new Hashtable<String, Class[]>();
			specialAttributes.put("portNumber",INT_ARG_TYPE);
			specialAttributes.put("driverType",INT_ARG_TYPE);
			specialAttributes.put("retrieveMessagesFromServerOnGetMessage",
								  BOOLEAN_ARG_TYPE);
			specialAttributes.put("retrieveMessageText",
								  BOOLEAN_ARG_TYPE);
		}
		
		try {
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
            Class<?> clazz = Class.forName(classname);
            ds = clazz.getConstructor().newInstance();

		// for remote server testing, check whether the hostName is set for the test
		// if so, and serverName is not yet set explicitly for the datasource, set it now
//IC see: https://issues.apache.org/jira/browse/DERBY-434
//IC see: https://issues.apache.org/jira/browse/DERBY-1052
		String hostName = getHostName();
		if ( (!isEmbeddedFramework()) && (hostName != null ) && (attrs.getProperty("serverName") == null) )
			attrs.setProperty("serverName", hostName);

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
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
				{ Integer.valueOf(value) };	
			}
			else if (argType  == BOOLEAN_ARG_TYPE)
			{
				args = new Boolean[] { Boolean.valueOf(value) };	
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
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
			case Types.BOOLEAN  : return "Types.BOOLEAN";
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
	  public static String getNameFromJdbcType(int jdbcType) {
		switch (jdbcType) {
			case Types.BIT 		:  return "Types.BIT";
//IC see: https://issues.apache.org/jira/browse/DERBY-3484
			case Types.BOOLEAN  : return "Types.BOOLEAN";
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
	 * @param s  String to convert  (e.g
	 * @return hex literal that can be inserted into a bit column.
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
	*/

	public static int getJDBCMajorVersion(Connection conn)
	{
		try {
			// DatabaseMetaData.getJDBCMajorVersion() was not part of JDBC 2.0.
			// Check if setSavepoint() is present to decide whether the version
			// is > 2.0.
			conn.getClass().getMethod("setSavepoint", null);
//IC see: https://issues.apache.org/jira/browse/DERBY-1546
			DatabaseMetaData meta = conn.getMetaData();
			Method method =
				meta.getClass().getMethod("getJDBCMajorVersion", null);
			return ((Number) method.invoke(meta, null)).intValue();
		} catch (Throwable t) {
			// Error probably means that either setSavepoint() or
			// getJDBCMajorVersion() is not present. Assume JDBC 2.0.
			return 2;
		}

	}

    /**
//IC see: https://issues.apache.org/jira/browse/DERBY-413
        Drop the test objects passed in as a string identifying the
        type of object (e.g. TABLE, PROCEDURE) and its name.
        Thus, for example, a testObject array could be:
        {"TABLE MYSCHEMA.MYTABLE", "PROCEDURE THISDUMMY"}
        The statement passed in must be a 'live' statement in the test.
    */
    public static void cleanUpTest (Statement s, String[] testObjects)
                                    throws SQLException {
        /* drop each object named */
        for (int i=0; i < testObjects.length; i++) {
            try {
                s.execute("drop " + testObjects[i]);
                //System.out.println("now dropping " + testObjects[i]);
            } catch (SQLException se) { // ignore...
            }
        }	
    }

    
    /**
     * Get connection to given database using the connection attributes. This
     * method is used by tests to get a secondary connection with 
     * different set of attributes. It does not use what is specified in 
     * app_properties file or system properties. This method uses DataSource 
     * class for CDC/Foundation Profile environments, which are based on 
     * JSR169. Using DataSource will not work with other j9 profiles. So
     * DriverManager is used for non-JSR169. The method is used as a wrapper to
     * hide this difference in getting connections in different environments.
     *  
     * @param databaseName
     * @param connAttrs
     * @return Connection to database 
     * @throws SQLException on failure to connect.
     * @throws ClassNotFoundException on failure to load driver.
     * @throws InstantiationException on failure to load driver.
     * @throws IllegalAccessException on failure to load driver.
     */
    public static Connection getConnection(String databaseName, String connAttrs)
//IC see: https://issues.apache.org/jira/browse/DERBY-949
    	throws SQLException {
        try {
            Connection conn;
            if(TestUtil.HAVE_DRIVER_CLASS) {
                // following is like loadDriver(), but
                // that method throws Exception, we want finer granularity
                String driverName;
                int framework = getFramework();
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
                    default:
                        driverName =  "org.apache.derby.jdbc.EmbeddedDriver";
                        break;
                } 
                // q: do we need a privileged action here, like in loadDriver?
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
                Class<?> clazz = Class.forName(driverName);
                clazz.getConstructor().newInstance();
				
                String url = getJdbcUrlPrefix() + databaseName;
                if (connAttrs != null) url += ";" + connAttrs;
                if (framework == DERBY_NET_FRAMEWORK)
                {
                    if (( connAttrs == null) || ((connAttrs != null) && (connAttrs.indexOf("user") < 0)))
//IC see: https://issues.apache.org/jira/browse/DERBY-993
                        url += ":" + "user=APP;password=APP;retrieveMessagesFromServerOnGetMessage=true;";
                }
                conn = DriverManager.getConnection(url);
    	    }
    	    else {
    		    //Use DataSource for JSR169
	    	    Properties prop = new Properties();
	            prop.setProperty("databaseName", databaseName);
    		    if (connAttrs != null)
	                prop.setProperty("connectionAttributes", connAttrs);
	            conn = getDataSourceConnection(prop);
    	    }
            return conn;
    	} catch (ClassNotFoundException cnfe) { 
		    System.out.println("FAILure: Class not found!");
		    cnfe.printStackTrace();
		    return null;
    	} catch (InstantiationException inste) {
    		System.out.println("FAILure: Cannot instantiate class");
    		inste.printStackTrace();
    		return null;
    	} catch (IllegalAccessException ille) {
    		System.out.println("FAILure: Not allowed to use class");
    		ille.printStackTrace();
    		return null;
//IC see: https://issues.apache.org/jira/browse/DERBY-6856
    	} catch (NoSuchMethodException ille) {
    		System.out.println("FAILure: No such constructor");
    		ille.printStackTrace();
    		return null;
    	} catch (java.lang.reflect.InvocationTargetException ille) {
    		System.out.println("FAILure: Cannot execute constructor");
    		ille.printStackTrace();
    		return null;
    	}
    }
    
    public static Connection getDataSourceConnection (Properties prop) throws SQLException {
//IC see: https://issues.apache.org/jira/browse/DERBY-398
//IC see: https://issues.apache.org/jira/browse/DERBY-597
		DataSource ds = TestUtil.getDataSource(prop);
		try {
			Connection conn = ds.getConnection();
			return conn;
		}
		catch (SQLException e) {
			throw e;
		}
	}
	
	public static void shutdownUsingDataSource (String dbName) throws SQLException {
		Properties prop = new Properties();
		prop.setProperty("databaseName", dbName );
		prop.setProperty("shutdownDatabase", "shutdown" );
		DataSource ds = TestUtil.getDataSource(prop);
		try {
			Connection conn = ds.getConnection();
		}
		catch (SQLException e) {
			throw e;
		}
	}
	
	//Used by metadata tests for DatabaseMetadata.getURL
	public static boolean compareURL(String url) {
			
//IC see: https://issues.apache.org/jira/browse/DERBY-398
		if(isEmbeddedFramework()) {
			if(url.compareTo("jdbc:derby:wombat") == 0)
				return true;
		} else if(isNetFramework()) {
			try {
				StringTokenizer urlTokenizer = new StringTokenizer(url, "/");
				String urlStart = urlTokenizer.nextToken();
				urlTokenizer.nextToken();
				String urlEnd = urlTokenizer.nextToken();
				
				if(urlEnd.compareTo("wombat;create=true") != 0)
					return false;
				
				if(isJCCFramework() && (urlStart.compareTo("jdbc:derby:net:") == 0))
					return true;
				
				if(isDerbyNetClientFramework() && (urlStart.compareTo("jdbc:derby:") == 0))
					return true;
				
			} catch (NoSuchElementException nsee) {
				//Should not reach here.
				return false;
			}
		}
		
		return false;
	}

	/**
	 * For JDK 1.5 or higher print all stack traces to the
	 * specified PrintWriter.
	 * 
	 * @param log  PrintWriter to print to
	 */
    public static void dumpAllStackTracesIfSupported(PrintWriter log)
	{
//IC see: https://issues.apache.org/jira/browse/DERBY-3503
		try {
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
			String version = AccessController.doPrivileged
				(new PrivilegedAction<String>(){
						public String run(){
							return System.getProperty("java.version");
						}
					}
				 );
                   
			JavaVersionHolder j=  new JavaVersionHolder(version); 
			
			if (j.atLeast(1,5)){
//IC see: https://issues.apache.org/jira/browse/DERBY-5840
				Class<?> c = Class.forName("org.apache.derbyTesting.functionTests.util.ThreadDump");
				final Method m = c.getMethod("getStackDumpString",new Class[] {});
				
				String dump;
				try {
					dump = AccessController.doPrivileged
						(new PrivilegedExceptionAction<String>(){
								public String run() throws
									IllegalArgumentException, 
									IllegalAccessException, 
									InvocationTargetException{
									return (String) m.invoke(null);
								}
							}
						 );
				}     catch (PrivilegedActionException e) {
					throw  e.getException();
				}
				log.println(dump);                    
			}                     
		}
		catch (Exception e){
			// if we get an exception trying to get a thread dump. Just print it to the log and continue.
			log.println("Error trying to dump thread stack traces");
			if (e instanceof InvocationTargetException)
				((InvocationTargetException) e).getTargetException().printStackTrace(log);
			else
				e.printStackTrace(log);
		}
		
	}
}


