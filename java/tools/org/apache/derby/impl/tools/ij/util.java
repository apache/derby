/*

   Derby - Class org.apache.derby.impl.tools.ij.util

   Copyright 1997, 2004 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derby.impl.tools.ij;

import org.apache.derby.tools.JDBCDisplayUtil;
import org.apache.derby.iapi.tools.i18n.*;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

import java.util.Properties;
import java.util.Vector;

/**
	Methods used to control setup for apps as
	well as display some internal ij structures.

	@see org.apache.derby.tools.JDBCDisplayUtil
	@author ames
 */
public class util implements java.security.PrivilegedAction {

	private util() {}

	//-----------------------------------------------------------------
	// Methods for starting up JBMS

	/**
	 * Find the argument that follows the specified parameter.
	 *
	 *	@param param the parameter (e.g. "-p")
	 *	@param args	the argument list to consider.
	 *
	 *	@return the argument that follows the parameter, or null if not found
	 */
	static public String getArg(String param, String[] args)
	{
		int pLocn;
		Properties p;

		if (args == null) return null;

		for (pLocn=0; pLocn<args.length; pLocn++) {
			if (param.equals(args[pLocn])) break;
		}
		if (pLocn >= (args.length-1))  // not found or no file
			return null;

		return args[pLocn+1];
	}

	/**
		ij is started with "-p[r] file OtherArgs";
		the file contains properties to control the driver and database
		used to run ij, and can provide additional system properties.
		<p>
		getPropertyArg will look at the args and take out a "-p <file>" pair,
		reading the file into the system properties.
		<p>
		If there was a -p without a following <file>, no action is taken.

		@exception IOException thrown if file not found

		@param args	the argument list to consider.
		@return true if a property item was found and loaded.
	 */
	static public boolean getPropertyArg(String[] args) throws IOException {
		String n;
		InputStream in1;
		Properties p;

		if ((n = getArg("-p", args))!= null){
			in1 = new FileInputStream(n);
			in1 = new BufferedInputStream(in1);
		}
		else if ((n = getArg("-pr", args)) != null) {
			in1 = getResourceAsStream(n);
			if (in1 == null) throw ijException.resourceNotFound();
		}
		else
			return false;

		p = System.getProperties();

		// Trim off excess whitespace in property file, if any, and
		// then load those properties into 'p'.
		util.loadWithTrimmedValues(in1, p);

		return true;
	}

	/**
		ij is started with "-ca[r] file OtherArgs";
		the file contains connection attibute properties 
		to pass to getConnection
		<p>
		getConnAttributeArg will look at the args and take out a 
		"-ca[r] <file>" pair and returning the Properties
		<p>

		@exception IOException thrown if file not found

		@param args	the argument list to consider.
		@return  properties in the file
	 */
	static public Properties getConnAttributeArg(String[] args) 
		throws IOException 
	{
		String n;
		InputStream in1;
		Properties p = new Properties();

		if ((n = getArg("-ca", args))!= null){
			in1 = new FileInputStream(n);
			in1 = new BufferedInputStream(in1);
		}
		else if ((n = getArg("-car", args)) != null) {
			in1 = getResourceAsStream(n);
			if (in1 == null) throw ijException.resourceNotFound();
		}
		else
			return null;

		// Trim off excess whitespace in property file, if any, and
		// then load those properties into 'p'.
		util.loadWithTrimmedValues(in1, p);

		return p;
	}



	/**
	  Convenience routine to qualify a resource name with "ij.defaultPackageName"
	  if it is not qualified (does not begin with a "/").

	  @param absolute true means return null if the name is not absolute and false
	  means return partial names. 
	  */
	static String qualifyResourceName(String resourceName, boolean absolute)
	{
		resourceName=resourceName.trim();
		if (resourceName.startsWith("/"))
		{
			return resourceName;
		}
		else
		{
			String pName = util.getSystemProperty("ij.defaultResourcePackage").trim();
			if (pName == null) return null;
			if ((pName).endsWith("/"))
				resourceName = pName+resourceName;
			else
				resourceName = pName+"/"+resourceName;
			if (absolute && !resourceName.startsWith("/"))
				return null;
			else
				return resourceName;
		}
	}
	/**
	  Convenience routine to get a resource as a BufferedInputStream. If the
	  resourceName is not absolute (does not begin with a "/") this qualifies
	  the name with the "ij.defaultResourcePackage" name.

	  @param String the name of the resource
	  @return a buffered stream for the resource if it exists and null otherwise.
	  */
	static public InputStream getResourceAsStream(String resourceName) 
	{
		Class c= util.class;
		resourceName = qualifyResourceName(resourceName,true);
		if (resourceName == null) 
			return null;
		InputStream is = c.getResourceAsStream(resourceName);
		if (is != null) 
			is = new BufferedInputStream(is, utilMain.BUFFEREDFILESIZE);
		return is;
	}

	/**
	  Return the name of the ij command file or null if none is
	  specified. The command file may be proceeded with -f flag on
	  the command line. Alternatively, the command file may be 
	  specified without a -f flag. In this case we assume the first
	  unknown argument is the command file.

	  <P>
	  This should only be called after calling invalidArgs.

	  <p>
	  If there is no such argument, a null is returned.

	  @param args	the argument list to consider.
	  @return the name of the first argument not preceded by "-p",
	  null if none found.
	  
	  @exception IOException thrown if file not found
	 */
	static public String getFileArg(String[] args) throws IOException {
		String fileName;
		int fLocn;
		boolean foundP = false;

		if (args == null) return null;
		if ((fileName=getArg("-f",args))!=null) return fileName;
		//
		//The first unknown arg is the file
		for (int ix=0; ix < args.length; ix++)
			if(args[ix].equals("-f")  ||
			   args[ix].equals("-fr") ||
			   args[ix].equals("-ca")  ||
			   args[ix].equals("-car")  ||
			   args[ix].equals("-p")  ||
			   args[ix].equals("-pr"))
				ix++; //skip the parameter to these args
			else
				return args[ix];
		return null;
	}

	/**
	  Return the name of a resource containing input commands or
	  null iff none has been specified.
	  */
 	static public String getInputResourceNameArg(String[] args) {
		return getArg("-fr", args);
	}

	/**
	  Verify the ij line arguments command arguments.
	  @return true if the args are invalid
	  <UL>
	  <LI>Only legal argument provided.
	  <LI>Only specify a quantity once.
	  </UL>
	 */
	static public boolean invalidArgs(String[] args, boolean gotProp, String file,
									   String inputResourceName) {
		int countSupported = 0;
		boolean haveInput = false;
		for (int ix=0; ix < args.length; ix++)
		{
			//
			//If the arguemnt is a supported flag skip the flags argument
			if(!haveInput && (args[ix].equals("-f") || args[ix].equals("-fr")))
			{
				haveInput = true;
				ix++;
				if (ix >= args.length) return true;
			}

			else if ((args[ix].equals("-p") || args[ix].equals("-pr") ||
					  args[ix].equals("-ca") || args[ix].equals("-car") ))
			{
				// next arg is the file/resource name
				ix++;
				if (ix >= args.length) return true;
			}


			//
			//Assume the first unknown arg is a file name.
			else if (!haveInput)
			{
				haveInput = true;
			}

			else
			{
				return true;
			}
		}
		return false;
	}

	/**
	 * print a usage message for invocations of main().
	 */
	static void Usage(LocalizedOutput out) {
     	out.println(
		LocalizedResource.getMessage("IJ_UsageJavaComCloudToolsIjPPropeInput"));
		out.flush();
   	}


    private static final Class[] STRING_P = { "".getClass() };
    private static final Class[] INT_P = { Integer.TYPE };


    static public void setupDataSource(Object ds) throws Exception {
	// Loop over set methods on Datasource object, if there is a property
	// then call the method with corresponding value.
	java.lang.reflect.Method[] methods = ds.getClass().getMethods();
	for (int i = 0; i < methods.length; i++) {
	    java.lang.reflect.Method m = methods[i];
	    String name = m.getName();
	    if (name.startsWith("set") && (name.length() > "set".length())) {
		String property = name.substring("set".length()); // setXyyyZwww
		property = "ij.dataSource."+property.substring(0,1).toLowerCase(java.util.Locale.ENGLISH)+ property.substring(1); // xyyyZwww
		String value = util.getSystemProperty(property);
		//System.out.println("setupDateSource: method="+name+" property="+property+" value="+((value==null)?"null":value));
		if (value != null) {
		    try {
			// call string method
			m.invoke(ds, new Object[] {value});
		    } catch (Throwable ignore) {
			// failed, assume it's an integer parameter
			m.invoke(ds, new Object[] {Integer.valueOf(value)});
		    }
		}
	    }
	}
    }

	/**
		This will look for the System properties "ij.driver" and "ij.database"
		and return a java.sql.Connection if it successfully connects.
		The deprecated driver and database properties are examined first.
		<p>
		If no connection was possible, it will return a null.
		<p>
		Failure to load the driver class is quietly ignored.

		@param defaultDriver the driver to use if no property value found
		@param defaultURL the database URL to use if no property value found
		@param connInfo Connection attributes to pass to getConnection
		@return a connection to the defaultURL if possible; null if not.
		@exception SQLException on failure to connect.
		@exception ClassNotFoundException on failure to load driver.
		@exception InstantiationException on failure to load driver.
		@exception IllegalAccessException on failure to load driver.
	 */
    static public Connection startJBMS(String defaultDriver, String defaultURL,
				       Properties connInfo) 
	throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException
    {
	Connection con = null;
        String driverName;
        String databaseURL;

	// deprecate the non-ij prefix.  actually, we should defer to jdbc.drivers...
        driverName = util.getSystemProperty("driver");
        if (driverName == null) driverName = util.getSystemProperty("ij.driver");
	if (driverName == null || driverName.length()==0) driverName = defaultDriver;
        if (driverName != null) {
	    util.loadDriver(driverName);
	}

	String jdbcProtocol = util.getSystemProperty("ij.protocol");
	if (jdbcProtocol != null)
	    util.loadDriverIfKnown(jdbcProtocol);

	// deprecate the non-ij prefix name
	databaseURL = util.getSystemProperty("database");
	if (databaseURL == null) databaseURL = util.getSystemProperty("ij.database");
	if (databaseURL == null || databaseURL.length()==0) databaseURL = defaultURL;
	if (databaseURL != null) {
	    // add protocol if might help find driver.
	    boolean noDriver = false;
	    try {
		// if have full URL, load driver for it
		if (databaseURL.startsWith("jdbc:"))
		    util.loadDriverIfKnown(databaseURL);
		DriverManager.getDriver(databaseURL);
	    } catch (SQLException se) {
		noDriver = true;
	    }
	    if (noDriver && jdbcProtocol != null)
		databaseURL = jdbcProtocol+databaseURL;

	    String user = util.getSystemProperty("ij.user");
	    String password = util.getSystemProperty("ij.password");

	    // Update connInfo for ij system properties and
	    // framework network server

	    connInfo = updateConnInfo(user, password,connInfo);

	    // JDBC driver
	    String driver = System.getProperty("driver");
	    if (driver == null) {
		driver = "org.apache.derby.jdbc.EmbeddedDriver";
	    }

	    // handle datasource property
	    String dsName = System.getProperty("ij.dataSource");
	    if (dsName == null) {
		loadDriver(driver);
		con = DriverManager.getConnection(databaseURL,connInfo);
		return con;

	    } else { // a datasource
		// Get a new proxied connection through DataSource
		Object ds = null; // really javax.sql.DataSource
		try {
		    Class dc = Class.forName(dsName);
		    ds = dc.newInstance();
		    
		    // set datasource properties
		    setupDataSource(ds);

		    // Java method call "by hand" {  con = ds.getConnection(); }
		    {
			java.lang.reflect.Method m = dc.getMethod("getConnection", null); 
			con = (java.sql.Connection) m.invoke(ds, new Object[] {});
		    }
		} catch (Throwable error) {
		    error.printStackTrace(System.out);
		}
		return con;
	    } // datasource
	}
	// failed
	return null;
    }


	public static Properties updateConnInfo(String user, String password, Properties connInfo)
	{
		String framework = util.getSystemProperty("framework");
		String ijGetMessages = util.getSystemProperty("ij.retrieveMessagesFromServerOnGetMessage");
		boolean retrieveMessages = false;
		
		
		// For JCC make sure we set it to retrieve messages
		if (framework != null  && ((framework.equals("DB2jNet") 
									|| framework.equals("DB2jcc"))))
			retrieveMessages = true;
		
		if (ijGetMessages != null)
		{
			if (ijGetMessages.equals("false"))
				retrieveMessages = false;
			else
				retrieveMessages = true;
			
		}
		
		if (connInfo == null)
			connInfo = new Properties();
		
		if (retrieveMessages == true)
		{
			connInfo.put("retrieveMessagesFromServerOnGetMessage",
						 "true");
		}
		if (user != null)
			connInfo.put("user",user);
		if (password != null)
			connInfo.put("password", password);
		
		return connInfo;
	}

	/**
		Utility interface that defaults driver and database to null.

		@return a connection to the defaultURL if possible; null if not.
		@exception SQLException on failure to connect.
		@exception ClassNotFoundException on failure to load driver.
		@exception InstantiationException on failure to load driver.
		@exception IllegalAccessException on failure to load driver.
	 */
    static public Connection startJBMS() throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException {
		return startJBMS(null,null);
	}
	
	/**
	   Utility interface that defaults connInfo to null
	   <p>


		@param defaultDriver the driver to use if no property value found
		@param defaultURL the database URL to use if no property value found
		@return a connection to the defaultURL if possible; null if not.
		@exception SQLException on failure to connect.
		@exception ClassNotFoundException on failure to load driver.
		@exception InstantiationException on failure to load driver.
		@exception IllegalAccessException on failure to load driver.
	 */
    static public Connection startJBMS(String defaultDriver, String defaultURL) 
			throws SQLException, ClassNotFoundException, InstantiationException,
				   IllegalAccessException {
		return startJBMS(defaultDriver,defaultURL,null);
		
	}
	//-----------------------------------------------------------------
	// Methods for displaying and checking results
	// See org.apache.derby.tools.JDBCDisplayUtil for more general displays.


	/**
		Display a vector of strings to the out stream.
	 */
	public static void DisplayVector(LocalizedOutput out, Vector v) {
		int l = v.size();
		for (int i=0;i<l;i++)
			out.println(v.elementAt(i));
	}

	/**
		Display a vector of statements to the out stream.
	public static void DisplayVector(AppStreamWriter out, Vector v, Connection conn) throws SQLException {
		int l = v.size();
AppUI.out.println("SIZE="+l);
		for (int i=0;i<l;i++) {
			Object o = v.elementAt(i);
			if (o instanceof Integer) { // update count
				JDBCDisplayUtil.DisplayUpdateCount(out,((Integer)o).intValue());
			} else { // o instanceof ResultSet
			    JDBCDisplayUtil.DisplayResults(out,(ResultSet)o,conn);
				((ResultSet)o).close(); // release the result set
			}
		}
	}
	 */

	/**
		Display a statement that takes parameters by
		stuffing it with rows from the result set and
		displaying each result each time through.
		Deal with autocommit behavior along the way.

		@exception SQLException thrown on db error
		@exception ijException thrown on ij error
	 */
	public static void DisplayMulti(LocalizedOutput out, PreparedStatement ps,
		ResultSet rs, Connection conn) throws SQLException, ijException {

		boolean autoCommited = false; // mark if autocommit in place
		boolean exec = false; // mark the first time through
		boolean anotherUsingRow = false;	// remember if there's another row 
											// from using.
		ResultSetMetaData rsmd = rs.getMetaData();
		int numCols = rsmd.getColumnCount();

		/* NOTE: We need to close the USING RS first
		 * so that RunTimeStatistic gets info from
		 * the user query.
		 */
		anotherUsingRow = rs.next();

		while (! autoCommited && anotherUsingRow) {
			// note the first time through
			if (!exec) {
				exec = true;

				// send a warning if additional results may be lost
				if (conn.getAutoCommit()) {
					out.println(LocalizedResource.getMessage("IJ_IjWarniAutocMayCloseUsingResulSet"));
					autoCommited = true;
				}
			}

			// We need to make sure we pass along the scale, because
			// setObject assumes a scale of zero (beetle 4365)
			for (int c=1; c<=numCols; c++) {
				ps.setObject(c,rs.getObject(c),
					 rsmd.getColumnType(c),
					 rsmd.getScale(c));
			}


			// Advance in the USING RS
			anotherUsingRow = rs.next();
			// Close the USING RS when exhausted and appropriate
			// NOTE: Close before the user query
			if (! anotherUsingRow || conn.getAutoCommit()) //if no more rows or if auto commit is on, close the resultset
			{
				rs.close();
			}

			/*
				4. execute the statement against those parameters
			 */

			ps.execute();
			JDBCDisplayUtil.DisplayResults(out,ps,conn);

			/*
				5. clear the parameters
			 */
			ps.clearParameters();
		}
		if (!exec) {
			rs.close(); //this means, using clause didn't qualify any rows. Just close the resultset associated with using clause
			throw ijException.noUsingResults();
		}
		// REMIND: any way to look for more rsUsing rows if autoCommit?
		// perhaps just document the behavior... 
	}

	public static final String getSystemProperty(String propertyName) {
		try
		{
			if (propertyName.startsWith("ij.") || propertyName.startsWith("derby."))
			{
				util u = new util();
				u.key = propertyName;
				return (String) java.security.AccessController.doPrivileged(u);
			}
			else
			{
				return System.getProperty(propertyName);
			}
		} catch (SecurityException se) {
			return null;
		}
	}

	private String key;

	public final Object run() {
		return System.getProperty(key);
	}
	/** 
	 * Read a set of properties from the received input stream, strip
	 * off any excess white space that exists in those property values,
	 * and then add those newly-read properties to the received
	 * Properties object; not explicitly removing the whitespace here can
	 * lead to problems.
	 *
	 * This method exists because of the manner in which the jvm reads
	 * properties from file--extra spaces are ignored after a _key_, but
	 * if they exist at the _end_ of a property decl line (i.e. as part
	 * of a _value_), they are preserved, as outlined in the Java API:
	 *
	 * "Any whitespace after the key is skipped; if the first non-
	 * whitespace character after the key is = or :, then it is ignored
 	 * and any whitespace characters after it are also skipped. All
	 * remaining characters on the line become part of the associated
	 * element string."
	 *
	 * @param iStr: An input stream from which the new properties are to be
	 *  loaded (should already be initialized).
	 * @param prop: A set of properties to which the properties from
	 *  iStr will be added (should already be initialized).
	 * @return A final properties set consisting of 'prop' plus all
	 * properties loaded from 'iStr' (with the extra whitespace (if any)
	 *  removed from all values), will be returned via the parameter.
	 *
		Copied here to avoid dependency on an engine class.
	 **/
	private static void loadWithTrimmedValues(InputStream iStr,
		Properties prop) throws IOException {

		// load the properties from the received input stream.
		Properties p = new Properties();
		p.load(iStr);

		// Now, trim off any excess whitespace, if any, and then
		// add the properties from file to the received Properties
		// set.
		for (java.util.Enumeration propKeys = p.propertyNames();
		  propKeys.hasMoreElements();) {
		// get the value, trim off the whitespace, then store it
		// in the received properties object.
			String tmpKey = (String)propKeys.nextElement();
			String tmpValue = p.getProperty(tmpKey);
			tmpValue = tmpValue.trim();
			prop.put(tmpKey, tmpValue);
		}

		return;

	}

	private static final String[][] protocolDrivers =
		{
		  { "jdbc:derby:net:",			"com.ibm.db2.jcc.DB2Driver"},
		  { "jdbc:derby:",				"org.apache.derby.jdbc.EmbeddedDriver" },
		};

	/**
		Find the appropriate driver and load it, given a JDBC URL.
		No action if no driver known for a given URL.

		@param jdbcProtocol the protocol to try.

		@exception ClassNotFoundException if unable to
			locate class for driver.
		@exception InstantiationException if unable to
			create an instance.
		@exception IllegalAccessException if driver class constructor not visible.
	 */
	public static void loadDriverIfKnown(String jdbcProtocol) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
		for (int i=0; i < protocolDrivers.length; i++) {
			if (jdbcProtocol.startsWith(protocolDrivers[i][0])) {
				loadDriver(protocolDrivers[i][1]);
				break; // only want the first one
			}
		}
	}

	/**
		Load a driver given a class name.

		@exception ClassNotFoundException if unable to
			locate class for driver.
		@exception InstantiationException if unable to
			create an instance.
		@exception IllegalAccessException if driver class constructor not visible.
	 */
	public static void loadDriver(String driverClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        Class.forName(driverClass).newInstance();
	}
}
