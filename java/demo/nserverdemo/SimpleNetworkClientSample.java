/*

   Derby - Class SimpleNetworkClientSample

   Copyright 2003, 2004 The Apache Software Foundation or its licensors, as applicable.

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

import java.sql.*;
import java.lang.reflect.*;
import javax.sql.DataSource;
import java.util.Properties;
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * The primary purpose of this program is to demonstrate how to obtain
 * client connections using DriverManager or a DataSource
 * and interact with Derby Network Server
 *
 * In particular,this sample program
 * 1)   loads the DB2 Universal JDBC Driver
 * 2)	obtains a client connection using the Driver Manager
 * 3)	obtains a client connection using a DataSource
 * 4)	tests the database connections by executing a sample query
 * and then exits the program
 *
 * Before running this program, please make sure that Clouscape Network Server is up
 * and running.
 *  <P>
 *  Usage: java SimpleNetworkClientSample
 *
 */
public class SimpleNetworkClientSample
{

	/*
	 * The database is located in the same directory where this program is being
	 * run. Alternately one can specify the absolute path of the database location
	 */
	private static String DBNAME="NSSimpleDB";

	/**
	 * Derby network server port ; default is 1527
	 */
	private static int NETWORKSERVER_PORT=1527;

	/**
	 * DB2 JDBC UNIVERSAL DRIVER class names
	 */
	private static final String DB2_JDBC_UNIVERSAL_DRIVER = "com.ibm.db2.jcc.DB2Driver";
	private static final String DB2_JCC_DS = "com.ibm.db2.jcc.DB2SimpleDataSource";

	/**
	 * This URL is used to connect to Derby Network Server using the DriverManager.
	 * Also, this url describes the target database for type 4 connectivity
	 * Notice that the properties may be established via the URL syntax
	 */
	private static final String CS_NS_DBURL= "jdbc:derby:net://localhost:"+NETWORKSERVER_PORT+"/"+DBNAME+";retrieveMessagesFromServerOnGetMessage=true;deferPrepares=true;";


	public static void main (String[] args)
		throws Exception
	{
		DataSource clientDataSource = null;
		Connection clientConn1 = null;
		Connection clientConn2 = null;


		try
		{
			System.out.println("Starting Sample client program ");

			// load DB2 JDBC UNIVERSAL DRIVER to enable client connections to
			// Derby Network Server
			loadJCCDriver();

			// get a client connection using DriverManager
			clientConn1 = getClientDriverManagerConnection();
			System.out.println("Got a client connection via the DriverManager.");

			// create a datasource with the necessary information
			javax.sql.DataSource myDataSource = getClientDataSource(DBNAME, null, null);

			// get a client connection using DataSource
			clientConn2 = getClientDataSourceConn(myDataSource);
			System.out.println("Got a client connection via a DataSource.");

			// test connections by doing some work
			System.out.println("Testing the connection obtained via DriverManager by executing a sample query ");
			test(clientConn1);
			System.out.println("Testing the connection obtained via a DataSource by executing a sample query ");
			test(clientConn2);

			System.out.println("Goodbye!");
		}
		catch (SQLException sqle)
		{
			System.out.println("Failure making connection: " + sqle);
			sqle.printStackTrace();
		}
		finally
		{

			if(clientConn1 != null)
				clientConn1.close();
			if(clientConn2 != null)
				clientConn2.close();
		}
	}

	/**
	 * Get a database connection from DataSource
	 * @pre Derby Network Server is started
	 * @param	ds	data source
	 * @return	returns database connection
	 * @throws Exception if there is any error
	 */
	public static Connection getClientDataSourceConn(javax.sql.DataSource ds)
		throws Exception
	{
		Connection conn = ds.getConnection("usr2", "pass2");
		System.out.print("connection from datasource; getDriverName = ");
		System.out.println(conn.getMetaData().getDriverName());
		return conn;
	}

	/**
	 * Creates a client data source and sets all the necessary properties in order to
	 * connect to Derby Network Server
	 * The server is assumed to be running on 1527 and on the localhost
	 * @param	database	database name; can include Derby URL attributes
	 * @param	user		database user
	 * @param	password
	 * @return	returns DataSource
	 * @throws Exception if there is any error
	 */
	public static javax.sql.DataSource getClientDataSource(String database, String user, String
									  password) throws SQLException, ClassNotFoundException, InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException
	{
		Class nsDataSource = Class.forName(DB2_JCC_DS);
		DataSource ds = (DataSource) nsDataSource.newInstance();

		// can also include Derby URL attributes along with the database name
		Class[] methodParams = new Class[] {String.class};
		Method dbname = nsDataSource.getMethod("setDatabaseName", methodParams);
		Object[] args = new Object[] {database};
		dbname.invoke(ds, args);

		if (user != null) {
			Method setuser = nsDataSource.getMethod("setUser", methodParams);
			args = new Object[] {user};
			setuser.invoke(ds, args);
		}
		if (password != null) {
			Method setpw = nsDataSource.getMethod("setPassword", methodParams);
			args = new Object[] {password};
			setpw.invoke(ds, args);
		}
		// host on which network server is running
		Method servername = nsDataSource.getMethod("setServerName", methodParams);
		args = new Object[] {"localhost"};
		servername.invoke(ds, args);

		// port on which Network Server is listening
		methodParams = new Class[] {int.class};
		Method portnumber = nsDataSource.getMethod("setPortNumber", methodParams);
		args = new Object[] {new Integer(1527)};
		portnumber.invoke(ds, args);

		// driver type must be 4 to access Derby Network Server
		Method drivertype = nsDataSource.getMethod("setDriverType", methodParams);
		args = new Object[] {new Integer(4)};
		drivertype.invoke(ds, args);

		return ds;

	}


	/**
	 * Load DB2 JDBC UNIVERSAL DRIVER
	 */
	public static void loadJCCDriver()
		throws Exception
	{
		// Load the JCC Driver
		Class.forName(DB2_JDBC_UNIVERSAL_DRIVER).newInstance();
	}

	/**
	 * Get a client connection using the DriverManager
	 * @pre DB2 JDBC Universal driver must have been loaded before calling this method
	 * @return Connection	client database connection
	 */
	public static Connection getClientDriverManagerConnection()
		throws Exception
	{

		// See Derby documentation for description of properties that may be set
		//  in the context of the network server.
		Properties properties = new java.util.Properties();

		// The user and password properties are a must, required by JCC
		properties.setProperty("user","cloud");
		properties.setProperty("password","scape");

		// Get database connection using the JCC client via DriverManager api
		Connection conn = DriverManager.getConnection(CS_NS_DBURL,properties); 

		return conn;
	}


	/**
	 * Test a connection by executing a sample query
	 * @param	conn 	database connection
	 * @throws Exception if there is any error
	 */
	public static void test(Connection conn)
		throws Exception
	{

	  Statement stmt = null;
	  ResultSet rs = null;
	  try
	  {
		// To test our connection, we will try to do a select from the system catalog tables
		stmt = conn.createStatement();
		rs = stmt.executeQuery("select count(*) from sys.systables");
		while(rs.next())
			System.out.println("number of rows in sys.systables = "+ rs.getInt(1));

	  }
	  catch(SQLException sqle)
	  {
		  System.out.println("SQLException when querying on the database connection; "+ sqle);
		  throw sqle;
  	  }
  	  finally
  	  {
		  if(rs != null)
		  	rs.close();
		  if(stmt != null)
		  	stmt.close();
 	  }
	}
}






