/* 
 * (C) Copyright IBM Corp. 2003.
 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

import java.sql.*;
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
 *  <P>
 *  <I>IBM Corp. reserves the right to change, rename, or
 * 	remove this interface at any time.</I>
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
	 * DB2 JDBC UNIVERSAL DRIVER class name
	 */
	private static final String DB2_JDBC_UNIVERSAL_DRIVER = "com.ibm.db2.jcc.DB2Driver";

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
									  password) throws SQLException
	{

		com.ibm.db2.jcc.DB2SimpleDataSource ds = new com.ibm.db2.jcc.DB2SimpleDataSource();

		// can also include Derby URL attributes along with the database name
		ds.setDatabaseName(database);

		if (user != null)
			ds.setUser(user);
		if (password != null)
			ds.setPassword(password);

		// host on which network server is running
		ds.setServerName("localhost");

		// port on which Network Server is listening
		ds.setPortNumber(1527);

		// driver type must be 4 to access Derby Network Server
		ds.setDriverType(4);

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
		Connection conn =  (com.ibm.db2.jcc.DB2Connection) DriverManager.getConnection(CS_NS_DBURL, properties);

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






