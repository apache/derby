/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.UpgradeTester

Copyright 1999, 2006 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.net.URLClassLoader;
import java.net.URL;
import java.net.MalformedURLException;
import java.util.Properties;
import java.io.File;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.derbyTesting.functionTests.harness.jvm;

/**
 * Tests upgrades including soft upgrade. Test consists of following phases:
   
	<OL>
	<LI> Create database with the <B>old</B> release.
	<LI> Boot the database with the <B>new</B> release in soft upgrade mode.
	Try to execute functionality that is not allowed in soft upgrade.
	<LI> Boot the database with the <B>old</B> release to ensure the
	database can be booted by the old release after soft upgrade.
	<LI> Boot the database with the <B>new</B> release in hard upgrade mode,
	specifying the upgrade=true attribute.
	<LI> Boot the database with the <B>old</B> release to ensure the
	database can not be booted by the old release after hard upgrade.
	</OL>
	<P>
	That's the general idea for GA releases. Alpha/beta releases do not
	support upgrade unless the derby.database.allowPreReleaseUpgrade
	property is set to true, in which case this program modifies the expected
	behaviour.
	<P>
	
    <P>
	This tests the following specifically.

	10.1 Upgrade issues

	<UL>
	<LI> Routines with explicit Java signatures.
	</UL>
	
	Metadata tests
	
	10.2 Upgrade tests:
	
	caseReusableRecordIdSequenceNumber
	
 */
public class UpgradeTester {
	
	/**
	 * Phases in upgrade test
	 */
	private static final String[] PHASES =
	{"CREATE", "SOFT UPGRADE", "POST SOFT UPGRADE", "UPGRADE", "POST UPGRADE"};
	
	
	/**
	 * Create a database with old version
	 */
	static final int PH_CREATE = 0;
	/**
	 * Perform soft upgrade with new version
	 */
	static final int PH_SOFT_UPGRADE = 1;
	/**
	 * Boot the database with old release after soft upgrade
	 */
	static final int PH_POST_SOFT_UPGRADE = 2;
	/**
	 * Perform hard upgrade with new version
	 */
	static final int PH_HARD_UPGRADE = 3;
	/**
	 * Boot the database with old release after hard upgrade
	 */
	static final int PH_POST_HARD_UPGRADE = 4;
	
	/**
	 * Use old release for this phase
	 */
	private static final int OLD_RELEASE = 0;
	/**
	 * Use new release for this phase
	 */	
	private static final int NEW_RELEASE = 1;
	
	// Location of jar file of old and new release
	private String oldJarLoc;
	private String newJarLoc;
	
	// Class loader for old and new release jars
	private URLClassLoader oldClassLoader;
	private URLClassLoader newClassLoader;
	
	// Major and Minor version number of old release
	private int oldMajorVersion;
	private int oldMinorVersion;
	
	// Major and Minor version number of new release
	private int newMajorVersion;
	private int newMinorVersion;
	
	// Indicate if alpha/beta releases should support upgrade
	private boolean allowPreReleaseUpgrade;
	
	// We can specify more jars, as required.
	private String[] jarFiles = new String [] { "derby.jar", 
												"derbynet.jar",
												"derbyclient.jar",
												"derbytools.jar"};
	
	// Test jar
	private String testJar = "derbyTesting.jar";
	
	// Boolean to indicate if the test is run using jars or classes folder 
	// in classpath
	private boolean[] isJar = new boolean[1];
	
	/**
	 * Constructor
	 * 
	 * @param oldMajorVersion Major version number of old release
	 * @param oldMinorVersion Minor version number of old release
	 * @param newMajorVersion Major version number of new release
	 * @param newMinorVersion Minor version number of new release
	 * @param allowPreReleaseUpgrade If true, set the system property 
	 * 'derby.database.allowPreReleaseUpgrade' to indicate alpha/beta releases 
	 * to support upgrade.
	 */
	public UpgradeTester(int oldMajorVersion, int oldMinorVersion,
						int newMajorVersion, int newMinorVersion,
						boolean allowPreReleaseUpgrade) {
		this.oldMajorVersion = oldMajorVersion;
		this.oldMinorVersion = oldMinorVersion;
		this.newMajorVersion = newMajorVersion;
		this.newMinorVersion = newMinorVersion;
		this.allowPreReleaseUpgrade = allowPreReleaseUpgrade; 
	}
	
	/**
	 * Set the location of jar files for old and new release
	 */
	private void setJarLocations() {
		this.oldJarLoc = getOldJarLocation();
		this.newJarLoc = getNewJarLocation();
	}
	
	/**
	 * Get the location of jars of old release. The location is specified 
	 * in the property "derbyTesting.jar.path".
	 *  
	 * @return location of jars of old release
	 */
	private String getOldJarLocation() {
		String jarLocation = null;
		
		String jarPath = System.getProperty("derbyTesting.jar.path");
		
		if((jarPath != null) && (jarPath.compareTo("JAR_PATH_NOT_SET") == 0)) {
			System.out.println("FAIL: Path to previous release jars not set");
			System.out.println("Check if derbyTesting.jar.path property has been set in ant.properties file");
			System.exit(-1);
		}
		
		String version = oldMajorVersion + "." + oldMinorVersion;
		jarLocation = jarPath + File.separator + version;
		
		return jarLocation;
	}
	
	/**
	 * Get the location of jar of new release. This is obtained from the
	 * classpath using findCodeBase method in jvm class.
	 * 
	 * @return location of jars of new release
	 */
	private String getNewJarLocation() {
		return jvm.findCodeBase(isJar);
	}
	
	/**
	 * This method creates two class loaders - one for old release and
	 * other for new release. It calls the appropriate create methods
	 * depending on what is used in the user's classpath - jars or 
	 * classes folder
	 *  
	 * @throws MalformedURLException
	 */
	private void createClassLoaders() throws MalformedURLException{
		if(isJar[0]){
			oldClassLoader = createClassLoader(oldJarLoc);
			newClassLoader = createClassLoader(newJarLoc);
		} else {
		  // classes folder in classpath
		  createLoadersUsingClasses();	
		}
	}
	
	/**
	 * Create a class loader using jars in the specified location. Add all jars 
	 * specified in jarFiles and the testing jar.
	 * 
	 * @param jarLoc Location of jar files
	 * @return class loader
	 * @throws MalformedURLException
	 */
	private URLClassLoader createClassLoader(String jarLoc) 
							throws MalformedURLException {
		URL[] url = new URL[jarFiles.length + 1];
		
		for(int i=0; i < jarFiles.length; i++) {
			url[i] = new File(jarLoc + File.separator + jarFiles[i]).toURL();
		}
		
		// Add derbyTesting.jar. Added from newer release
		url[jarFiles.length] = new File(newJarLoc + File.separator + testJar).toURL();
		
		// Specify null for parent class loader to avoid mixing up 
		// jars specified in the system classpath
		return new URLClassLoader(url, null);		
	}
	
	/**
	 * Create old and new class loader. This method is used when classes folder
	 *  is specified in the user's classpath.
	 * 
	 * @throws MalformedURLException
	 */
	private void createLoadersUsingClasses() 
							throws MalformedURLException {
		URL[] oldUrl = new URL[jarFiles.length + 1];

		for(int i=0; i < jarFiles.length; i++) {
			oldUrl[i] = new File(oldJarLoc + File.separator + jarFiles[i]).toURL();
		}

		// Use derby testing classes from newer release. To get the
		// testing classes from newer release, we need to add the whole 
		// classes folder. So the oldClassLoader may contain extra classes
		// from the newer version
		oldUrl[jarFiles.length] = new File(newJarLoc).toURL();

		oldClassLoader = new URLClassLoader(oldUrl, null);
		
		URL[] newUrl = new URL[] {new File(newJarLoc).toURL()};
		newClassLoader = new URLClassLoader(newUrl, null);
	}
	
	/**
	 * Set the context class loader
	 * @param classLoader class loader
	 */
	private static void setClassLoader(URLClassLoader classLoader) {
		Thread.currentThread().setContextClassLoader(classLoader);
	}
	
	/**
	 * Set the context class loader to null
	 */
	private static void setNullClassLoader() {
		Thread.currentThread().setContextClassLoader(null);
	}
	
	/**
	 * Runs the upgrade tests by calling runPhase for each phase.
	 * @throws Exception
	 */
	public void runUpgradeTests() throws Exception{
		// Set the system property to allow alpha/beta release 
		// upgrade as specified
		if(allowPreReleaseUpgrade)
			System.setProperty("derby.database.allowPreReleaseUpgrade", 
							   "true");
		else
			System.setProperty("derby.database.allowPreReleaseUpgrade", 
								"false");
		
		setJarLocations();
		createClassLoaders();
		runPhase(OLD_RELEASE, PH_CREATE);
		runPhase(NEW_RELEASE, PH_SOFT_UPGRADE);
		runPhase(OLD_RELEASE, PH_POST_SOFT_UPGRADE);
		runPhase(NEW_RELEASE, PH_HARD_UPGRADE);
		runPhase(OLD_RELEASE, PH_POST_HARD_UPGRADE);
	}
	
	/**
	 * Runs each phase of upgrade test.
	 * 1. Chooses the classloader to use based on the release (old/new)
	 * 2. Gets a connection.
	 * 3. If connection is successful, checks the version using metadata,
	 * runs tests and shuts down the database.
	 * 
	 * @param version Old or new version
	 * @param phase Upgrade test phase
	 * @throws Exception
	 */
	private void runPhase(int version, int phase) 
												throws Exception{
		System.out.println("\n\nSTART - phase " + PHASES[phase]);
		
		URLClassLoader classLoader = null;
		switch(version) {
			case OLD_RELEASE:
				classLoader = oldClassLoader;
				break;
			case NEW_RELEASE:
				classLoader = newClassLoader;
				break;
			default:
				System.out.println("ERROR: Specified an invalid release type");
				return;
		}
		
		boolean passed = true;
		Connection conn = null;
		String dbName = "wombat";
		
		setClassLoader(classLoader);
		
		conn = getConnection(classLoader, dbName, phase);
				
		if(conn != null) {
			passed = caseVersionCheck(version, conn);
			passed = caseReusableRecordIdSequenceNumber(conn, phase, 
								oldMajorVersion, oldMinorVersion) && passed;
			passed = caseInitialize(conn, phase) && passed;
			passed = caseProcedures(conn, phase, oldMajorVersion, 
									oldMinorVersion) && passed;
			runMetadataTest(classLoader, conn);
			conn.close();
			shutdown(classLoader, dbName);
		}
		setNullClassLoader();
		
		System.out.println("END - " + (passed ? "PASS" : "FAIL") +
							" - phase " + PHASES[phase]);
	}

	/**
	 * Get a connection to the database using the specified class loader. 
	 * The connection attributes depend on the phase of upgrade test.
	 * 
	 * @param classLoader Class loader
	 * @param dbName database name
	 * @param phase Upgrade test phase
	 * @return connection to the database
	 * @throws Exception
	 */
	private Connection getConnection(URLClassLoader classLoader, String dbName,
									int phase) throws Exception{
		Connection conn = null;
		Properties prop = new Properties();
		prop.setProperty("databaseName", dbName);
		
		switch(phase) {
			case PH_CREATE:
				prop.setProperty("connectionAttributes", "create=true");
				break;
			case PH_SOFT_UPGRADE:
			case PH_POST_SOFT_UPGRADE:
			case PH_POST_HARD_UPGRADE:
				break;	
			case PH_HARD_UPGRADE:
				prop.setProperty("connectionAttributes", "upgrade=true");				
				break;
			default:
				break;
		}
		
		try {
			conn = getConnectionUsingDataSource(classLoader, prop);
		} catch (SQLException sqle) {
			if(phase != PH_POST_HARD_UPGRADE)
				throw sqle;
			
			// After hard upgrade, we should not be able to boot
			// the database with older version. Possible SQLStates are
			// XSLAP, if the new release is alpha/beta; XSLAN, otherwise.
			if(sqle.getSQLState().equals("XJ040")) { 
				SQLException nextSqle = sqle.getNextException(); 
				if(nextSqle.getSQLState().equals("XSLAP") ||
					nextSqle.getSQLState().equals("XSLAN") )
					System.out.println("Expected exception: Failed to start" +
						" database with old version after hard upgrade");
			}
		}
				
		return conn;
	}
	
	/**
	 * Get a connection using data source obtained from TestUtil class.
	 * Load TestUtil class using the specified class loader.
	 *  
	 * @param classLoader
	 * @param prop
	 * @return
	 * @throws Exception
	 */
	private Connection getConnectionUsingDataSource(URLClassLoader classLoader, Properties prop) throws Exception{
		Connection conn = null;
		
		try {
			Class testUtilClass = Class.forName("org.apache.derbyTesting.functionTests.util.TestUtil", 
												true, classLoader);
			Object testUtilObject = testUtilClass.newInstance();
		
			// Instead of calling TestUtil.getDataSourceConnection, call 
			// TestUtil.getDataSource and then call its getConnection method.
			// This is because we do not want to lose the SQLException
			// which we get when shutting down the database. 
			java.lang.reflect.Method method = testUtilClass.getMethod("getDataSource", new Class[] { prop.getClass() });
	      	DataSource ds = (DataSource) method.invoke(testUtilClass, new Object[] { prop });
      	conn = ds.getConnection();
		} catch(SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			handleReflectionExceptions(e);
			throw e;
		} 

      	return conn;
	}
	
	/**
	 * Verify the product version from metadata
	 * @param version Old or new version
	 * @param conn Connection
	 * @throws SQLException
	 */
	private boolean caseVersionCheck(int version, Connection conn) 
														throws SQLException{
		boolean passed = false;
		int actualMajorVersion;
		int actualMinorVersion;
				
		if (conn == null)
			return false;
		
		actualMajorVersion = conn.getMetaData().getDatabaseMajorVersion();
		actualMinorVersion = conn.getMetaData().getDatabaseMinorVersion();
		
		switch(version) {
			case OLD_RELEASE:
				passed = (actualMajorVersion == oldMajorVersion) && (actualMinorVersion == oldMinorVersion);
				break;
			case NEW_RELEASE:
				passed = (actualMajorVersion == newMajorVersion) && (actualMinorVersion == newMinorVersion);
				break;
			default:	
				passed = false;
				break;
		}
		
		System.out.println("complete caseVersionCheck - passed " + passed);
		return passed;
	}
	
	/**
	 * In 10.2: We will write a ReusableRecordIdSequenceNumber in the 
	 * header of a FileContaienr.
	 * 
	 * Verify here that a 10.1 Database does not malfunction from this.
	 * 10.1 Databases should ignore the field.
	 */
	static boolean caseReusableRecordIdSequenceNumber(Connection conn, 
											   int phase, 
											   int dbMajor, int dbMinor)
		throws SQLException
	{
		boolean runCompress = dbMajor>10 || dbMajor==10 && dbMinor>=1;
		final boolean passed;
		switch(phase) {
		case PH_CREATE: {
			Statement s = conn.createStatement();
			s.execute("create table CT1(id int)");
			s.execute("insert into CT1 values 1,2,3,4,5,6,7,8,9,10");
			conn.commit();
			passed = true;
			break;
		}
		case PH_SOFT_UPGRADE:
			if (runCompress) {
				System.out.println("caseReusableRecordIdSequenceNumber - Running compress");
				PreparedStatement ps = conn.prepareStatement
					("call SYSCS_UTIL.SYSCS_INPLACE_COMPRESS_TABLE(?,?,?,?,?)");
				ps.setString(1, "APP"); // schema
				ps.setString(2, "CT1");  // table name
				ps.setInt(3, 1); // purge
				ps.setInt(4, 1); // defragment rows
				ps.setInt(5, 1); // truncate end
				ps.executeUpdate();
				conn.commit();
			}
			passed = true;
			break;
		case PH_POST_SOFT_UPGRADE: {
			// We are now back to i.e 10.1
			Statement s = conn.createStatement();
			ResultSet rs = s.executeQuery("select * from CT1");
			while (rs.next()) {
				rs.getInt(1);
			}
			s.execute("insert into CT1 values 11,12,13,14,15,16,17,18,19");
			conn.commit();
			passed = true;
			break;
		}
		case PH_HARD_UPGRADE:
			passed = true;
			break;
		default:
			passed = false;
			break;
		}
		System.out.println("complete caseReusableRecordIdSequenceNumber - passed " + passed);
		return passed;
	}
	
	/**
	 * Perform some transactions
	 * 
	 * @param conn Connection
	 * @param phase Upgrade test phase
	 * @return true if the test passes
	 * @throws SQLException
	 */
	private boolean caseInitialize(Connection conn, int phase)
	throws SQLException {
	
		boolean passed = true;
	
		switch (phase) {
		case PH_CREATE:
			conn.createStatement().executeUpdate("CREATE TABLE PHASE" +
												"(id INT NOT NULL, ok INT)");
			conn.createStatement().executeUpdate("CREATE TABLE TABLE1" +
						"(id INT NOT NULL PRIMARY KEY, name varchar(200))");
			break;
		case PH_SOFT_UPGRADE:
			break;
		case PH_POST_SOFT_UPGRADE:
			break;
		case PH_HARD_UPGRADE:
			break;
		default:
			passed = false;
			break;
		}
	
		PreparedStatement ps = conn.prepareStatement("INSERT INTO PHASE(id) " +
													 "VALUES (?)");
		ps.setInt(1, phase);
		ps.executeUpdate();
		ps.close();
		
		// perform some transactions
		ps = conn.prepareStatement("INSERT INTO TABLE1 VALUES (?, ?)");
		for (int i = 1; i < 20; i++)
		{
			ps.setInt(1, i + (phase * 100));
			ps.setString(2, "p" + phase + "i" + i);
			ps.executeUpdate();
		}
		ps.close();
		ps = conn.prepareStatement("UPDATE TABLE1 set name = name || 'U' " +
									" where id = ?");
		for (int i = 1; i < 20; i+=3)
		{
			ps.setInt(1, i + (phase * 100));
			ps.executeUpdate();
		}
		ps.close();
		ps = conn.prepareStatement("DELETE FROM TABLE1 where id = ?");
		for (int i = 1; i < 20; i+=4)
		{
			ps.setInt(1, i + (phase * 100));
			ps.executeUpdate();
		}
		ps.close();
		System.out.println("complete caseInitialize - passed " + passed);
		return passed;
	}
	
	/**
	 * Procedures
	 * 10.1 - Check that a procedure with a signature can not be added if the
	 * on-disk database version is 10.0.
	 *
	 * @param conn Connection
	 * @param phase Upgrade test phase
	 * @param dbMajor Major version of old release 
	 * @param dbMinor Minor version of old release
	 * @return true, if the test passes
	 * @throws SQLException
	 */
	private boolean caseProcedures(Connection conn, int phase, 
									int dbMajor, int dbMinor)
									throws SQLException {
		
		boolean signaturesAllowedInOldRelease =
			dbMajor > 10 || (dbMajor == 10 && dbMinor >= 1);

		boolean passed = true;

		switch (phase) {
		case PH_CREATE:
			break;
		case PH_SOFT_UPGRADE:
			
			try {
				conn.createStatement().execute("CREATE PROCEDURE GC() " +
						"LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME" +
						" 'java.lang.System.gc()'");
				if (!signaturesAllowedInOldRelease)
				{
					System.out.println("FAIL : created procedure with " +
										"signature");
					passed = false;
				}
			} catch (SQLException sqle) {
				if (signaturesAllowedInOldRelease 
						|| !"XCL47".equals(sqle.getSQLState())) {
					System.out.println("FAIL " + sqle.getSQLState() 
										+ " -- " + sqle.getMessage());
					passed = false;
				}
			}
			break;
		case PH_POST_SOFT_UPGRADE:
			try {
				conn.createStatement().execute("CALL GC()");
				if (!signaturesAllowedInOldRelease)
					System.out.println("FAIL : procedure was created" +
										" in soft upgrade!");
					
			} catch (SQLException sqle)
			{
				if (signaturesAllowedInOldRelease)
					System.out.println("FAIL : procedure was created not in " +
										"soft upgrade!" + sqle.getMessage());
			}
			break;
		case PH_HARD_UPGRADE:
			if (!signaturesAllowedInOldRelease)
				conn.createStatement().execute("CREATE PROCEDURE GC() " +
						"LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME " +
						"'java.lang.System.gc()'");
			conn.createStatement().execute("CALL GC()");
			break;
		default:
			passed = false;
			break;
		}

		System.out.println("complete caseProcedures - passed " + passed);
		return passed;
	}
	
	/**
	 * Run metadata test
	 * 
	 * @param classLoader Class loader to be used to load the test class
	 * @param conn Connection
	 * @throws Exception
	 */
	private void runMetadataTest(URLClassLoader classLoader, Connection conn) 
				throws Exception{
		try {
	      	Statement stmt = conn.createStatement();
	      	
			Class metadataClass = Class.forName("org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata", 
							  				true, classLoader);
			Object metadataObject = metadataClass.newInstance();
			java.lang.reflect.Field f1 = metadataClass.getField("con");
	      	f1.set(metadataObject, conn);
	      	java.lang.reflect.Field f2 = metadataClass.getField("s");
	      	f2.set(metadataObject, stmt);
			java.lang.reflect.Method method = metadataClass.getMethod("runTest", 
																	  null);
	      	method.invoke(metadataObject, null);
		} catch(SQLException sqle) {
			throw sqle;
		} catch (Exception e) {
			handleReflectionExceptions(e);
			throw e;
		} 
    }
	
	/**
	 * Shutdown the database
	 * @param classLoader
	 * @param dbName
	 * @throws Exception
	 */
	private void shutdown(URLClassLoader classLoader, String dbName) 
												throws Exception {
		Properties prop = new Properties();
		prop.setProperty("databaseName", dbName);
		prop.setProperty("connectionAttributes", "shutdown=true");
		
		try { 
			getConnectionUsingDataSource(classLoader, prop);
		} catch (SQLException sqle) {
			if(sqle.getSQLState().equals("08006")) {
				System.out.println("Expected exception during shutdown: " 
									+ sqle.getMessage());
			} else
				throw sqle;
		}
	}
	
	/**
	 * Display the sql exception
	 * @param sqle SQLException
	 */
	public static void dumpSQLExceptions(SQLException sqle) {
		do
		{
			System.out.println("SQLSTATE("+sqle.getSQLState()+"): " 
								+ sqle.getMessage());
			sqle = sqle.getNextException();
		} while (sqle != null);
	}
	
	/**
	 * Prints the possible causes for exceptions thrown when trying to
	 *  load classes and invoke methods.
	 * 
	 * @param e Exception
	 */
	private void handleReflectionExceptions(Exception e) {
		System.out.println("FAIL - Unexpected exception - " + e.getMessage());
		System.out.println("Possible Reason - Test could not find the " +
				"location of jar files. Please check if you are running " +
				"with jar files in the classpath. The test does not run with " +
				"classes folder in the classpath. Also, check that old " +
				"jars are checked out from the repository or specified in " +
				"derbyTesting.jar.path property in ant.properties");
		e.printStackTrace();
	}
}
