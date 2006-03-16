/* IBM Confidential
 *
 * Product ID: 5697-F53
 *
 * (C) Copyright IBM Corp. 2003.
 *
 * The source code for this program is not published or otherwise divested
 * of its trade secrets, irrespective of what has been deposited with the
 * U.S. Copyright Office.
 */

package org.apache.derbyTesting.upgradeTests;

import java.sql.*;

import org.apache.derbyTesting.functionTests.tests.jdbcapi.metadata;

/**
	Tests upgrades including soft upgrade.
	
	Test is broken into phases, by running this main java program separately
	each time, with either the old release or the new release in the class
	path and the indication of the phase. The phases are:
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
	A script is provided runphases.ksh to run the phases with
	correct old and new classpaths and phase numbers. Currently
	checking for pass/failure is a manual process of looking
	at the output.
	
    <P>
	This tests the following specifically.

	10.1 Upgrade issues

	<UL>
	<LI> Routines with explicit Java signatures.
	</UL>
*/



public class phaseTester {

	private static final String[] PHASES =
	{"CREATE", "SOFT UPGRADE", "POST SOFT UPGRADE", "UPGRADE", "POST UPGRADE"};

	/** test run at create of original database with old version */
	static final int PH_CREATE = 0;
	/** test run in soft upgrade mode with new version */
	static final int PH_SOFT_UPGRADE = 1;
	/** test run in post soft upgrade mode with old version */
	static final int PH_POST_SOFT = 2;
	/** test run in hard upgrade mode with new version */
	static final int PH_HARD_UPGRADE = 3;
	/** test run in post hard upgrade mode with old version */
	static final int PH_POST_HARD_UPGRADE = 4;

	/**
		arg0 = jdbc url
		arg1 = phase
		arg2 = dbmajor version (of original database)
		arg3 = dbminor version (of original database)
		arg4 = tempdbname (just for version checking)
	*/
	public static void main(String[] args) throws Exception {

		String dbName = args[0];
		int phase = Integer.valueOf(args[1]).intValue();
		int dbMajor = Integer.valueOf(args[2]).intValue();
		int dbMinor = Integer.valueOf(args[3]).intValue();
		String tempDbName = args[4].concat(args[1]);


		new org.apache.derby.jdbc.EmbeddedDriver();

		String url;
		switch (phase) {
		case PH_CREATE:
			url = "jdbc:derby:" + dbName + ";create=true";
			break;
		case PH_SOFT_UPGRADE:
		case PH_POST_SOFT:
		case PH_POST_HARD_UPGRADE:
			url = "jdbc:derby:" + dbName;
			break;
		case PH_HARD_UPGRADE:
			url = "jdbc:derby:" + dbName + ";upgrade=true";
			break;
		default:
			url = "jdbc:unknownphase:";
			break;
		}

		System.out.println("\n\nSTART - phase " + PHASES[phase] + " db version " + dbMajor + "." + dbMinor);
		System.out.println("jdbc url is " + url);

		Connection conn;
		
		try {
			conn = DriverManager.getConnection(url);
		} catch (SQLException sqle) {
			do {
				System.out.println(sqle.toString());
				//sqle.printStackTrace(System.out);
				sqle = sqle.getNextException();
			} while (sqle != null);
			conn = null;
		}

		String pvs;
		if (conn != null) {

			pvs = conn.getMetaData().getDatabaseProductVersion();
			System.out.println("Engine " + conn.getMetaData().getDatabaseProductName() + " " + pvs);
		} else {
			// just create a database with this engine to get the version info.
			Connection ct = DriverManager.getConnection("jdbc:derby:" + tempDbName + ";create=true");

			pvs = ct.getMetaData().getDatabaseProductVersion();
			System.out.println("Engine " + ct.getMetaData().getDatabaseProductName() + " " + pvs);
			ct.close();
		}

		pvs = pvs.toLowerCase();

		boolean isBeta = pvs.indexOf("beta") != -1 || pvs.indexOf("alpha") != -1;
		
		if (isBeta) {
			
			boolean allowPreReleaseUpgrade = 
				Boolean.getBoolean("derby.database.allowPreReleaseUpgrade");
			
			System.out.println("Pre-release Software - derby.database.allowPreReleaseUpgrade="
					+ allowPreReleaseUpgrade);
			
			if (allowPreReleaseUpgrade)
				isBeta = false;
		}


		/*
		** Now run the tests.
		*/

		boolean passed = true;

		passed = caseConnectionCheck(conn, phase, dbMajor, dbMinor, isBeta) && passed;

		if (conn != null && !conn.isClosed()) {

			passed = caseReusableRecordIdSequenceNumber(conn, phase, dbMajor, dbMinor, isBeta) && passed;
			passed = caseInitialize(conn, phase, dbMajor, dbMinor, isBeta) && passed;
			passed = caseProcedures(conn, phase, dbMajor, dbMinor, isBeta) && passed;

			setPhaseComplete(conn, phase, passed);

			//test the metadata calls at this stages of the db. This is to make
			//sure that they don't break between these forms of upgrades of a db
			metadata metadataTest = new metadata();
			metadataTest.con = conn;
			metadata.s = conn.createStatement();
			metadataTest.runTest();
		}

		System.out.println("END - " + (passed ? "PASS" : "FAIL") + " - phase " + PHASES[phase] + " db version " + dbMajor + "." + dbMinor);

		if (conn != null && !conn.isClosed())
			conn.close();

	}


	/**
		Test case template
	*/
	static boolean caseTemplate(Connection conn, int phase, int dbMajor, int dbMinor, boolean isBeta)
		throws SQLException {

		boolean passed = true;

		switch (phase) {
		case PH_CREATE:
			break;
		case PH_SOFT_UPGRADE:
			break;
		case PH_POST_SOFT:
			break;
		case PH_HARD_UPGRADE:
			break;
		default:
			passed = false;
			break;
		}

		System.out.println("complete caseTemplate - passed " + passed);
		return passed;
	}


	static boolean caseConnectionCheck(Connection conn, int phase, int dbMajor, int dbMinor, boolean isBeta)
		throws SQLException {

		boolean passed = true;
		boolean needConn;

		switch (phase) {
		case PH_CREATE:
			needConn = true;
			break;

		case PH_SOFT_UPGRADE:
			needConn = !isBeta;
			break;
	
		case PH_POST_SOFT:
			needConn = true;
			break;
			
		case PH_HARD_UPGRADE:
			needConn = !isBeta;
			break;

		case PH_POST_HARD_UPGRADE:
			needConn = false;
			break;

		default:
			needConn = false;
			passed = false;
			break;
		}

		if (needConn)
		{
			if (conn == null) {
				System.out.println("FAIL - NO CONNECTION");
				passed = false;
			}
		}
		else
		{
			if (conn != null) {
				System.out.println("FAIL - CONNECTION SUCCEEDED");
				passed = false;
				conn.close();				conn = null;
			}
		}

		System.out.println("complete caseConnectionCheck - passed " + passed);
		return passed;
	}

	/**
	*/
	static boolean caseInitialize(Connection conn, int phase, int dbMajor, int dbMinor, boolean isBeta)
		throws SQLException {

		boolean passed = true;

		switch (phase) {
		case PH_CREATE:
			conn.createStatement().executeUpdate("CREATE TABLE PHASE(id INT NOT NULL, ok INT)");
			conn.createStatement().executeUpdate("CREATE TABLE TABLE1(id INT NOT NULL PRIMARY KEY, name varchar(200))");
			break;
		case PH_SOFT_UPGRADE:
			break;
		case PH_POST_SOFT:
			break;
		case PH_HARD_UPGRADE:
			break;
		default:
			passed = false;
			break;
		}

		PreparedStatement ps = conn.prepareStatement("INSERT INTO PHASE(id) VALUES (?)");
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
		ps = conn.prepareStatement("UPDATE TABLE1 set name = name || 'U' where id = ?");
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

	static void setPhaseComplete(Connection conn, int phase, boolean passed) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("UPDATE PHASE SET ok = ? where id = ?");
		ps.setInt(1, passed ? 1 : 0);
		ps.setInt(2, phase);
		ps.executeUpdate();
		ps.close();
	}
	static boolean checkPhaseComplete(Connection conn, int phase) throws SQLException {

		PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM PHASE where id = ? and ok = 1");
		ps.setInt(1, phase);
		ResultSet rs = ps.executeQuery();
		rs.next();
		int count = rs.getInt(1);
		rs.close();
		ps.close();
		return count == 1;
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
											   int dbMajor, int dbMinor, 
											   boolean isBeta)
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
				System.out.println("Running compress");
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
		case PH_POST_SOFT: {
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
		System.out.println("complete caseReusableRecordIdSequenceNumber  - passed " + passed);
		return passed;
	}
	
	/*
	** Procedures
	*  10.1 - Check that a procedure with a signature can not be added if the
	*  on-disk database version is 10.0.
	*/

	static boolean caseProcedures(Connection conn, int phase, int dbMajor, int dbMinor, boolean isBeta)
		throws SQLException {
		
		boolean signaturesAllowedInOldRelease =
			dbMajor > 10 || (dbMajor == 10 && dbMinor >= 1);

		boolean passed = true;

		switch (phase) {
		case PH_CREATE:
			break;
		case PH_SOFT_UPGRADE:
			
			try {
				conn.createStatement().execute("CREATE PROCEDURE GC() LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc()'");
				if (!signaturesAllowedInOldRelease)
				{
					System.out.println("FAIL : created procedure with signature");
					passed = false;
				}
			} catch (SQLException sqle) {
				if (signaturesAllowedInOldRelease || !"XCL47".equals(sqle.getSQLState())) {
					System.out.println("FAIL " + sqle.getSQLState() + " -- " + sqle.getMessage());
					passed = false;
				}
			}
			break;
		case PH_POST_SOFT:
			try {
				conn.createStatement().execute("CALL GC()");
				if (!signaturesAllowedInOldRelease)
					System.out.println("FAIL : procedure was created in soft upgrade!");
					
			} catch (SQLException sqle)
			{
				if (signaturesAllowedInOldRelease)
					System.out.println("FAIL : procedure was created not in soft upgrade!" + sqle.getMessage());
			}
			break;
		case PH_HARD_UPGRADE:
			if (!signaturesAllowedInOldRelease)
				conn.createStatement().execute("CREATE PROCEDURE GC() LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME 'java.lang.System.gc()'");
			conn.createStatement().execute("CALL GC()");
			break;
		default:
			passed = false;
			break;
		}

		System.out.println("complete caseProcedures - passed " + passed);
		return passed;
	}


}
