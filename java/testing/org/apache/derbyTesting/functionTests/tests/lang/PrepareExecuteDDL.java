package org.apache.derbyTesting.functionTests.tests.lang;

import org.apache.derbyTesting.functionTests.util.BaseJDBCTestCase;
import org.apache.derbyTesting.functionTests.util.JDBC;

import java.sql.*;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test the dependency system for active statements when 
 * a DDL is executed in a separate connection after the
 * prepare but before the execute.
 *
 */
public class PrepareExecuteDDL extends BaseJDBCTestCase {
	
	private Connection conn;
	private Connection connDDL;
	
	/**
	 * List of statements that are prepared and then executed.
	 * The testPrepareExecute method prepares each statement
	 * in this list, executes one DDL, executes each prepared
	 * statement and then checks the result.
	 * <BR>
	 * The result checking is driven off the initial text
	 * of the statement.
	 */
	private static final String[] STMTS =
	{
		"SELECT * FROM PED001",
		"SELECT A, B FROM PED001",
	};
	
	/**
	 * All the DDL commands that will be executed, one per
	 * fixture, as the mutation between the prepare and execute.
	 */
	private static final String[] DDL =
	{
		"ALTER TABLE PED001 ADD COLUMN D BIGINT",
		"ALTER TABLE PED001 ADD CONSTRAINT PED001_PK PRIMARY KEY (A)",
		"ALTER TABLE PED001 LOCKSIZE ROW",
		"ALTER TABLE PED001 LOCKSIZE TABLE",
		"DROP TABLE PED001",
	};
	
	/**
	 * Create a suite of tests, one per statement in DDL.
	 */
    public static Test suite() {
        TestSuite suite = new TestSuite();
        for (int i = 0; i < DDL.length; i++)
        	suite.addTest(new PrepareExecuteDDL("testPrepareExcute", DDL[i]));
        return suite;
    }
	private final String ddl;
	
	private PrepareExecuteDDL(String name, String ddl)
	{
		super(name);
		this.ddl = ddl;
	}
	
	public void testPrepareExcute() throws SQLException
	{
		PreparedStatement[] psa= new PreparedStatement[STMTS.length];
		for (int i = 0; i < STMTS.length; i++)
		{
			String sql = STMTS[i];
			psa[i] = conn.prepareStatement(sql);
		}
		
		connDDL.createStatement().execute(ddl);
		
		for (int i = 0; i < STMTS.length; i++)
		{
			String sql = STMTS[i];
			if (sql.startsWith("SELECT "))
				checkSelect(psa[i], sql);
		}
	}
	
	private void checkSelect(PreparedStatement ps, String sql)
	throws SQLException
	{
		assertEquals(true, sql.startsWith("SELECT "));
		
		boolean result;
		try {
			result = ps.execute();
		} catch (SQLException e) {
			
			//TODO: Use DMD to see if table exists or not.
			assertSQLState("42X05", e);
			
			return;
		}
		assertTrue(result);
		
		ResultSet rs = ps.getResultSet();
		
		DatabaseMetaData dmd = connDDL.getMetaData();
		JDBC.assertMetaDataMatch(dmd, rs.getMetaData());
		
		boolean isSelectStar = sql.startsWith("SELECT * ");
		
		if (isSelectStar)
			;
		
		JDBC.assertDrainResults(rs);
	}
	
	/**
	 * Set the fixture up with a clean, standard table PED001.
	 */
	protected void setUp() throws SQLException
	{
		
		connDDL = getConnection();
		Statement s = connDDL.createStatement();
		
		s.execute(
		"CREATE TABLE PED001 (A INT NOT NULL, B DECIMAL(6,4), C VARCHAR(20))");
		
		s.close();
		
		conn = getConnection();
	}
	
	/**
	 * Tear-down the fixture by removing the table (if it still
	 * exists).
	 */
	protected void tearDown() throws SQLException
	{
		Statement s = conn.createStatement();
		try {
			s.execute("DROP TABLE PED001");
		} catch (SQLException e) {
			assertSQLState("42Y55", e);
		}
		s.close();
		JDBC.cleanup(conn);
		JDBC.cleanup(connDDL);
		
	}
}