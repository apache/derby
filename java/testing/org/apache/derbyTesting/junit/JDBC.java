/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.JDBC
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.junit;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import junit.framework.Assert;

/**
 * JDBC utility methods for the JUnit tests.
 *
 */
public class JDBC {
	
    /**
     * Tell if we are allowed to use DriverManager to create database
     * connections.
     */
    private static final boolean HAVE_DRIVER
                           = haveClass("java.sql.Driver");
    
    /**
     * Does the Savepoint class exist, indicates
     * JDBC 3 (or JSR 169). 
     */
    private static final boolean HAVE_SAVEPOINT
                           = haveClass("java.sql.Savepoint");

    /**
     * Does the java.sql.SQLXML class exist, indicates JDBC 4. 
     */
    private static final boolean HAVE_SQLXML
                           = haveClass("java.sql.SQLXML");

    /**
     * Can we load a specific class, use this to determine JDBC level.
     * @param className Class to attempt load on.
     * @return true if class can be loaded, false otherwise.
     */
    protected static boolean haveClass(String className)
    {
        try {
            Class.forName(className);
            return true;
        } catch (Exception e) {
        	return false;
        }    	
    }
 	/**
 	 * <p>
	 * Return true if the virtual machine environment
	 * supports JDBC4 or later.
	 * </p>
	 */
	public static boolean vmSupportsJDBC4()
	{
		return HAVE_DRIVER
	       && HAVE_SQLXML;
	}
 	/**
 	 * <p>
	 * Return true if the virtual machine environment
	 * supports JDBC3 or later.
	 * </p>
	 */
	public static boolean vmSupportsJDBC3()
	{
		return HAVE_DRIVER
		       && HAVE_SAVEPOINT;
	}

	/**
 	 * <p>
	 * Return true if the virtual machine environment
	 * supports JDBC2 or later.
	 * </p>
	 */
	public static boolean vmSupportsJDBC2()
	{
		return HAVE_DRIVER;
	}

	/**
 	 * <p>
	 * Return true if the virtual machine environment
	 * supports JSR169 (JDBC 3 subset).
	 * </p>
	 */
	public static boolean vmSupportsJSR169()
	{
		return !HAVE_DRIVER
		       && HAVE_SAVEPOINT;
	}	
	
	/**
	 * Rollback and close a connection for cleanup.
	 * Test code that is expecting Connection.close to succeed
	 * normally should just call conn.close().
	 * 
	 * <P>
	 * If conn is not-null and isClosed() returns false
	 * then both rollback and close will be called.
	 * If both methods throw exceptions
	 * then they will be chained together and thrown.
	 * @throws SQLException Error closing connection.
	 */
	public static void cleanup(Connection conn) throws SQLException
	{
		if (conn == null)
			return;
		if (conn.isClosed())
			return;
		
		SQLException sqle = null;
		try {
			conn.rollback();
		} catch (SQLException e) {
			sqle = e;
		}
		
		try {
			conn.close();
		} catch (SQLException e) {
			if (sqle == null)
			    sqle = e;
			else
				sqle.setNextException(e);
			throw sqle;
		}
	}
	
	/**
	 * Drop a database schema by dropping all objects in it
	 * and then executing DROP SCHEMA. If the schema is
	 * APP it is cleaned but DROP SCHEMA is not executed.
	 * 
	 * TODO: Handle dependencies by looping in some intelligent
	 * way until everything can be dropped.
	 * 

	 * 
	 * @param dmd DatabaseMetaData object for database
	 * @param schema Name of the schema
	 * @throws SQLException database error
	 */
	public static void dropSchema(DatabaseMetaData dmd, String schema) throws SQLException
	{		
		Connection conn = dmd.getConnection();
		Assert.assertFalse(conn.getAutoCommit());
		Statement s = dmd.getConnection().createStatement();
        
        // Functions - not supported by JDBC meta data until JDBC 4
        PreparedStatement psf = conn.prepareStatement(
                "SELECT ALIAS FROM SYS.SYSALIASES A, SYS.SYSSCHEMAS S" +
                " WHERE A.SCHEMAID = S.SCHEMAID " +
                " AND A.ALIASTYPE = 'F' " +
                " AND S.SCHEMANAME = ?");
        psf.setString(1, schema);
        ResultSet rs = psf.executeQuery();
        dropUsingDMD(s, rs, schema, "ALIAS", "FUNCTION");        
        psf.close();
  
		// Procedures
		rs = dmd.getProcedures((String) null,
				schema, (String) null);
		
		dropUsingDMD(s, rs, schema, "PROCEDURE_NAME", "PROCEDURE");
		
		// Views
		rs = dmd.getTables((String) null, schema, (String) null,
				new String[] {"VIEW"});
		
		dropUsingDMD(s, rs, schema, "TABLE_NAME", "VIEW");
		
		// Tables
		rs = dmd.getTables((String) null, schema, (String) null,
				new String[] {"TABLE"});
		
		dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");
        
        // At this point there may be tables left due to
        // foreign key constraints leading to a dependency loop.
        // Drop any constraints that remain and then drop the tables.
        // If there are no tables then this should be a quick no-op.
        rs = dmd.getExportedKeys((String) null, schema, (String) null);
        while (rs.next())
        {
            short keyPosition = rs.getShort("KEY_SEQ");
            if (keyPosition != 1)
                continue;
            String fkName = rs.getString("FK_NAME");
            // No name, probably can't happen but couldn't drop it anyway.
            if (fkName == null)
                continue;
            String fkSchema = rs.getString("FKTABLE_SCHEM");
            String fkTable = rs.getString("FKTABLE_NAME");
            
            String ddl = "ALTER TABLE " +
                JDBC.escape(fkSchema, fkTable) +
                " DROP FOREIGN KEY " +
                JDBC.escape(fkName);
            s.executeUpdate(ddl);
        }
        conn.commit();
                
        // Tables (again)
        rs = dmd.getTables((String) null, schema, (String) null,
                new String[] {"TABLE"});        
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");

        // Synonyms - need work around for DERBY-1790 where
        // passing a table type of SYNONYM fails.
        rs = dmd.getTables((String) null, schema, (String) null,
                new String[] {"AA_DERBY-1790-SYNONYM"});
        
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "SYNONYM");
                
		// Finally drop the schema if it is not APP
		if (!schema.equals("APP")) {
			s.executeUpdate("DROP SCHEMA " + JDBC.escape(schema) + " RESTRICT");
		}
		conn.commit();
		s.close();
	}
	
	/**
	 * DROP a set of objects based upon a ResultSet from a
	 * DatabaseMetaData call.
	 * 
	 * TODO: Handle errors to ensure all objects are dropped,
	 * probably requires interaction with its caller.
	 * 
	 * @param s Statement object used to execute the DROP commands.
	 * @param rs DatabaseMetaData ResultSet
	 * @param schema Schema the objects are contained in
	 * @param mdColumn The column name used to extract the object's
	 * name from rs
	 * @param dropType The keyword to use after DROP in the SQL statement
	 * @throws SQLException database errors.
	 */
	private static void dropUsingDMD(
			Statement s, ResultSet rs, String schema,
			String mdColumn,
			String dropType) throws SQLException
	{
		String dropLeadIn = "DROP " + dropType + " ";
		
        // First collect the set of DROP SQL statements.
        ArrayList ddl = new ArrayList();
		while (rs.next())
		{
            String objectName = rs.getString(mdColumn);
            ddl.add(dropLeadIn + JDBC.escape(schema, objectName));
		}
		rs.close();
                
        // Execute them as a complete batch, hoping they will all succeed.
        s.clearBatch();
        int batchCount = 0;
        for (Iterator i = ddl.iterator(); i.hasNext(); )
        {
            Object sql = i.next();
            if (sql != null) {
                s.addBatch(sql.toString());
                batchCount++;
            }
        }

		int[] results;
        boolean hadError;
		try {
		    results = s.executeBatch();
		    Assert.assertNotNull(results);
		    Assert.assertEquals("Incorrect result length from executeBatch",
		    		batchCount, results.length);
            hadError = false;
		} catch (BatchUpdateException batchException) {
			results = batchException.getUpdateCounts();
			Assert.assertNotNull(results);
			Assert.assertTrue("Too many results in BatchUpdateException",
					results.length <= batchCount);
            hadError = true;
		}
		
        // Remove any statements from the list that succeeded.
		boolean didDrop = false;
		for (int i = 0; i < results.length; i++)
		{
			int result = results[i];
			if (result == -3 /* Statement.EXECUTE_FAILED*/)
				hadError = true;
			else if (result == -2/*Statement.SUCCESS_NO_INFO*/)
				didDrop = true;
			else if (result >= 0)
				didDrop = true;
			else
				Assert.fail("Negative executeBatch status");
            
            if (didDrop)
                ddl.set(i, null);
		}
        s.clearBatch();
        if (didDrop) {
            // Commit any work we did do.
            s.getConnection().commit();
        }

        // If we had failures drop them as individual statements
        // until there are none left or none succeed. We need to
        // do this because the batch processing stops at the first
        // error. This copes with the simple case where there
        // are objects of the same type that depend on each other
        // and a different drop order will allow all or most
        // to be dropped.
        if (hadError) {
            do {
                hadError = false;
                didDrop = false;
                for (ListIterator i = ddl.listIterator(); i.hasNext();) {
                    Object sql = i.next();
                    if (sql != null) {
                        try {
                            s.executeUpdate(sql.toString());
                            i.set(null);
                            didDrop = true;
                        } catch (SQLException e) {
                            hadError = true;
                        }
                    }
                }
                if (didDrop)
                    s.getConnection().commit();
            } while (hadError && didDrop);
        }
	}
	
	/**
	 * Assert all columns in the ResultSetMetaData match the
	 * table's defintion through DatabaseMetadDta. Only works
	 * if the complete select list correspond to columns from
	 * base tables.
	 * <BR>
	 * Does not require that the complete set of any table's columns are
	 * returned.
	 * @throws SQLException 
	 * 
	 */
	public static void assertMetaDataMatch(DatabaseMetaData dmd,
			ResultSetMetaData rsmd) throws SQLException
	{
		for (int col = 1; col <= rsmd.getColumnCount(); col++)
		{
			// Only expect a single column back
		    ResultSet column = dmd.getColumns(
		    		rsmd.getCatalogName(col),
		    		rsmd.getSchemaName(col),
		    		rsmd.getTableName(col),
		    		rsmd.getColumnName(col));
		    
		    Assert.assertTrue("Column missing " + rsmd.getColumnName(col),
		    		column.next());
		    
		    Assert.assertEquals(column.getInt("DATA_TYPE"),
		    		rsmd.getColumnType(col));
		    
		    Assert.assertEquals(column.getInt("NULLABLE"),
		    		rsmd.isNullable(col));
		    
		    Assert.assertEquals(column.getString("TYPE_NAME"),
		    		rsmd.getColumnTypeName(col));
		    
		    column.close();
		}
	}
	
	/**
	 * Drain a single ResultSet by reading all of its
	 * rows and columns. Each column is accessed using
	 * getString() and asserted that the returned value
	 * matches the state of ResultSet.wasNull().
	 *
	 * Provides simple testing of the ResultSet when the
	 * contents are not important.
	 *
	 * @param rs Result set to drain.
	 * @throws SQLException
	 */
	public static void assertDrainResults(ResultSet rs)
	    throws SQLException
	{
		assertDrainResults(rs, -1);
	}

	/**
	 * Does the work of assertDrainResults() as described
	 * above.  If the received row count is non-negative,
	 * this method also asserts that the number of rows
	 * in the result set matches the received row count.
	 *
	 * @param rs Result set to drain.
	 * @param expectedRows If non-negative, indicates how
	 *  many rows we expected to see in the result set.
	 * @throws SQLException
	 */
	public static void assertDrainResults(ResultSet rs,
	    int expectedRows) throws SQLException
	{
		ResultSetMetaData rsmd = rs.getMetaData();
		
		int rows = 0;
		while (rs.next()) {
			for (int col = 1; col <= rsmd.getColumnCount(); col++)
			{
				String s = rs.getString(col);
				Assert.assertEquals(s == null, rs.wasNull());
			}
			rows++;
		}
		rs.close();

		if (rows >= 0)
			Assert.assertEquals("Unexpected row count:", expectedRows, rows); 
	}
	
    /**
     * Takes a result set and an array of expected colum names (as
     * Strings)  and asserts that the column names in the result
     * set metadata match the number, order, and names of those
     * in the array.
     *
     * @param rs ResultSet for which we're checking column names.
     * @param expectedColNames Array of expected column names.
     */
    public static void assertColumnNames(ResultSet rs,
        String [] expectedColNames) throws SQLException
    {
        ResultSetMetaData rsmd = rs.getMetaData();
        int actualCols = rsmd.getColumnCount();

        Assert.assertEquals("Unexpected column count:",
            expectedColNames.length, rsmd.getColumnCount());

        for (int i = 0; i < actualCols; i++)
        {
            Assert.assertEquals("Column names do not match:",
                expectedColNames[i], rsmd.getColumnName(i+1));
        }
    }

    /**
     * Takes a result set and a two-dimensional array and asserts
     * that the rows and columns in the result set match the number,
     * order, and values of those in the array.  Each row in
     * the array is compared with the corresponding row in the
     * result set.
     *
     * Will throw an assertion failure if any of the following
     * is true:
     *
     *  1. Expected vs actual number of columns doesn't match
     *  2. Expected vs actual number of rows doesn't match
     *  3. Any column in any row of the result set does not "equal"
     *     the corresponding column in the expected 2-d array.  If
     *     "allAsTrimmedStrings" is true then the result set value
     *     will be retrieved as a String and compared, via the ".equals()"
     *     method, to the corresponding object in the array (with the
     *     assumption being that the objects in the array are all 
     *     Strings).  Otherwise the result set value will be retrieved
     *     and compared as an Object, which is useful when asserting
     *     the JDBC types of the columns in addition to their values.
     *
     * NOTE: It follows from #3 that the order of the rows in the
     * in received result set must match the order of the rows in
     * the received 2-d array.  Otherwise the result will be an
     * assertion failure.
     *
     * @param rs The actual result set.
     * @param expectedRows 2-Dimensional array of objects representing
     *  the expected result set.
     * @param allAsTrimmedStrings Whether or not to fetch (and compare)
     *  all values from the actual result set as trimmed Strings; if
     *  false the values will be fetched and compared as Objects.  For
     *  more on how this parameter is used, see assertRowInResultSet().
     */
    public static void assertFullResultSet(ResultSet rs,
        Object [][] expectedRows, boolean allAsTrimmedStrings)
        throws SQLException
    {
        int rows;
        ResultSetMetaData rsmd = rs.getMetaData();

        // Assert that we have the right number of columns.
        Assert.assertEquals("Unexpected column count:",
            expectedRows[0].length, rsmd.getColumnCount());

        for (rows = 0; rs.next(); rows++)
        {
            /* If we have more actual rows than expected rows, don't
             * try to assert the row.  Instead just keep iterating
             * to see exactly how many rows the actual result set has.
             */
            if (rows < expectedRows.length)
            {
                assertRowInResultSet(rs, rows + 1,
                    expectedRows[rows], allAsTrimmedStrings);
            }
        }

        // And finally, assert the row count.
        Assert.assertEquals("Unexpected row count:", expectedRows.length, rows);
    }

    /**
     * Assert that every column in the current row of the received
     * result set matches the corresponding column in the received
     * array.  This means that the order of the columns in the result
     * set must match the order of the values in expectedRow.
     *
     * <p>
     * If the expected value for a given row/column is a SQL NULL,
     * then the corresponding value in the array should be a Java
     * null.
     *
     * <p>
     * If a given row/column could have different values (for instance,
     * because it contains a timestamp), the expected value of that
     * row/column could be an object whose <code>equals()</code> method
     * returns <code>true</code> for all acceptable values. (This does
     * not work if one of the acceptable values is <code>null</code>.)
     *
     * @param rs Result set whose current row we'll check.
     * @param rowNum Row number (w.r.t expected rows) that we're
     *  checking.
     * @param expectedRow Array of objects representing the expected
     *  values for the current row.
     * @param asTrimmedStrings Whether or not to fetch and compare
     *  all values from "rs" as trimmed Strings.  If true then the
     *  value from rs.getString() AND the expected value will both
     *  be trimmed before comparison.  If such trimming is not
     *  desired (ex. if we were testing the padding of CHAR columns)
     *  then this param should be FALSE and the expected values in
     *  the received array should include the expected whitespace.  
     *  If for example the caller wants to check the padding of a
     *  CHAR(8) column, asTrimmedStrings should be FALSE and the
     *  expected row should contain the expected padding, such as
     *  "FRED    ".
     */
    private static void assertRowInResultSet(ResultSet rs, int rowNum,
        Object [] expectedRow, boolean asTrimmedStrings) throws SQLException
    {
        String s;
        boolean ok;
        Object obj = null;
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 0; i < expectedRow.length; i++)
        {
            if (asTrimmedStrings)
            {
                // Trim the expected value, if non-null.
                if (expectedRow[i] != null)
                    expectedRow[i] = ((String)expectedRow[i]).trim();

                /* Different clients can return different values for
                 * boolean columns--namely, 0/1 vs false/true.  So in
                 * order to keep things uniform, take boolean columns
                 * and get the JDBC string version.  Note: since
                 * Derby doesn't have a BOOLEAN type, we assume that
                 * if the column's type is SMALLINT and the expected
                 * value's string form is "true" or "false", then the
                 * column is intended to be a mock boolean column.
                 */
                if ((expectedRow[i] != null)
                    && (rsmd.getColumnType(i+1) == Types.SMALLINT))
                {
                    s = expectedRow[i].toString();
                    if (s.equals("true") || s.equals("false"))
                        obj = (rs.getShort(i+1) == 0) ? "false" : "true";
                }
                else
                {
                    obj = rs.getString(i+1);

                    // Trim the rs string.
                    if (obj != null)
                        obj = ((String)obj).trim();
                }

            }
            else
                obj = rs.getObject(i+1);

            ok = (rs.wasNull() && (expectedRow[i] == null))
                || (!rs.wasNull()
                    && (expectedRow[i] != null)
                    && expectedRow[i].equals(obj));

            if (!ok)
            {
                Assert.fail("Column value mismatch @ column '" +
                    rsmd.getColumnName(i+1) + "', row " + rowNum +
                    ":\n    Expected: " + expectedRow[i] +
                    "\n    Found:    " + obj);
            }
        }
    }

	/**
	 * Escape a non-qualified name so that it is suitable
	 * for use in a SQL query executed by JDBC.
	 */
	public static String escape(String name)
	{
		return "\"" + name + "\"";
	}	
	/**
	 * Escape a schama-qualified name so that it is suitable
	 * for use in a SQL query executed by JDBC.
	 */
	public static String escape(String schema, String name)
	{
		return "\"" + schema + "\".\"" + name + "\"";
	}

}
