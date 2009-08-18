/*
 *
 * Derby - Class org.apache.derbyTesting.junit.JDBC
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

import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.ListIterator;

import junit.framework.Assert;

/**
 * JDBC utility methods for the JUnit tests.
 * Note that JSR 169 is a subset of JDBC 3 and
 * JDBC 3 is a subset of JDBC 4.
 * The base level for the Derby tests is JSR 169.
 */
public class JDBC {
    
    /**
     * Helper class whose <code>equals()</code> method returns
     * <code>true</code> for all strings on this format: SQL061021105830900
     */
    public static class GeneratedId {
        public boolean equals(Object o) {
            // unless JSR169, use String.matches...
            if (JDBC.vmSupportsJDBC3()) 
            {
                return o instanceof String &&
                ((String) o).matches("SQL[0-9]{15}");
            }
            else
            {
                String tmpstr = (String)o;
                boolean b = true;
                if (!(o instanceof String))
                    b = false;
                if (!(tmpstr.startsWith("SQL")))
                    b = false;
                if (tmpstr.length() != 18)
                    b = false;
                for (int i=3 ; i<18 ; i++)
                {
                    if (Character.isDigit(tmpstr.charAt(i)))
                        continue;
                    else
                    {
                        b = false;
                        break;
                    }
                }
            return b;
            }
        }
        public String toString() {
            return "xxxxGENERATED-IDxxxx";
        }
    }

    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just tables.
     */
    public static final String[] GET_TABLES_TABLE = new String[] {"TABLE"};
    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just views.
     */
    public static final String[] GET_TABLES_VIEW = new String[] {"VIEW"};
    /**
     * Constant to pass to DatabaseMetaData.getTables() to fetch
     * just synonyms.
     */
    public static final String[] GET_TABLES_SYNONYM =
        new String[] {"SYNONYM"};
    
    /**
     * Types.SQLXML value without having to compile with JDBC4.
     */
    public static final int SQLXML = 2009;
	
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
    static boolean haveClass(String className)
    {
        try {
            Class.forName(className);
            return true;
        } catch (Exception e) {
        	return false;
        }    	
    }
 	/**
	 * Return true if the virtual machine environment
	 * supports JDBC4 or later. JDBC 4 is a superset
     * of JDBC 3 and of JSR169.
     * <BR>
     * This method returns true in a JDBC 4 environment
     * and false in a JDBC 3 or JSR 169 environment.
	 */
	public static boolean vmSupportsJDBC4()
	{
		return HAVE_DRIVER
	       && HAVE_SQLXML;
	}
 	/**
	 * Return true if the virtual machine environment
	 * supports JDBC3 or later. JDBC 3 is a super-set of JSR169
     * and a subset of JDBC 4.
     * <BR>
     * This method will return true in a JDBC 3 or JDBC 4
     * environment, but false in a JSR169 environment.
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
	 * Return true if the virtual machine environment
	 * supports JSR169. JSR169 is a subset of JDBC 3
     * and hence a subset of JDBC 4 as well.
     * <BR>
     * This method returns true only in a JSR 169
     * environment.
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
        // Need to use the CHAR() function on A.ALIASTYPE
        // so that the compare will work in any schema.
        PreparedStatement psf = conn.prepareStatement(
                "SELECT ALIAS FROM SYS.SYSALIASES A, SYS.SYSSCHEMAS S" +
                " WHERE A.SCHEMAID = S.SCHEMAID " +
                " AND CHAR(A.ALIASTYPE) = 'F' " +
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
                GET_TABLES_VIEW);
		
		dropUsingDMD(s, rs, schema, "TABLE_NAME", "VIEW");
		
		// Tables
		rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);
		
		dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");
        
        // At this point there may be tables left due to
        // foreign key constraints leading to a dependency loop.
        // Drop any constraints that remain and then drop the tables.
        // If there are no tables then this should be a quick no-op.
        ResultSet table_rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);

        while (table_rs.next()) {
            String tablename = table_rs.getString("TABLE_NAME");
            rs = dmd.getExportedKeys((String) null, schema, tablename);
            while (rs.next()) {
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
            rs.close();
        }
        table_rs.close();
        conn.commit();
                
        // Tables (again)
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_TABLE);        
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "TABLE");

        // Synonyms - need work around for DERBY-1790 where
        // passing a table type of SYNONYM fails.
        rs = dmd.getTables((String) null, schema, (String) null,
                GET_TABLES_SYNONYM);
        
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
        if (ddl.isEmpty())
            return;
                
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
     * Assert a result set is empty.
     * If the result set is not empty it will
     * be drained before the check to see if
     * it is empty.
     * The ResultSet is closed by this method.

     */
    public static void assertEmpty(ResultSet rs)
    throws SQLException
    {
        assertDrainResults(rs, 0);
    }
	
    /**
     * 
     * @param rs
     */
    public static void assertClosed(ResultSet rs)
    {
        try { 
            rs.next();
            Assert.fail("ResultSet not closed");
        }catch (SQLException sqle){
            Assert.assertEquals("XCL16", sqle.getSQLState());
        }
        
        
    }
    
    /**
     * Assert that no warnings were returned from a JDBC getWarnings()
     * method such as Connection.getWarnings. Reports the contents
     * of the warning if it is not null.
     * @param warning Warning that should be null.
     */
    public static void assertNoWarnings(SQLWarning warning)
    {
        if (warning == null)
            return;
        
        Assert.fail("Expected no SQLWarnings - got: " + warning.getSQLState() 
                + " " + warning.getMessage());
    }
    
    /**
     * Assert that the statement has no more results(getMoreResults) and it 
     * indeed does not return any resultsets(by checking getResultSet). 
     * Also, ensure that update count is -1.
     * @param s Statement holding no results.
     * @throws SQLException Exception checking results.
     */
    public static void assertNoMoreResults(Statement s) throws SQLException
    {
    	Assert.assertFalse(s.getMoreResults());
        Assert.assertTrue(s.getUpdateCount() == -1);
        Assert.assertNull(s.getResultSet());
    }
    
    /**
     * Assert that a ResultSet representing generated keys is non-null
     * and of the correct type. This method leaves the ResultSet
     * open and does not fetch any date from it.
     * 
     * @param description For assert messages
     * @param keys ResultSet returned from getGeneratedKeys().
     * @throws SQLException
     */
    public static void assertGeneratedKeyResultSet(
            String description, ResultSet keys) throws SQLException
    {
        
        Assert.assertNotNull(description, keys);
        
        // Requirements from section 13.6 JDBC 4 specification
        Assert.assertEquals(
                description + 
                " - Required CONCUR_READ_ONLY for generated key result sets",
                ResultSet.CONCUR_READ_ONLY, keys.getConcurrency());
        
        int type = keys.getType();
        if ( (type != ResultSet.TYPE_FORWARD_ONLY) &&
             (type != ResultSet.TYPE_SCROLL_INSENSITIVE))
        {
            Assert.fail(description +
                    " - Invalid type for generated key result set" + type);
        }
        
        

    }
    
    /**
     * Drain a ResultSet and assert it has at least one row.
     *
     * The ResultSet is closed by this method.
     */
    public static void assertDrainResultsHasData(ResultSet rs)
    throws SQLException
    {
        int rowCount = assertDrainResults(rs, -1);
        Assert.assertTrue("ResultSet expected to have data", rowCount > 0);
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
     * The ResultSet is closed by this method.
	 *
	 * @param rs Result set to drain.
     * @return the number of rows seen.

	 * @throws SQLException
	 */
	public static int assertDrainResults(ResultSet rs)
	    throws SQLException
	{
		return assertDrainResults(rs, -1);
	}

	/**
	 * Does the work of assertDrainResults() as described
	 * above.  If the received row count is non-negative,
	 * this method also asserts that the number of rows
	 * in the result set matches the received row count.
     * 
     * The ResultSet is closed by this method.
	 *
	 * @param rs Result set to drain.
	 * @param expectedRows If non-negative, indicates how
	 *  many rows we expected to see in the result set.
     *  @return the number of rows seen.
	 * @throws SQLException
	 */
	public static int assertDrainResults(ResultSet rs,
	    int expectedRows) throws SQLException
	{
		ResultSetMetaData rsmd = rs.getMetaData();
		
		int rows = 0;
		while (rs.next()) {
			for (int col = 1; col <= rsmd.getColumnCount(); col++)
			{
				String s = rs.getString(col);
				Assert.assertEquals(s == null, rs.wasNull());
                if (rs.wasNull())
                    assertResultColumnNullable(rsmd, col);
			}
			rows++;
		}
		rs.close();

		if (expectedRows >= 0)
			Assert.assertEquals("Unexpected row count:", expectedRows, rows);
        
        return rows;
	}
    
    /**
     * Assert that a column is nullable in its ResultSetMetaData.
     * Used when a utility method checking the contents of a
     * ResultSet sees a NULL value. If the value is NULL then
     * the column's definition in ResultSetMetaData must allow NULLs
     * (or not disallow NULLS).
     * @param rsmd Metadata of the ResultSet
     * @param col Position of column just fetched that was NULL.
     * @throws SQLException Error accessing meta data
     */
    private static void assertResultColumnNullable(ResultSetMetaData rsmd, int col)
    throws SQLException
    {
        Assert.assertFalse(rsmd.isNullable(col) == ResultSetMetaData.columnNoNulls); 
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
     * Takes a result set and an array of expected column types
     * from java.sql.Types 
     * and asserts that the column types in the result
     * set metadata match the number, order, and names of those
     * in the array.
     * 
     * No length information for variable length types
     * can be passed. For ResultSets from JDBC DatabaseMetaData
     * the specification only indicates the types of the
     * columns, not the length.
     *
     * @param rs ResultSet for which we're checking column names.
     * @param expectedTypes Array of expected column types.
     */
    public static void assertColumnTypes(ResultSet rs,
        int[] expectedTypes) throws SQLException
    {
        ResultSetMetaData rsmd = rs.getMetaData();
        int actualCols = rsmd.getColumnCount();

        Assert.assertEquals("Unexpected column count:",
                expectedTypes.length, rsmd.getColumnCount());

        for (int i = 0; i < actualCols; i++)
        {
            Assert.assertEquals("Column types do not match for column " + (i+1),
                    expectedTypes[i], rsmd.getColumnType(i+1));
        }
    }
    /**
     * Takes a Prepared Statement and an array of expected parameter types
     * from java.sql.Types 
     * and asserts that the parameter types in the ParamterMetaData
     * match the number and order of those
     * in the array.
     * @param ps PreparedStatement for which we're checking parameter names.
     * @param expectedTypes Array of expected parameter types.
     */
    public static void assertParameterTypes (PreparedStatement ps,
	        int[] expectedTypes) throws SQLException
	    {
		ParameterMetaData pmd = ps.getParameterMetaData();
	        int actualParams = pmd.getParameterCount();

	        Assert.assertEquals("Unexpected parameter count:",
	                expectedTypes.length, pmd.getParameterCount());

	        for (int i = 0; i < actualParams; i++)
	        {
	            Assert.assertEquals("Types do not match for parameter " + (i+1),
	                    expectedTypes[i], pmd.getParameterType(i+1));
	        }
	    }
    
    /**
     * Check the nullability of the column definitions for
     * the ResultSet matches the expected values.
     * @param rs
     * @param nullability
     * @throws SQLException 
     */
    public static void assertNullability(ResultSet rs,
            boolean[] nullability) throws SQLException
    {
        ResultSetMetaData rsmd = rs.getMetaData();
        int actualCols = rsmd.getColumnCount();

        Assert.assertEquals("Unexpected column count:",
                nullability.length, rsmd.getColumnCount());

        for (int i = 0; i < actualCols; i++)
        {
            int expected = nullability[i] ?
               ResultSetMetaData.columnNullable : ResultSetMetaData.columnNoNulls;
            Assert.assertEquals("Column nullability do not match for column " + (i+1),
                    expected, rsmd.isNullable(i+1));
        }       
    }
    /**
     * Asserts a ResultSet returns a single row with a single
     * column equal to the passed in String value. The value can
     * be null to indicate SQL NULL. The comparision is make
     * using assertFullResultSet in trimmed string mode.
     *  As a side effect, this method closes the ResultSet.
     */
    public static void assertSingleValueResultSet(ResultSet rs,
            String value) throws SQLException
    {
        String[] row = new String[] {value};
        String[][] set = new String[][] {row};
        assertFullResultSet(rs, set);
    }
    
    /**
     * assertFullResultSet() using trimmed string comparisions.
     * Equal to
     * <code>
     * assertFullResultSet(rs, expectedRows, true)
     * </code>
     *  As a side effect, this method closes the ResultSet.
     */
    public static void assertFullResultSet(ResultSet rs,
            String [][] expectedRows)
            throws SQLException
     {
        assertFullResultSet(rs, expectedRows, true);
     }

    /**
     * Takes a result set and a two-dimensional array and asserts
     * that the rows and columns in the result set match the number,
     * order, and values of those in the array.  Each row in
     * the array is compared with the corresponding row in the
     * result set. As a side effect, this method closes the ResultSet.
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
        assertFullResultSet( rs, expectedRows, allAsTrimmedStrings, true );
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
     * @param closeResultSet If true, the ResultSet is closed on the way out.
     */
    public static void assertFullResultSet(ResultSet rs,
        Object [][] expectedRows, boolean allAsTrimmedStrings, boolean closeResultSet)
        throws SQLException
    {
        int rows;
        ResultSetMetaData rsmd = rs.getMetaData();

        // Assert that we have the right number of columns. If we expect an
        // empty result set, the expected column count is unknown, so don't
        // check.
        if (expectedRows.length > 0) {
            Assert.assertEquals("Unexpected column count:",
                expectedRows[0].length, rsmd.getColumnCount());
        }

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

        if ( closeResultSet ) { rs.close(); }

        // And finally, assert the row count.
        Assert.assertEquals("Unexpected row count:", expectedRows.length, rows);
    }

    /**
     * Similar to assertFullResultSet(...) above, except that this
     * method takes a BitSet and checks the received expectedRows
     * against the columns referenced by the BitSet.  So the assumption
     * here is that expectedRows will only have as many columns as
     * there are "true" bits in the received BitSet.
     *
     * This method is useful when we expect there to be a specific
     * ordering on some column OC in the result set, but do not care
     * about the ordering of the non-OC columns when OC is the
     * same across rows.  Ex.  If we have the following results with
     * an expected ordering on column J:
     *
     *   I    J
     *   -    -
     *   a    1
     *   b    1
     *   c    2
     *   c    2
     *
     * Then this method allows us to verify that J is sorted as
     * "1, 1, 2, 2" without having to worry about whether or not
     * (a,1) comes before (b,1).  The caller would simply pass in
     * a BitSet whose content was {1} and an expectedRows array
     * of {{"1"},{"1"},{"2"},{"2"}}.
     *
     * For now this method always does comparisons with
     * "asTrimmedStrings" set to true, and always closes
     * the result set.
     */
    public static void assertPartialResultSet(ResultSet rs,
        Object [][] expectedRows, BitSet colsToCheck)
        throws SQLException
    {
        int rows;

        // Assert that we have the right number of columns. If we expect an
        // empty result set, the expected column count is unknown, so don't
        // check.
        if (expectedRows.length > 0) {
            Assert.assertEquals("Unexpected column count:",
                expectedRows[0].length, colsToCheck.cardinality());
        }

        for (rows = 0; rs.next(); rows++)
        {
            /* If we have more actual rows than expected rows, don't
             * try to assert the row.  Instead just keep iterating
             * to see exactly how many rows the actual result set has.
             */
            if (rows < expectedRows.length)
            {
                assertRowInResultSet(rs, rows + 1,
                    expectedRows[rows], true, colsToCheck);
            }
        }

        rs.close();

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
        assertRowInResultSet(
            rs, rowNum, expectedRow, asTrimmedStrings, (BitSet)null);
    }

    /**
     * See assertRowInResultSet(...) above.
     *
     * @param colsToCheck If non-null then for every bit b
     *   that is set in colsToCheck, we'll compare the (b+1)-th column
     *   of the received result set's current row to the i-th column
     *   of expectedRow, where 0 <= i < # bits set in colsToCheck.
     *   So if colsToCheck is { 0, 3 } then expectedRow should have
     *   two objects and we'll check that:
     *
     *     expectedRow[0].equals(rs.getXXX(1));
     *     expectedRow[1].equals(rs.getXXX(4));
     *
     *   If colsToCheck is null then the (i+1)-th column in the
     *   result set is compared to the i-th column in expectedRow,
     *   where 0 <= i < expectedRow.length.
     */
    private static void assertRowInResultSet(ResultSet rs,
        int rowNum, Object [] expectedRow, boolean asTrimmedStrings,
        BitSet colsToCheck) throws SQLException
    {
        int cPos = 0;
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 0; i < expectedRow.length; i++)
        {
            cPos = (colsToCheck == null)
                ? (i+1)
                : colsToCheck.nextSetBit(cPos) + 1;

            Object obj;
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
                    && (rsmd.getColumnType(cPos) == Types.SMALLINT))
                {
                    String s = expectedRow[i].toString();
                    if (s.equals("true") || s.equals("false"))
                        obj = (rs.getShort(cPos) == 0) ? "false" : "true";
                    else
                        obj = rs.getString(cPos);
                        
                }
                else
                {
                    obj = rs.getString(cPos);

                }
                
                // Trim the rs string.
                if (obj != null)
                    obj = ((String)obj).trim();

            }
            else
                obj = rs.getObject(cPos);

            boolean ok = (rs.wasNull() && (expectedRow[i] == null))
                || (!rs.wasNull()
                    && (expectedRow[i] != null)
                    && (expectedRow[i].equals(obj)
                        || (obj instanceof byte[] // Assumes byte arrays
                            && Arrays.equals((byte[] )obj,
                                             (byte[] )expectedRow[i]))));
            if (!ok)
            {
                Object expected = expectedRow[i];
                Object found = obj;
                if (obj instanceof byte[]) {
                    expected = bytesToString((byte[] )expectedRow[i]);
                    found = bytesToString((byte[] )obj);
                }
                Assert.fail("Column value mismatch @ column '" +
                    rsmd.getColumnName(cPos) + "', row " + rowNum +
                    ":\n    Expected: >" + expected +
                    "<\n    Found:    >" + found + "<");
            }
            
            if (rs.wasNull())
                assertResultColumnNullable(rsmd, cPos);

        }
    }
    
    /**
     * Assert two result sets have the same contents.
     * MetaData is determined from rs1, thus if rs2 has extra
     * columns they will be ignored. The metadata for the
     * two ResultSets are not compared.
     * <BR>
     * The compete ResultSet is walked for both ResultSets,
     * and they are both closed.
     * <BR>
     * Columns are compared as primitive ints or longs, Blob,
     * Clobs or as Strings.
     * @throws IOException 
     */
    public static void assertSameContents(ResultSet rs1, ResultSet rs2)
            throws SQLException, IOException {
        ResultSetMetaData rsmd = rs1.getMetaData();
        int columnCount = rsmd.getColumnCount();
        while (rs1.next()) {
            Assert.assertTrue(rs2.next());
            for (int col = 1; col <= columnCount; col++) {
                switch (rsmd.getColumnType(col)) {
                case Types.SMALLINT:
                case Types.INTEGER:
                    Assert.assertEquals(rs1.getInt(col), rs2.getInt(col));
                    break;
                case Types.BIGINT:
                    Assert.assertEquals(rs1.getLong(col), rs2.getLong(col));
                    break;
                case Types.BLOB:
                    BaseJDBCTestCase.assertEquals(rs1.getBlob(col),
                            rs2.getBlob(col));
                    break;
                case Types.CLOB:
                    BaseJDBCTestCase.assertEquals(rs1.getClob(col),
                            rs2.getClob(col));
                    break;
                default:
                    Assert.assertEquals(rs1.getString(col), rs2.getString(col));
                    break;
                }
                Assert.assertEquals(rs1.wasNull(), rs2.wasNull());
            }
        }
        Assert.assertFalse(rs2.next());
        
        rs1.close();
        rs2.close();
    }

    /**
     * Assert that the ResultSet contains the same rows as the specified
     * two-dimensional array. The order of the results is ignored. Convert the
     * results to trimmed strings before comparing. The ResultSet object will
     * be closed.
     *
     * @param rs the ResultSet to check
     * @param expectedRows the expected rows
     */
    public static void assertUnorderedResultSet(
            ResultSet rs, String[][] expectedRows) throws SQLException {
        assertUnorderedResultSet(rs, expectedRows, true);
    }

    /**
     * Assert that the ResultSet contains the same rows as the specified
     * two-dimensional array. The order of the results is ignored. Objects are
     * read out of the ResultSet with the <code>getObject()</code> method and
     * compared with <code>equals()</code>. If the
     * <code>asTrimmedStrings</code> is <code>true</code>, the objects are read
     * with <code>getString()</code> and trimmed before they are compared. The
     * ResultSet object will be closed when this method returns.
     *
     * @param rs the ResultSet to check
     * @param expectedRows the expected rows
     * @param asTrimmedStrings whether the object should be compared as trimmed
     * strings
     */
    public static void assertUnorderedResultSet(
            ResultSet rs, Object[][] expectedRows, boolean asTrimmedStrings)
                throws SQLException {

        if (expectedRows.length == 0) {
            assertEmpty(rs);
            return;
        }

        ResultSetMetaData rsmd = rs.getMetaData();
        Assert.assertEquals("Unexpected column count",
                            expectedRows[0].length, rsmd.getColumnCount());

        ArrayList expected = new ArrayList(expectedRows.length);
        for (int i = 0; i < expectedRows.length; i++) {
            Assert.assertEquals("Different column count in expectedRows",
                                expectedRows[0].length, expectedRows[i].length);
            if (asTrimmedStrings) {
                ArrayList row = new ArrayList(expectedRows[i].length);
                for (int j = 0; j < expectedRows[i].length; j++) {
                    String val = (String) expectedRows[i][j];
                    row.add(val == null ? null : val.trim());
                }
                expected.add(row);
            } else {
                expected.add(Arrays.asList(expectedRows[i]));
            }
        }

        ArrayList actual = new ArrayList(expectedRows.length);
        while (rs.next()) {
            ArrayList row = new ArrayList(expectedRows[0].length);
            for (int i = 1; i <= expectedRows[0].length; i++) {
                if (asTrimmedStrings) {
                    String s = rs.getString(i);
                    row.add(s == null ? null : s.trim());
                } else {
                    row.add(rs.getObject(i));
                }
                if (rs.wasNull())
                    assertResultColumnNullable(rsmd, i);
            }
            actual.add(row);
        }
        rs.close();

        Assert.assertEquals("Unexpected row count",
                            expectedRows.length, actual.size());

        Assert.assertTrue("Missing rows in ResultSet",
                          actual.containsAll(expected));

        actual.removeAll(expected);
        Assert.assertTrue("Extra rows in ResultSet", actual.isEmpty());
    }

    /**
     * Asserts that the current schema is the same as the one specified.
     *
     * @param con connection to check schema in
     * @param schema expected schema name
     * @throws SQLException if something goes wrong
     */
    public static void assertCurrentSchema(Connection con, String schema)
            throws SQLException {
        Statement stmt = con.createStatement();
        try {
            JDBC.assertSingleValueResultSet(
                    stmt.executeQuery("VALUES CURRENT SCHEMA"), schema);
        } finally {
            stmt.close();
        }
    }

    /**
     * Convert byte array to String.
     * Each byte is converted to a hexadecimal string representation.
     *
     * @param ba Byte array to be converted.
     * @return Hexadecimal string representation. Returns null on null input.
     */
    private static String bytesToString(byte[] ba)
    {
        if (ba == null) return null;
        StringBuffer s = new StringBuffer();
        for (int i = 0; i < ba.length; ++i) {
            s.append(Integer.toHexString(ba[i] & 0x00ff));
        }
        return s.toString();
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
         
        /**
         * Return Type name from jdbc type
         * 
         * @param jdbcType  jdbc type to translate
         */
        public static String sqlNameFromJdbc(int jdbcType) {
            switch (jdbcType) {
            case Types.BIT          :  return "Types.BIT";
            case Types.BOOLEAN  : return "Types.BOOLEAN";
            case Types.TINYINT      :  return "Types.TINYINT";
            case Types.SMALLINT     :  return "SMALLINT";
            case Types.INTEGER      :  return "INTEGER";
            case Types.BIGINT       :  return "BIGINT";
            
            case Types.FLOAT        :  return "Types.FLOAT";
            case Types.REAL         :  return "REAL";
            case Types.DOUBLE       :  return "DOUBLE";
            
            case Types.NUMERIC      :  return "Types.NUMERIC";
            case Types.DECIMAL      :  return "DECIMAL";
            
            case Types.CHAR         :  return "CHAR";
            case Types.VARCHAR      :  return "VARCHAR";
            case Types.LONGVARCHAR  :  return "LONG VARCHAR";
            case Types.CLOB         :  return "CLOB";
            
            case Types.DATE         :  return "DATE";
            case Types.TIME         :  return "TIME";
            case Types.TIMESTAMP    :  return "TIMESTAMP";
            
            case Types.BINARY       :  return "CHAR () FOR BIT DATA";
            case Types.VARBINARY    :  return "VARCHAR () FOR BIT DATA";
            case Types.LONGVARBINARY:  return "LONG VARCHAR FOR BIT DATA";
            case Types.BLOB         :  return "BLOB";

            case Types.OTHER        :  return "Types.OTHER";
            case Types.NULL         :  return "Types.NULL";
            default : return String.valueOf(jdbcType);
                }
        }

       
}
