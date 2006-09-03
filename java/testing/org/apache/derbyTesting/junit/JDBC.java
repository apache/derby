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
    private static boolean haveClass(String className)
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

        // Synonyms - need work around for DERBY-1790 where
        // passing a table type of SYNONYM fails.
        rs = dmd.getTables((String) null, schema, (String) null,
                new String[] {"AA_DERBY-1790-SYNONYM"});
        
        dropUsingDMD(s, rs, schema, "TABLE_NAME", "SYNONYM");
        
		// Finally drop the schema if it is not APP
		if (!schema.equals("APP")) {
			s.execute("DROP SCHEMA " + JDBC.escape(schema) + " RESTRICT");
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
		
		s.clearBatch();
		int batchCount = 0;
		while (rs.next())
		{
            String objectName = rs.getString(mdColumn);
			s.addBatch(dropLeadIn + JDBC.escape(schema, objectName));
			batchCount++;
		}
		rs.close();
		int[] results;
		try {
		    results = s.executeBatch();
		    Assert.assertNotNull(results);
		    Assert.assertEquals("Incorrect result length from executeBatch",
		    		batchCount, results.length);
		} catch (BatchUpdateException batchException) {
			results = batchException.getUpdateCounts();
			Assert.assertNotNull(results);
			Assert.assertTrue("Too many results in BatchUpdateException",
					results.length <= batchCount);
		}
		
		boolean hadError = false;
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
		}
		
		// Commit any work we did do.
		s.getConnection().commit();
		s.clearBatch();
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
	 * Provides simple testing of the ResultSet when the contents
	 * are not important.
	 * @param rs
	 * @throws SQLException
	 */
	public static void assertDrainResults(ResultSet rs)
	    throws SQLException
	{
		ResultSetMetaData rsmd = rs.getMetaData();
		
		while (rs.next()) {
			for (int col = 1; col <= rsmd.getColumnCount(); col++)
			{
				String s = rs.getString(col);
				Assert.assertEquals(s == null, rs.wasNull());
			}
		}
		rs.close();
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
