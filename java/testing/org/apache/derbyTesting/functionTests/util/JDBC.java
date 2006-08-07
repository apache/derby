/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.util.JDBC
 *
 * Copyright 2006 The Apache Software Foundation or its 
 * licensors, as applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.util;

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
	 * Provides simple testing of the ResultSet when then contents
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
}
