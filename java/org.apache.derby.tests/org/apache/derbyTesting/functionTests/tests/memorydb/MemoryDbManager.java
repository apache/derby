/*

   Derby - Class org.apache.derbyTesting.functionsTests.tests.memorydb.MemoryDbManager

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */
package org.apache.derbyTesting.functionTests.tests.memorydb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.ArrayList;

import org.apache.derbyTesting.junit.BaseJDBCTestCase;

/**
 * Collection of convenience methods for dealing with in-memory databases.
 * The class will keep track of databases, connections and statements
 * created through its methods, and will delete / close these when the
 * clean up method is invoked. This is very much the same as what
 * {@code BaseJDBCTestCase} does, with the exception of deleting the
 * databases.
 * <p>
 * Note: It may be possible to integrate this functionality into the existing
 * JUnit framework, for instance if you want to run the entire test suite with
 * the in-memory back end.
 */
public class MemoryDbManager {

    private static final String ATTR_CREATE = ";create=true";

    /** JDBC protocl prefix used for in-memory databases. */
    private static final String JDBC_PREFIX = "jdbc:derby:memory:";
    /** Shared manager instance. */
    private static final MemoryDbManager DBM = new MemoryDbManager();

    /**
     * Returns a shared manager instance.
     *
     * @return The shared manager instance.
     */
    public static MemoryDbManager getSharedInstance() {
        return DBM;
    }

    /** List of openend statements, closed at clean up. */
    private final ArrayList<Statement> STATEMENTS = new ArrayList<Statement>();
    /** List of openend connections, closed at clean up. */
    private final ArrayList<Connection> CONNECTIONS = new ArrayList<Connection>();
    /** List of created databases, deleted at clean up. */
    private final ArrayList<String> DATABASES = new ArrayList<String>();

    public MemoryDbManager() { }

    /**
     * Creates a new connection to the specified database (url).
     * <p>
     * Note that the specified URL will be appended to a fixed JDBC protcol
     * prefix.
     *
     * @param dbNameAndAttributes database name and any JDBC url attributes
     * @return A connection to the specified database.
     * @throws SQLException if connecting to the database fails
     */
    public Connection getConnection(String dbNameAndAttributes)
            throws SQLException {
        final String url = JDBC_PREFIX + dbNameAndAttributes;
        try {
            DriverManager.getDriver(url);
        } catch (SQLException sqle) {
            // Rely on logic in the default method for obtaining a
            // connection to load the driver.
            new BaseJDBCTestCase("dummy") {}.getConnection();
        }
        Connection con = DriverManager.getConnection(url);
        if (!CONNECTIONS.contains(con)) {
            CONNECTIONS.add(con);
        }
        return con;
    }

    /**
     * Creates a new statement from the given connection and keeps track of
     * it and closes it when the clean up is invoked.
     *
     * @param con the connection to use for creation
     * @return A new statement.
     * @throws SQLException if creating the statement fails
     * @see #cleanUp()
     */
    public Statement createStatement(Connection con)
            throws SQLException {
        Statement stmt = con.createStatement();
        STATEMENTS.add(stmt);
        if (!CONNECTIONS.contains(con)) {
            CONNECTIONS.add(con);
        }
        return stmt;
    }

    /**
     * Creates a new prepared statement from the given connection and keeps
     * track of it and closes it when the clean up is invoked.
     *
     * @param con the connection to use for creation
     * @param sql the sql text to prepare
     * @return A new prepared statement.
     * @throws SQLException if creating the statement fails
     * @see #cleanUp()
     */
    public PreparedStatement prepareStatement(Connection con, String sql)
            throws SQLException {
        PreparedStatement pStmt = con.prepareStatement(sql);
        STATEMENTS.add(pStmt);
        if (!CONNECTIONS.contains(con)) {
            CONNECTIONS.add(con);
        }
        return pStmt;
    }

    /**
     * Drops the specified database.
     * <p>
     * Note that the specified URL will be appended to a fixed JDBC protcol
     * prefix.
     *
     * @param dbNameAndAttributes the database name and any attributes
     *      required to access the database (<em>excluding</em> the delete
     *      attribute, which is added by this method)
     * @throws SQLException if deleting the database fails
     */
    public void dropDatabase(String dbNameAndAttributes)
            throws SQLException {
        String url = JDBC_PREFIX + dbNameAndAttributes + ";drop=true";
        try {
            DriverManager.getConnection(url);
            BaseJDBCTestCase.fail("Dropping database should raise exception.");
        } catch (SQLException sqle) {
            if (sqle.getSQLState().equals("08006")) {
                // Database was deleted.
            } else if (sqle.getSQLState().equals("XJ004")) {
                // Database didn't exist. Already dropped?
            } else {
                BaseJDBCTestCase.assertSQLState("Dropping database failed: (" +
                        sqle.getSQLState() + ") "+ sqle.getMessage(),
                        "08006", sqle);
            }
        }
    }

    /**
     * Creates a new database and keeps track of it to delete it when the
     * clean up is invoked.
     * <p>
     * If the database already exists, a connection to the existing
     * database is returned.
     *
     * @param dbName the database name
     * @return A connection to the database.
     * @throws SQLException if creating or connecting to the database fails
     */
    public Connection createDatabase(String dbName)
            throws SQLException {
        return createDatabase(dbName, null, null, null);
    }

    /**
     * Creates a new database and keeps track of it to delete it when the
     * clean up is invoked.
     * <p>
     * If the database already exists, a connection to the existing
     * database is returned.
     *
     * @param dbName the database name
     * @param dbAttributes database attributes (i.e. encryption)
     * @param user user name
     * @param password user password
     * @return A connection to the database.
     * @throws SQLException if creating or connecting to the database fails
     */
    public Connection createDatabase(String dbName, String dbAttributes,
                                     String user, String password)
            throws SQLException {
        String userAttr = "";
        if (user != null) {
            userAttr = ";user=" + user;
        }
        if (password != null) {
            userAttr += ";password=" + password;
        }
        String url = dbName;
        if (dbAttributes != null) {
            url += ";" + dbAttributes;
        }
        if (!userAttr.equals("")) {
            url += userAttr;
        }
        if (url.indexOf(ATTR_CREATE) == -1) {
            url += ATTR_CREATE;
        }
        Connection con = getConnection(url);
        if (con.getWarnings() != null) {
            // See if there are more than one warning.
            SQLWarning w = con.getWarnings();
            String warnings = w.getMessage();
            while ((w = w.getNextWarning()) != null) {
                warnings += " || " + w.getMessage();
            }
            BaseJDBCTestCase.fail(
                    "Warning(s) when creating database: " + warnings);
        }
        // Keep track of the database we just created, so that we can
        // delete it.
        DATABASES.add(dbName + userAttr);
        return con;

    }

    /**
     * Cleans up database resources by closing known statements and
     * connection, and deleting known in-memory databases.
     * @throws SQLException
     */
    public void cleanUp()
            throws SQLException {
        // Close all known statements.
        for (int i=STATEMENTS.size() -1; i >= 0; i--) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            Statement stmt = STATEMENTS.remove(i);
            stmt.close();
        }
        // Close all known connections.
        for (int i=CONNECTIONS.size() -1; i >= 0; i--) {
            Connection con = CONNECTIONS.remove(i);
            try {
                con.rollback();
            } catch (SQLException sqle) {
                // Ignore this exception.
            }
            con.close();
        }
        // Delete all known databases.
        for (int i=DATABASES.size() -1; i >= 0; i--) {
//IC see: https://issues.apache.org/jira/browse/DERBY-6213
            dropDatabase(DATABASES.remove(i));
        }
    }
}
