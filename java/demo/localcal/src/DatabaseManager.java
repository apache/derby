/*
 
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at
 
      http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 
 */

import javax.sql.*;
import java.sql.*;
import org.apache.derby.jdbc.EmbeddedDataSource;

/**
 * A container for the singleton data source, so we don't have to
 * create a separate one for each class that wants to do JDBC
 */
public class DatabaseManager {
    
    private static EmbeddedDataSource ds;
    
    public static String REQUESTS_TABLE = "APP.REQUESTS";
    public static String EVENTS_TABLE   = "APP.EVENTS";
    
    // We want to keep the same connection for a given thread
    // as long as we're in the same transaction
    private static ThreadLocal<Connection> tranConnection = new ThreadLocal();

    private static void initDataSource(String dbname, String user, 
            String password) {
        ds = new EmbeddedDataSource();
        ds.setDatabaseName(dbname);
        ds.setUser(user);
        ds.setPassword(password);
        ds.setCreateDatabase("create");   
    }
    
    public static void logSql() throws Exception {
        executeUpdate("CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(" +
            "'derby.language.logStatementText', 'true')");
    }
    
    public static synchronized void beginTransaction() throws Exception {
        if ( tranConnection.get() != null ) {
            throw new Exception("This thread is already in a transaction");
        }
        Connection conn = getConnection();
        conn.setAutoCommit(false);
        tranConnection.set(conn);
    }
    
    public static void commitTransaction() throws Exception {
        if ( tranConnection.get() == null ) {
            throw new Exception("Can't commit: this thread isn't currently in a " +
                    "transaction");
        }
        tranConnection.get().commit();
        tranConnection.set(null);
    }
    
    public static void rollbackTransaction() throws Exception {
        if ( tranConnection.get() == null ) {
            throw new Exception("Can't rollback: this thread isn't currently in a " +
                    "transaction");
        }
        tranConnection.get().rollback();
        tranConnection.set(null);
    }
        
    /** get a connection */
    public static Connection getConnection() throws Exception {
        if ( tranConnection.get() != null ) {
            return tranConnection.get();
        } else {
            return ds.getConnection();
        }
    }
    
    public static void releaseConnection(Connection conn) throws Exception {
        // We don't close the connection while we're in a transaction,
        // as it needs to be used by others in the same transaction context
        if ( tranConnection.get() == null ) {
            conn.close();
        }
    }
        
    public static void initDatabase(String dbname, String user, String password,
            boolean dropTables) 
        throws Exception {
        initDataSource(dbname, user, password);

        if ( dropTables ) {
            dropTables();
        }
        
        // Assumption: if the requests table doesn't exist, none of the
        // tables exists.  Avoids multiple queries to the database
        if ( ! tableExists("REQUESTS") ) {
            createTables();
        }
    }
    
    private static boolean tableExists(String tablename) throws Exception {
        Connection conn = getConnection();
        ResultSet rs;
        boolean exists;
        
        try {
            DatabaseMetaData md = conn.getMetaData();
        
            rs = md.getTables(null, "APP", tablename, null);
            exists = rs.next();
        } finally {
            releaseConnection(conn);
        }
        
        return exists;
    } 
    
    private static void createTables() throws Exception {
        System.out.println("Creating tables");
        
        executeUpdate(
            "CREATE TABLE " + REQUESTS_TABLE + "(" +
                "sequence_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, " +
                "request_type INTEGER, " +
                "event_id VARCHAR(300), " +
                "date VARCHAR(20), " +
                "title VARCHAR(300), " +
                "edit_url VARCHAR(300))");
                        
        executeUpdate(
            "CREATE TABLE " + EVENTS_TABLE + "(" +
                "event_id VARCHAR(300) PRIMARY KEY, " +
                "date VARCHAR(20), " +
                "title VARCHAR(300), " +
                "edit_url VARCHAR(300), " +
                "version_id VARCHAR(300))");             
    }
    
    /**
     * Drop the tables.  Used mostly for unit testing, to get back
     * to a clean state
     */
    public static void dropTables() throws Exception {
        try {
            executeUpdate("DROP TABLE " + REQUESTS_TABLE);
        } catch ( SQLException sqle ) {
            if (! tableDoesntExist(sqle.getSQLState())) {
                throw sqle;
            }
        }
        
        try {
            executeUpdate("DROP TABLE " + EVENTS_TABLE);
        } catch ( SQLException sqle ) {
            if (! tableDoesntExist(sqle.getSQLState())) {
                throw sqle;
            }
        }
    }
    
    private static boolean tableDoesntExist(String sqlState) {
        return sqlState.equals("42X05") ||
               sqlState.equals("42Y55");
    }
    
    /**
     * Clean out the tables
     */
    public static void clearTables() throws Exception {
        Connection conn = getConnection();
        
        try {
            executeUpdate("DELETE FROM " + REQUESTS_TABLE);
            executeUpdate("DELETE FROM " + EVENTS_TABLE);
        } finally {
            releaseConnection(conn);
        }
        
    }
    
    /**
     * Helper wrapper around boilerplate JDBC code.  Execute a statement
     * that doesn't return results using a PreparedStatment, and returns 
     * the number of rows affected
     */
    public static int executeUpdate(String statement) 
            throws Exception {
        Connection conn = getConnection();
        try {
           PreparedStatement ps = conn.prepareStatement(statement);
           return ps.executeUpdate();
        } finally {
            releaseConnection(conn);
        }
    }
    
    /**
     * Helper wrapper around boilerplat JDBC code.  Execute a statement
     * that returns results using a PreparedStatement that takes no 
     * parameters (you're on your own if you're binding parameters).
     *
     * @return the results from the query
     */
    public static ResultSet executeQueryNoParams(Connection conn, 
            String statement) throws Exception {
       PreparedStatement ps = conn.prepareStatement(statement);
       return ps.executeQuery();
    }
}
