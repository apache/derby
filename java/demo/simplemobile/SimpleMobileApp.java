/*

   Derby - Class SimpleMobileApp

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.derby.jdbc.EmbeddedSimpleDataSource;  // from derby.jar

/**
 * <p>This sample program is a small JDBC application showing how to use and
 * access Derby in a mobile (Java ME, formerly known as J2ME) environment.
 * </p>
 * <p>Instructions for how to run this program are given in 
 * <A HREF=example.html>readme.html</A> located in the same directory as this
 * source file by default.
 *
 * <p>Derby supports <em>embedded</em> access in a mobile Java environment 
 * provided that the Java Virtual Machine supports all of the following:
 * </p>
 * <ul>
 *   <li>Connected Device Configuration (CDC) 1.1 (JSR-218)</li>
 *   <li>Foundation Profile (FP) 1.1 (JSR-219) or better</li>
 *   <li>JDBC Optional package (JSR-169) for Java ME platforms</li>
 * </ul>
 * 
 * <p>(Older versions of Derby may support older versions of the above mentioned
 * specifications. Please refer to release notes or ask on the derby-user
 * mailing list for details.)
 * </p>
 * <p>The main difference between accessing a database in a Java ME environment
 * as opposed to a Java SE or EE environment is that the JSR-169 specification
 * does not include the <code>java.sql.DriverManager</code> class. However, 
 * Derby provides a DataSource class that can be used to obtain connections to 
 * Derby databases: <code>org.apache.derby.jdbc.EmbeddedSimpleDataSource</code>.
 * This is demonstrated in this simple demo application.
 * </p>
 * <p>To compile this application on your own, make sure you include derby.jar 
 * in the compiler's classpath; see <a href="readme.html">readme.html</a> for 
 * details.
 * </p>
 */
public class SimpleMobileApp {

    /**
     * <p>To run this application run your Java launcher with derby.jar and 
     * this class is the classpath. Any arguments to this class will be ignored.
     * </p>
     * <p>Example:
     * </p>
     * <p>
     * <code>&lt;mobileJvm&gt; -cp .:$DERBY_HOME/lib/derby.jar SimpleMobileApp</code>
     * </p>
     * <p>The application will exit with an error if no JDBC (JSR-169 or better) 
     * support is detected.
     * </p>
     * 
     * @param args No arguments required. Any supplied arguments will be ignored.
     */
    public static void main(String[] args) {
        
        SimpleMobileApp demo = new SimpleMobileApp();
        if (vmSupportsJSR169()) {
            demo.runDemo();
        } else {
            System.err.println("No valid JDBC support detected in this JVM. If "
                    + "you are running a Java ME (CDC) JVM, make sure support "
                    + "for the JSR-169 optional package is available to the "
                    + "JVM. Otherwise, make sure your JVM supports JDBC 3.0 or "
                    + "newer.\n");
            System.exit(1);
        }
        System.out.println("SimpleMobileApp finished");
    }
    
    private void runDemo() {
        
        System.out.println("SimpleMobileApp started");
        
        /* If we are using a Java ME (CDC/Foundation) JVM, 
         * we need to use a DataSource to obtain connections to our
         * database, because java.sql.DriverManager is not available.
         * 
         * When using JVMs supporting Java SE we can use either 
         * java.sql.DriverManager or a Datasource to obtain connections. 
         * 
         * If we were to use a DataSource for Java SE, we could use
         * the org.apache.derby.jdbc.EmbeddedDataSource, rather than the
         * org.apache.derby.jdbc.EmbeddedSimpleDataSource that we need to 
         * use for Java ME.
         */
        
        EmbeddedSimpleDataSource ds = new EmbeddedSimpleDataSource();
        
        /*
         * The connection specifies "create" in the DataSource settings for
         * the database to be created when connecting for the first time. 
         * 
         * To remove the database, remove the directory simpleMobileDB and its 
         * contents.
         * 
         * The directory simpleMobileDB will be created in the directory that 
         * the system property <code>derby.system.home</code> points to, or the
         * current directory (<code>user.dir</code>) if derby.system.home is not
         * set.
         */
        
        String dbName = "simpleMobileDB"; // the name of the database
        ds.setDatabaseName(dbName);
        // tell Derby to create the database if it does not already exist
        ds.setCreateDatabase("create"); 
        
        /* We will be using Statement and PreparedStatement objects for 
         * executing SQL. These objects are resources that should be released 
         * explicitly after use, hence the try-catch-finally pattern below.
         */
        Connection conn = null;
        Statement s = null;
        PreparedStatement ps = null;
        ResultSet rs = null;    // used for retreiving the results of a query
        try {
            /* By default, the schema APP will be used when no username is 
             * provided.
             * Otherwise, the schema name is the same as the user name.
             * If you want to use a different schema, or provide a username and
             * password for other reasons, you can connect using:
             * 
             *   Connection conn = ds.getConnection(username, password);
             * or use the
             *    setUser(String) and setPassword(String) methods of
             * EmbeddedSimpleDataSource.
             * 
             * Note that user authentication is off by default, meaning that any
             * user can connect to your database using any password. To enable
             * authentication, see the Derby Developer's Guide.
             */
            conn = ds.getConnection();
            System.out.println("Connected to and created database " + dbName);
            
            /* Creating a statement object that we can use for running various
             * SQL statements commands against the database.*/
            s = conn.createStatement();

            // autoCommit is on by default
            
            /* Create a table... */
            s.execute("create table streetaddr(num int, addr varchar(40))");
            System.out.println("Created table streetaddr");
            
            // Insert some rows...
            s.execute("insert into streetaddr values (1956,'Webster St.')");
            System.out.println("Inserted 1956 Webster");
            s.execute("insert into streetaddr values (1910,'Union St.')");
            System.out.println("Inserted 1910 Union");
            
            // Update some rows...
            
            /* It is recommended to use PreparedStatements whenever you are
             * repeating execution of an SQL statement. PreparedStatements also
             * allows you to parameterize variables. By using PreparedStatements
             * you may increase performance (because the Derby engine does not 
             * have to recompile the SQL statement each time it is executed) and
             * improve security (because of Java type checking).
             */
            
            // use this PreparedStatement for updating a row identified by num
            ps = conn.prepareStatement(
                    "update streetaddr set num=?, addr=? where num=?");
            
            // update one row...
            ps.setInt(1, 180);
            ps.setString(2, "Grand Ave.");
            ps.setInt(3, 1956);
            ps.executeUpdate();
            System.out.println("Updated 1956 Webster to 180 Grand");

            // update another row...
            ps.setInt(1, 300);
            ps.setString(2, "Lakeshore Ave.");
            ps.setInt(3, 180);
            ps.execute();
            System.out.println("Updated 180 Grand to 300 Lakeshore");

            // Select the rows and verify some of the results...
            rs = s.executeQuery("SELECT num, addr FROM streetaddr ORDER BY num");

            // Verification: Number of rows and sorted contents of the num column
            boolean correctResults = true;
            if (!rs.next())
            {
                System.err.println("No rows in table! (ResultSet was empty)");
                correctResults = false;
            } else {
                int num;
                int rows = 0;
                do {
                    rows++;
                    num = rs.getInt(1);
                    if ((rows == 1) && (num != 300)) {
                        System.err.println("Wrong first row returned! "
                                + "Expected num = 300, but got " + num);
                        correctResults = false;
                    } else if ((rows == 2) && (num != 1910)) {
                        System.err.println("Wrong second row returned! "
                        + "Expected num = 1910, but got " + num);
                        correctResults = false;
                    }
                } while (rs.next());
                if (rows !=2) {
                    System.err.println("Wrong number of rows in ResultSet "
                        + "(streetaddr table): " + rows);
                    correctResults = false;
                }
            }

            if (correctResults) {
                System.out.println("Verified the rows");
            } else {
                System.out.println("Verification failed: Wrong results!");
            }

            /* This demo automatically drops the table. This way the demo can
             * be run the same way multiple times. If you want the data to
             * stay in the database, comment out the following Statement
             * execution and recompile the class.
             */
            s.execute("drop table streetaddr");
            System.out.println("Dropped table streetaddr");
            
            // shut down the database
            /* In embedded mode, an application should shut down the database.
             * If the application fails to shut down the database explicitly,
             * the Derby does not perform a checkpoint when the JVM shuts down, 
             * which means that the next connection will be slower because
             * Derby has to perform recovery operations.
             * Explicitly shutting down the database using the appropriate 
             * data source property is recommended.
             * This style of shutdown will always throw an SQLException, but in
             * this case the exception is (usually) not an indication that 
             * something went wrong.
             */
             try {
                 ds.setShutdownDatabase("shutdown");
                 ds.getConnection();
             } catch (SQLException se) {
                 if (!( (se.getErrorCode() == 45000) 
                         && ("08006".equals(se.getSQLState()) ))) {
                    // if the error code or SQLState is different, we have an
                    // unexpected exception (shutdown failed)
                    printSQLException(se);
                    se.printStackTrace();
                 } else {
                     System.out.println(dbName + " shut down successfully");
                 }
             }

        } catch (SQLException e) {
            printSQLException(e);
        } finally {
            // release all open resources to avoid unnecessary memory usage
            
            // ResultSet
            try {
                if (rs != null) {
                    rs.close();
                    rs = null;
                }
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
            
            // Statement
            try {
                if (s != null) {
                    s.close();
                    s = null;
                }
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
            
            //PreparedStatement
            try {
                if (ps != null) {
                    ps.close();
                    ps = null;
                }
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }

            //Connection
            try {
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
            } catch (SQLException sqle) {
                printSQLException(sqle);
            }
        }
    }
    
    /**
     * Prints information about an SQLException to System.err.
     * Use this information to debug the problem.
     * 
     * @param e Some SQLException which info should be printed
     */
    public static void printSQLException(SQLException e) {

        do {
            System.err.println("\n----- SQLException caught: -----");
            System.err.println("  SQLState:   " + e.getSQLState());
            System.err.println("  Error Code: " + e.getErrorCode());
            System.err.println("  Message:    " + e.getMessage());
            //e.printStackTrace(System.err); // enable and recompile to get more info
            System.err.println();
            e = e.getNextException();
        } while (e != null);
    }
    
   /**
    * Checks if this Java Virtual Machine includes support for the JDBC optional
    * package for CDC platforms (JSR-169), or better (JDBC 3.0 or newer), by
    * checking the availability of classes or interfaces introduced in or
    * removed from specific versions of JDBC-related specifications.
    * 
    * @return true if the required JDBC support level is detected, false 
    *         otherwise.
    */
    public static boolean vmSupportsJSR169() {
        if (haveClass("java.sql.Savepoint")) {
            /* New in JDBC 3.0, and is also included in JSR-169.
             * JSR-169 is a subset of JDBC 3 which does not include the Driver 
             * interface.
             * See http://wiki.apache.org/db-derby/VersionInfo for details.
             */
            return true;
        } else {
            return false;
        }
    }
    
   /**
    * Checks if this JVM is able to load a specific class. May for instance
    * be used for determining the level of JDBC support available.
    * @param className Fully qualified name of class to attempt to load.
    * @return true if the class can be loaded, false otherwise.
    */
   private static boolean haveClass(String className) {
       try {
           Class.forName(className);
           return true;
       } catch (Exception e) {
           return false;
       }
   }

}
