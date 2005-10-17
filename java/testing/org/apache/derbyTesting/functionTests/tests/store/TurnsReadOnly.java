/*

   Derby - Class org.apache.derbyTesting.functionTests.store.LogChecksumSetup

   Copyright 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

/*
 * This class is a test where you are not able to create the lock file
 * when booting an existing database.  The database will then become
 * read-only.  The tests first creates a database and then shutdowns,
 * turns off write access to the database directory and then boots the
 * database again.  A non-default log directory is used since that
 * uncovered a bug (DERBY-555).  (logDevice is set in the
 * _app.properties file)
 *
 * NB! This test is not included in derbyall since it creates a
 * read-only directory which will be annoying when trying to clean
 * test directories.  When Java 6 can be used, it will be possible to
 * turn on write access at the end of the test.
 *
 * @author oystein.grovlen@sun.com
 */

public class TurnsReadOnly
{
    
    public static void main(String[] argv) throws Throwable 
    {
        try {
            ij.getPropertyArg(argv); 
            Connection conn = ij.startJBMS();
            conn.setAutoCommit(true);
            System.out.println("Database has been booted.");

            Statement s = conn.createStatement();
            s.execute("CREATE TABLE t1(a INT)");
            System.out.println("Table t1 created.");

            // Shut down database
            Properties shutdownAttrs = new Properties();
            shutdownAttrs.setProperty("shutdownDatabase", "shutdown");
            System.out.println("Shutting down database ...");
            try {
                DataSource ds = TestUtil.getDataSource(shutdownAttrs);
                ds.getConnection();
            } catch(SQLException se) {
				if (se.getSQLState() != null 
                    && se.getSQLState().equals("XJ015")) {
					System.out.println("Database shutdown completed");
                } else {
                    throw se;
                }
            }

            // Make database directory read-only.
            String derbyHome = System.getProperty("derby.system.home");
            File dbDir = new File(derbyHome, "wombat");
            dbDir.setReadOnly();
            
            // Boot database, check that it is read-only
            conn = ij.startJBMS();
            conn.setAutoCommit(true);
            System.out.println("Database has been booted.");
            s = conn.createStatement();
            try {
                s.execute("INSERT INTO t1 VALUES(1)");
            } catch(SQLException se) {
				if (se.getSQLState() != null 
                    && se.getSQLState().equals("25502")) {
					System.out.println("Database is read-only");
                } else {
                    throw se;
                }
            }

        } catch (SQLException sqle) {
            org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, 
                                                                    sqle);
            sqle.printStackTrace(System.out);
        }
    }
}
