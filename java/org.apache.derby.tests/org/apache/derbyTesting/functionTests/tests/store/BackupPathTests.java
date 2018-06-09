/*

   Derby - Class org.apache.derbyTesting.functionTests.store.BackupPathTests

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

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.CallableStatement;
import java.io.File;
import org.apache.derby.tools.ij;

/*
 * This class tests online backup with various types of paths.
 * 1 ) backup path is same as a database directory. This should fail backup
 *     can not be made onto a database directory. (DERBY-304 bug).
 * 2 ) backup path is a sub directory in the database. 
 * 3 ) Redo backup with same path as in second case.
 * 4 ) backup path is absolute path.

 * If the path refers to some sub directory inside a database, backup 
 * will succeed because there is no easy way to catch this weird case,
 * especially if the backup path refers to another database directory.  
 *
 *
 * @version 1.0
 */

public class BackupPathTests
{
    
    public static void main(String[] argv) throws Throwable 
    {
        try {

            ij.getPropertyArg(argv); 
            Connection conn = ij.startJBMS();
            conn.setAutoCommit(true);
            
            Statement stmt = conn.createStatement();
            //install a jar, so that there is a jar directory under the db.
            stmt.execute(
                     "call sqlj.install_jar(" + 
                     "'extin/brtestjar.jar', 'math_routines', 0)");

            stmt.close();

            logMsg("Begin Backup Path Tests");
            String derbyHome = System.getProperty("derby.system.home");
            String dbHome = derbyHome + File.separator + "wombat" ; 

            logMsg("case1 : try Backup with backup path as database dir");
            try {
                performBackup(conn, dbHome);
            } catch(SQLException sqle) {
                // expected to fail with following error code. 
                if (sqle.getSQLState() != null && 
                    sqle.getSQLState().equals("XSRSC")) {
                    logMsg("Backup in to a database dir failed");
                } else {
                    throw sqle;
                }
            }
            
            logMsg("End test case1");
            logMsg("case2 : Backup with backup path as database jar dir");
            String jarDir = dbHome + File.separator + "jar";
            performBackup(conn, jarDir);
            logMsg("End test case 2");

            logMsg("case 3: Backup again into the same db jar dir location");
            performBackup(conn, jarDir);
            logMsg("End test case 3");

            logMsg("case 4: Backup using an absolute path");
            String absBackupPath = 
                new File("extinout/backupPathTests").getAbsolutePath();
            performBackup(conn, absBackupPath); 
            logMsg("End test case 4");
            conn.close();
            logMsg("End Backup Path Tests");

        } catch (SQLException sqle) {
            org.apache.derby.tools.JDBCDisplayUtil.ShowSQLException(System.out, 
                                                                    sqle);
            sqle.printStackTrace(System.out);
        }
    }


    private static void performBackup(Connection conn, 
                                      String backupPath) 
        throws SQLException
    {
        CallableStatement backupStmt = 	
            conn.prepareCall("CALL SYSCS_UTIL.SYSCS_BACKUP_DATABASE(?)");
        backupStmt.setString(1, backupPath);
        backupStmt.execute();
        backupStmt.close();
    }

    
    /**
     * Write message to the standard output.
     */
    private static void logMsg(String   str)	{
        System.out.println(str);
    }

}
