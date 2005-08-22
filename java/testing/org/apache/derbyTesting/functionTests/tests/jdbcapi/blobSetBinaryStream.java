/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.blobSetBinaryStream

   Copyright 2003, 2005 The Apache Software Foundation or its licensors, as applicable.

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

package org.apache.derbyTesting.functionTests.tests.jdbcapi;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class blobSetBinaryStream {
    
        static String fileName;
        static long fileLength;

        static boolean debug = true;
        private static final String START = "\nSTART: ";

        static
        {
            fileName = "extin/aclob.txt";
        }
        
        public static void main(String[] args)
        {
            System.out.println("Test blob setBinaryStream with multiple writes starting");

            try
            {
                // use the ij utility to read the property file and
                // make the initial connection.
                ij.getPropertyArg(args);
                Connection conn = ij.startJBMS();
                // turn off autocommit, otherwise blobs/clobs cannot hang around
                // until end of transaction
                conn.setAutoCommit(false);

                testBlob1(conn);

                // restart the connection
                conn.commit();
                conn.close();
                System.out.println("FINISHED TEST blobSetBinaryStream :-)");

            }
            catch (Throwable e)
            {
                System.out.println("FAIL -- unexpected exception:" + e.toString());
                if (debug) e.printStackTrace();
            }
            System.out.println("Test blob setBinaryStream with multiple writes finished\n");
        }
        
        private static void testBlob1(Connection conn)
        {
            try {
                System.out.println(START + "testBlob1");

                Statement stmt1 = conn.createStatement();
                stmt1.execute("create table testBlobX1 (a blob(300K), b integer)");
                stmt1.close();

                byte[] b2 = new byte[1];
                b2[0] = (byte)64;
                PreparedStatement stmt2 = conn.prepareStatement(
                        "insert into testBlobX1(a,b) values(?,?)");
                stmt2.setBytes(1,  b2);
                stmt2.setInt(2, 1);
                stmt2.execute();
                stmt2.close();
                
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT a FROM testBlobX1 WHERE b = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Blob blob = rs3.getBlob(1);

                File file = new File(fileName);
                fileLength = file.length();
                InputStream fileIn = new FileInputStream(file);

                if (blob != null) {
                    int count = 0;
                    byte[] buffer = new byte[1024];
                    OutputStream outstream = blob.setBinaryStream(1L);
                    while ((count = fileIn.read(buffer)) != -1) {
                        outstream.write(buffer, 0, count);
                    }
                    outstream.close();
                    fileIn.close();
                    PreparedStatement stmt4 = conn.prepareStatement(
                        "UPDATE testBlobX1 SET a = ? WHERE b = 1");
                    stmt4.setBlob(1,  blob);
                    stmt4.executeUpdate();
                    
                    stmt4.close();
                } else {
                    System.out.println("FAIL -- blob is NULL");
                }

                rs3.close();
                rs3 = stmt3.executeQuery();
                
                if (rs3.next()) {
                    long new_length = rs3.getBlob(1).length();
                    if (new_length != fileLength) {
                        System.out.println(
                                "FAIL -- wrong blob length; original: " + 
                                fileLength + " blob length: " + new_length);
                    }
                } else {
                    System.out.println("FAIL -- blob not found");
                }
                rs3.close();
                stmt3.close();

                System.out.println("testBlob1 finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }
}
