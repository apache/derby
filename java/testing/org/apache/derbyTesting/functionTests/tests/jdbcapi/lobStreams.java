/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbcapi.lobStreams

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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.derby.tools.ij;
import org.apache.derbyTesting.functionTests.util.TestUtil;

public class lobStreams {
    
        static String[] fileName = new String[2];
        static long fileLength;

        static boolean debug = true;
        private static final String START = "\nSTART: ";

        static
        {
            fileName[0] = "extin/aclob.utf";
            fileName[1] = "extin/littleclob.utf";
        }
        
        public static void main(String[] args)
        {
            System.out.println("Test lob stream with multiple writes starting");

            try
            {
                // use the ij utility to read the property file and
                // make the initial connection.
                ij.getPropertyArg(args);
                Connection conn = ij.startJBMS();
                // turn off autocommit, otherwise blobs/clobs cannot hang around
                // until end of transaction
                conn.setAutoCommit(false);

                prepareTable(conn);
                testBlobWrite3Param(conn);
                resetBlobClob(conn);
                testBlobWrite1Param(conn);
                resetBlobClob(conn);
                testClobWrite3Param(conn);
                resetBlobClob(conn);
                testClobWrite1Param(conn);

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
            System.out.println("Test lob stream with multiple writes finished\n");
        }
        
        private static void prepareTable(Connection conn) {
            try {
                Statement stmt1 = conn.createStatement();
                stmt1.execute("create table testBlobX1 (a integer, b blob(300K), c clob(300K))");
                stmt1.close();

                byte[] b2 = new byte[1];
                b2[0] = (byte)64;
                String c2 = "c";
                PreparedStatement stmt2 = conn.prepareStatement(
                        "INSERT INTO testBlobX1(a, b, c) VALUES (?, ?, ?)");
                stmt2.setInt(1, 1);
                stmt2.setBytes(2,  b2);
                stmt2.setString(3,  c2);
                stmt2.execute();
                stmt2.close();

            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
            
        }

        private static void resetBlobClob(Connection conn) {
            try {
                byte[] b2 = new byte[1];
                b2[0] = (byte)64;
                String c2 = "a";
                PreparedStatement stmt = conn.prepareStatement(
                        "UPDATE testBlobX1 SET b = ?, c = ? WHERE a = 1");
                stmt.setBytes(1,  b2);
                stmt.setString(2,  c2);
                stmt.execute();
                stmt.close();

            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
            
        }

        private static void testBlobWrite3Param(Connection conn)
        {
            try {
                System.out.println(START + "testBlobWrite3Param");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT b FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Blob blob = rs3.getBlob(1);

                File file = new File(fileName[0]);
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
                        "UPDATE testBlobX1 SET b = ? WHERE a = 1");
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
                    } else {
                        // Check contents ...
                        InputStream fStream = new FileInputStream(file);
                        InputStream lStream = rs3.getBlob(1).getBinaryStream();

                        if (!compareLob2File(fStream, lStream))
                            System.out.println("FAIL - Blob and file contents do not match");

                        fStream.close();
                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- blob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testBlobWrite3Param finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

        private static void testBlobWrite1Param(Connection conn)
        {
            try {
                System.out.println(START + "testBlobWrite1Param");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT b FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Blob blob = rs3.getBlob(1);

                File file = new File(fileName[1]);
                fileLength = file.length();
                InputStream fileIn = new FileInputStream(file);

                if (blob != null) {
                    int buffer;
                    OutputStream outstream = blob.setBinaryStream(1L);
                    while ((buffer = fileIn.read()) != -1) {
                        outstream.write(buffer);
                    }
                    outstream.close();
                    fileIn.close();
                    
                    PreparedStatement stmt4 = conn.prepareStatement(
                        "UPDATE testBlobX1 SET b = ? WHERE a = 1");
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
                    } else {
                        // Check contents ...
                        InputStream fStream = new FileInputStream(file);
                        InputStream lStream = rs3.getBlob(1).getBinaryStream();

                        if (!compareLob2File(fStream, lStream))
                            System.out.println("FAIL - Blob and file contents do not match");

                        fStream.close();
                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- blob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testBlobWrite1Param finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

        private static void testClobWrite3Param(Connection conn)
        {
            try {
                System.out.println(START + "testClobWrite3Param");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT c FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Clob clob = rs3.getClob(1);

                File file = new File(fileName[0]);
                fileLength = file.length();
                InputStream fileIn = new FileInputStream(file);
                
                if (clob != null) {
                    int count = 0;
                    byte[] buffer = new byte[1024];
                    OutputStream outstream = clob.setAsciiStream(1L);
                    while ((count = fileIn.read(buffer)) != -1) {
                        outstream.write(buffer, 0, count);
                    }
                    outstream.close();
                    fileIn.close();
                    
                    PreparedStatement stmt4 = conn.prepareStatement(
                        "UPDATE testBlobX1 SET c = ? WHERE a = 1");
                    stmt4.setClob(1,  clob);
                    stmt4.executeUpdate();
                    stmt4.close();
                } else {
                    System.out.println("FAIL -- clob is NULL");
                }

                rs3.close();
                rs3 = stmt3.executeQuery();
                
                if (rs3.next()) {
                    long new_length = rs3.getClob(1).length();
                    if (new_length != fileLength) {
                        System.out.println(
                                "FAIL -- wrong clob length; original: " + 
                                fileLength + " clob length: " + new_length);
                    } else {
                        // Check contents ...
                        InputStream fStream = new FileInputStream(file);
                        InputStream lStream = rs3.getClob(1).getAsciiStream();

                        if (!compareLob2File(fStream, lStream))
                            System.out.println("FAIL - Clob and file contents do not match");

                        fStream.close();
                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- clob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testClobWrite3Param finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

        private static void testClobWrite1Param(Connection conn)
        {
            try {
                System.out.println(START + "testClobWrite1Param");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT c FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Clob clob = rs3.getClob(1);

                File file = new File(fileName[1]);
                fileLength = file.length();
                InputStream fileIn = new FileInputStream(file);

                if (clob != null) {
                    int buffer;
                    OutputStream outstream = clob.setAsciiStream(1L);
                    while ((buffer = fileIn.read()) != -1) {
                        outstream.write(buffer);
                    }
                    outstream.close();
                    fileIn.close();
                    
                    PreparedStatement stmt4 = conn.prepareStatement(
                        "UPDATE testBlobX1 SET c = ? WHERE a = 1");
                    stmt4.setClob(1,  clob);
                    stmt4.executeUpdate();
                    stmt4.close();
                    
                } else {
                    System.out.println("FAIL -- clob is NULL");
                }

                rs3.close();
                rs3 = stmt3.executeQuery();
                
                if (rs3.next()) {
                    long new_length = rs3.getClob(1).length();
                    if (new_length != fileLength) {
                        System.out.println(
                                "FAIL -- wrong clob length; original: " + 
                                fileLength + " clob length: " + new_length);
                    } else {
                        // Check contents ...
                        InputStream fStream = new FileInputStream(file);
                        InputStream lStream = rs3.getClob(1).getAsciiStream();

                        if (!compareLob2File(fStream, lStream))
                            System.out.println("FAIL - Clob and file contents do not match");

                        fStream.close();
                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- clob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testClobWrite1Param finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }
        
        private static boolean compareLob2File(InputStream fStream, InputStream lStream) {
            byte[] fByte = new byte[1024];
            byte[] lByte = new byte[1024];
            int lLength = 0, fLength = 0;
            String fString, lString;
            
            try {
                do {
                    fLength = fStream.read(fByte, 0, 1024);
                    lLength = lStream.read(lByte, 0, 1024);
                    if (!java.util.Arrays.equals(fByte, lByte))
                        return false;
                } while (fLength > 0 && lLength > 0);

                fStream.close();
                lStream.close();
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            } 
            return true;
        }
}
