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
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
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
        static String sep;
        static long fileLength;

        static boolean debug = true;
        private static final String START = "\nSTART: ";
		
		private static final String unicodeTestString = "This is a test string containing a few " +
				"non-ascii characters:\nÆØÅ and æøå are used in norwegian: 'Blåbærsyltetøy' means" +
				"'blueberry jam', and tastes great on pancakes. =)";

        static
        {
//            fileName[0] = "extin" + sep + "aclob.utf";
 //           fileName[1] = "extin" + sep + "littleclob.utf";
            fileName[0] =  "aclob.utf";
            fileName[1] =  "littleclob.utf";
        }
        
        public static void main(String[] args)
        {
            System.out.println("Test lob stream with multiple writes starting");

            // check to see if we have the correct extin path, if the files aren't right here, try one more time
	    boolean exists = (new File("extin", fileName[0])).exists();
            String sep =  System.getProperty("file.separator");
	    if (!exists) 
            {
                // assume it's in a dir up, if that's wrong too, too bad...
                String userdir =  System.getProperty("user.dir");
                fileName[0] = userdir + sep + ".." + sep + "extin" + sep + fileName[0];
                fileName[1] = userdir + sep + ".." + sep + "extin" + sep + fileName[1];
            }
            else
            {
                // assume it's in a dir up, if that's wrong too, too bad...
                fileName[0] = "extin" + sep + fileName[0];
                fileName[1] = "extin" + sep + fileName[1];
            }


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
                testClobAsciiWrite3Param(conn);
                resetBlobClob(conn);
                testClobAsciiWrite1Param(conn);
                resetBlobClob(conn);
                testClobCharacterWrite3ParamChar(conn);
                resetBlobClob(conn);
                testClobCharacterWrite3ParamString(conn);
                resetBlobClob(conn);
                testClobCharacterWrite1ParamString(conn);
                resetBlobClob(conn);
                testClobCharacterWrite1Char(conn);

                // restart the connection
                conn.commit();
                cleanUp(conn);
                conn.commit();
                conn.close();

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

		/**
		 * Tests the BlobOutputStream.write(byte  b[], int off, int len) method
		 **/
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

		/**
		 * Tests the BlobOutputStream.write(int b) method
		 **/
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

		/**
		 * Tests the ClobOutputStream.write(byte  b[], int off, int len) method
		 **/
        private static void testClobAsciiWrite3Param(Connection conn)
        {
            try {
                System.out.println(START + "testClobAsciiWrite3Param");
               
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
               
                System.out.println("testClobAsciiWrite3Param finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

		/**
		 * Tests the ClobOutputStream.write(int b) method
		 **/
        private static void testClobAsciiWrite1Param(Connection conn)
        {
            try {
                System.out.println(START + "testClobAsciiWrite1Param");
               
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
               
                System.out.println("testClobAsciiWrite1Param finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

		/**
		 * Tests the ClobWriter.write(char cbuf[], int off, int len) method
		 **/
        private static void testClobCharacterWrite3ParamChar(Connection conn)
        {
            try {
                System.out.println(START + "testClobCharacterWrite3ParamChar");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT c FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Clob clob = rs3.getClob(1);
                char[] testdata = unicodeTestString.toCharArray();
				

                if (clob != null) {
                    Writer clobWriter = clob.setCharacterStream(1L);
                    clobWriter.write(testdata, 0, testdata.length);
                    clobWriter.close();
                    
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
                    if (new_length != testdata.length) {
                        System.out.println(
                                "FAIL -- wrong clob length; original: " + 
                                testdata.length + " clob length: " + new_length);
                    } else {
                        // Check contents ...
                        Reader lStream = rs3.getClob(1).getCharacterStream();

                        if (!compareClobReader2CharArray(testdata, lStream))
                            System.out.println("FAIL - Clob and buffer contents do not match");

                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- clob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testClobCharacterWrite3ParamChar finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

		/**
		 * Tests the ClobWriter.write(String str, int off, int len) method
		 **/
		private static void testClobCharacterWrite3ParamString(Connection conn)
        {
            try {
                System.out.println(START + "testClobCharacterWrite3ParamString");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT c FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Clob clob = rs3.getClob(1);
				

                if (clob != null) {
                    Writer clobWriter = clob.setCharacterStream(1L);
                    clobWriter.write(unicodeTestString, 0, unicodeTestString.length());
                    clobWriter.close();
                    
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
                    if (new_length != unicodeTestString.length()) {
                        System.out.println(
                                "FAIL -- wrong clob length; original: " + 
                                unicodeTestString.length() + " clob length: " + new_length);
                    } else {
                        // Check contents ...
                        Reader lStream = rs3.getClob(1).getCharacterStream();

                        if (!compareClobReader2CharArray(unicodeTestString.toCharArray(), lStream))
                            System.out.println("FAIL - Clob and buffer contents do not match");

                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- clob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testClobCharacterWrite3ParamString finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }

		/**
		 * Tests the ClobWriter.write(String str) method
		 **/
		private static void testClobCharacterWrite1ParamString(Connection conn)
        {
            try {
                System.out.println(START + "testClobCharacterWrite1ParamString");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT c FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Clob clob = rs3.getClob(1);
				

                if (clob != null) {
                    Writer clobWriter = clob.setCharacterStream(1L);
                    clobWriter.write(unicodeTestString);
                    clobWriter.close();
                    
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
                    if (new_length != unicodeTestString.length()) {
                        System.out.println(
                                "FAIL -- wrong clob length; original: " + 
                                unicodeTestString.length() + " clob length: " + new_length);
                    } else {
                        // Check contents ...
                        Reader lStream = rs3.getClob(1).getCharacterStream();

                        if (!compareClobReader2CharArray(unicodeTestString.toCharArray(), lStream))
                            System.out.println("FAIL - Clob and buffer contents do not match");

                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- clob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testClobCharacterWrite1ParamString finished");
            } catch (SQLException e) {
                TestUtil.dumpSQLExceptions(e);
            } catch (Throwable e) {
                if (debug) e.printStackTrace();
            }
        }
		/**
		 * Tests the ClobWriter.write(int c) method
		 **/
		private static void testClobCharacterWrite1Char(Connection conn)
        {
            try {
                System.out.println(START + "testClobCharacterWrite1Char");
               
                PreparedStatement stmt3 = conn.prepareStatement(
                    "SELECT c FROM testBlobX1 WHERE a = 1");
                
                ResultSet rs3 = stmt3.executeQuery();
                
                rs3.next();

                Clob clob = rs3.getClob(1);
				
				char testchar = 'a';

                if (clob != null) {
                    Writer clobWriter = clob.setCharacterStream(1L);
                    clobWriter.write(testchar);
                    clobWriter.close();
                    
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
					Clob fish = rs3.getClob(1);
                    if (new_length != 1) {
                        System.out.println(
                                "FAIL -- wrong clob length; original: " + 
                                1 + " clob length: " + new_length);
                    } else {
                        // Check contents ...
                        Reader lStream = rs3.getClob(1).getCharacterStream();
						char clobchar = (char) lStream.read();
						
                        if (clobchar != testchar)
                            System.out.println("FAIL - fetched Clob and original contents do not match");

                        lStream.close();
                        
                    }
                } else {
                    System.out.println("FAIL -- clob not found");
                }
                rs3.close();
                stmt3.close();
               
                System.out.println("testClobCharacterWrite1Char finished");
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

		private static boolean compareClobReader2CharArray(char[] cArray, Reader charReader) {
			char[] clobChars = new char[cArray.length];

			int readChars = 0;
			int totalCharsRead = 0;

			try {
				do {
					readChars = charReader.read(clobChars, totalCharsRead, cArray.length - totalCharsRead);
					if (readChars != -1) 
						totalCharsRead += readChars;
				} while (readChars != -1 && totalCharsRead < cArray.length);
				charReader.close();
				if (!java.util.Arrays.equals(cArray, clobChars))
					return false;

			} catch (Throwable e) {
				if (debug) e.printStackTrace();
			}
			return true;
		}

		
        private static void cleanUp(Connection conn) throws SQLException {
            String[] testObjects = {"table testBlobX1"};
            Statement cleanupStmt = conn.createStatement();
            TestUtil.cleanUpTest(cleanupStmt, testObjects);
        }

}
