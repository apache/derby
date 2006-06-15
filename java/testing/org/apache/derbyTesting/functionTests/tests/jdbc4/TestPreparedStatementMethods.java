/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestPreparedStatementMethods
 
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

package org.apache.derbyTesting.functionTests.tests.jdbc4;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Reader;
import java.io.InputStream;
import java.io.IOException;
import java.security.*;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Clob;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLXML;
import java.sql.Statement;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.derby.tools.ij;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derbyTesting.functionTests.util.SQLStateConstants;
/**
 * This class is used to test the implementations of the JDBC 4.0 methods 
 * in the PreparedStatement interface 
 */

public class TestPreparedStatementMethods {
    
    static Connection conn = null;
    PreparedStatement ps = null;
    
    String filepath;
    String sep;
    boolean exists;
    /*
     * This function is used to build the path to the directory where the files 
     * that are used to create the test blob and clob are present
     */
    void buildFilePath(String filename) {
        filepath = "extin";
        sep = System.getProperty("file.separator");
        exists = (new File("extin", filename)).exists();
        if(!exists){
            String userDir = System.getProperty("user.dir");
            filepath = userDir + sep + ".." + sep + filepath;
        }
    }
    void t_setNString() {
        try {
            ps.setNString(0,null);
            System.out.println("UnImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method"+e);
            e.printStackTrace();
        }
    }
    void t_setNCharacterStream() {
        try {
            ps.setNCharacterStream(0,null,0);
            System.out.println("UnImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method"+e);
            e.printStackTrace();
        }
    }
    void t_setNClob1() {
        try {
            ps.setNClob(0,(NClob)null);
            System.out.println("UnImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method"+e);
            e.printStackTrace();
        }
    }
    /*
     * Compares the two clobs supplied to se if they are similar
     * returns true if they are similar and false otherwise 
     */
    boolean compareClob(Clob clob1,Clob clob2) {
        int c1,c2;
        InputStream is1=null,is2=null;
        try {
            is1 = clob1.getAsciiStream();
            is2 = clob2.getAsciiStream();
            if(clob1.length()!=clob2.length())
                return false;
        } catch(SQLException sqle){
            sqle.printStackTrace();
        }
        try {
            for(long i=0;i<clob1.length();i++) {
                c1=is1.read();
                c2=is2.read();
                if(c1!=c2)
                    return false;
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return true;
    }
    /*
     * Compares the two blobs supplied to se if they are similar
     * returns true if they are similar and false otherwise 
     */
    boolean compareBlob(Blob blob1,Blob blob2) {
        int c1,c2;
        InputStream is1=null,is2=null;
        try {
            is1 = blob1.getBinaryStream();
            is2 = blob2.getBinaryStream();
            if(blob1.length()!=blob2.length())
                return false;
        } catch(SQLException sqle){
            sqle.printStackTrace();
        }
        try {
            for(long i=0;i<blob1.length();i++) {
                c1=is1.read();
                c2=is2.read();
                if(c1!=c2)
                    return false;
            }
        } catch(IOException e) {
            e.printStackTrace();
        } catch(SQLException e) {
            e.printStackTrace();
        }
        return true;
    }
    /*
     * Build the clob value to be inserted into the table and insert it using 
     * the setClob method in the PreparedStatement interface
     */
    Clob buildAndInsertClobValue(int n,String filename,Connection conn){
        int c;
        byte [] fromFile = new byte[1024];
        Clob clob=null;
        try {
            clob = conn.createClob();
            java.io.OutputStream os = clob.setAsciiStream(1);
            buildFilePath(filename);
            File f = new File(filepath + sep + filename);
            InputStream is = getInputStream(f);
            c = is.read(fromFile);
            while(c>0) {
                os.write(fromFile,0,c);
                c = is.read(fromFile);
            }
            PreparedStatement ps = conn.prepareStatement("insert into clobtable3 values(?,?)");
            ps.setInt(1, n);
            ps.setClob(2,clob);
            ps.executeUpdate();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(PrivilegedActionException pae) {
            pae.printStackTrace();
        } catch(SQLException sqle) {
            sqle.printStackTrace();
        }
        return clob;
    }
    /*
     * Build the clob value to be inserted into the table and insert it using
     * the setBlob method in the PreparedStatement interface
     */
    Blob buildAndInsertBlobValue(int n,String filename,Connection conn){
        int c;
        byte [] fromFile = new byte[1024];
        Blob blob=null;
        try {
            blob = conn.createBlob();
            java.io.OutputStream os = blob.setBinaryStream(1);
            buildFilePath(filename);
            File f = new File(filepath + sep + filename);
            InputStream is = getInputStream(f);
            c = is.read(fromFile);
            while(c>0) {
                os.write(fromFile,0,c);
                c = is.read(fromFile);
            }
            PreparedStatement ps = conn.prepareStatement("insert into blobtable3 values(?,?)");
            ps.setInt(1, n);
            ps.setBlob(2,blob);
            ps.executeUpdate();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(PrivilegedActionException pae) {
            pae.printStackTrace();
        } catch(SQLException sqle) {
            sqle.printStackTrace();
        }
        return blob;
    }

	/**
     * May need to convert this into a privileged block for reading a file. 
     */
    protected static FileInputStream getInputStream(final File f)
		throws PrivilegedActionException, FileNotFoundException
    {
        return (FileInputStream)AccessController.doPrivileged
			( new PrivilegedExceptionAction<FileInputStream>(  )
        {
			public FileInputStream run() throws FileNotFoundException
			{
				return new FileInputStream(f);
			}
			});
    }


	/*
     * 1) Insert the clob in to the clob table by calling the 
     *    buildAndInsertClobValue function
     * 2) Check whether the clob value has been correctly inserted in to the 
     *    table by using the compareClob function
     */
    void t_setClob() {
        try {
            int c;
            byte [] fromFile;
            fromFile = new byte[1024];
            Statement s = conn.createStatement();
            s.execute("create table clobtable3(n int)");
            s.execute("alter table clobtable3 add column clobCol CLOB(1M)");
            s.close();
            Clob clob = buildAndInsertClobValue(0000,"short.txt",conn);
            Clob clob1 = buildAndInsertClobValue(1000, "aclob.txt",conn);
            Clob clob2 = buildAndInsertClobValue(2000,"littleclob.txt",conn);
            PreparedStatement ps3 = conn.prepareStatement("select * from " +
                    "clobtable3 where n=1000");
            ResultSet rs3 = ps3.executeQuery();
            rs3.next();
            Clob clob3 = rs3.getClob(2);
            if(!compareClob(clob1,clob3)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Clob values");
            }
            PreparedStatement ps4 = conn.prepareStatement("select * from " +
                    "clobtable3");
            ResultSet rs4 = ps4.executeQuery();
            rs4.next();
            Clob clob4 = rs4.getClob(2);
            if(!compareClob(clob,clob4)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Clob values");
            }
            rs4.next();
            clob4 = rs4.getClob(2);
            if(!compareClob(clob1,clob4)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Clob values");
            }
            rs4.next();
            clob4 = rs4.getClob(2);
            if(!compareClob(clob2,clob4)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Clob values");
            }
            
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                e.printStackTrace();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    /*
     * 1) Insert the blob in to the blob table by calling the 
     *    buildAndInsertBlobValue function
     * 2) Check whether the blob value has been correctly inserted in to the 
     *    table by using the compareBlob function
     */
    void t_setBlob() {
        try {
            int c;
            byte [] fromFile;
            fromFile = new byte[1024];
            Statement s = conn.createStatement();
            s.execute("create table blobtable3(n int)");
            s.execute("alter table blobtable3 add column blobCol BLOB(1M)");
            s.close();
            Blob blob = buildAndInsertBlobValue(0000, "short.txt", conn);
            Blob blob1 = buildAndInsertBlobValue(1000, "aclob.txt", conn);
            Blob blob2 = buildAndInsertBlobValue(2000, "littleclob.txt", conn);
            PreparedStatement ps3 = conn.prepareStatement("select * from " +
                    "blobtable3 where n=1000");
            ResultSet rs3 = ps3.executeQuery();
            rs3.next();
            Blob blob3 = rs3.getBlob(2);
            if(!compareBlob(blob1,blob3)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Blob values");
            }
            PreparedStatement ps4 = conn.prepareStatement("select * from blobtable3");
            ResultSet rs4 = ps4.executeQuery();
            rs4.next();
            Blob blob4 = rs4.getBlob(2);
            if(!compareBlob(blob,blob4)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Blob values");
            }
            rs4.next();
            blob4 = rs4.getBlob(2);
            if(!compareBlob(blob1,blob4)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Blob values");
            }
            rs4.next();
            blob4 = rs4.getBlob(2);
            if(!compareBlob(blob2,blob4)) {
                System.out.println("Difference between the inserted and the " +
                        "queried Blob values");
            }
            
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                e.printStackTrace();
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    /*
     * The setClob method on the embedded side. Here functionality has still not
     * been added. It still throws a notImplemented exception only
     */
    void t_Clob_setMethods_Embedded(Connection conn){
        try {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select * from clobtable3");
            rs.next();
            Clob clob = rs.getClob(2);
            PreparedStatement ps = conn.prepareStatement("insert into clobtable3" +
                    " values(?,?)");
            ps.setInt(1, 3000);
            ps.setClob(2, clob);
            ps.executeUpdate();
            ps.close();
        } catch(SQLException e){
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
                e.printStackTrace();
            }
        }
    }
    /*
     * The setBlob method on the embedded side. Here functionality has still not
     * been added. It still throws a notImplemented exception only
     */
    void t_Blob_setMethods_Embedded(Connection conn){
        try {
            Statement s = conn.createStatement();
            ResultSet rs = s.executeQuery("select * from blobtable3");
            rs.next();
            Blob blob = rs.getBlob(2);
            PreparedStatement ps = conn.prepareStatement("insert into blobtable3" +
                    " values(?,?)");
            ps.setInt(1, 3000);
            ps.setBlob(2, blob);
            ps.executeUpdate();
            ps.close();
        } catch(SQLException e){
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
                e.printStackTrace();
            }
        }
    }
    
    void t_setNClob2() {
        try {
            ps.setNClob(0,null,0);
            System.out.println("UnImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method"+e);
            e.printStackTrace();
        }
    }
    void t_setSQLXML() {
        try {
            ps.setSQLXML(0,null);
            System.out.println("UnImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method"+e);
            e.printStackTrace();
        }
    }
    void t_setPoolable() {
        try {
            // Set the poolable statement hint to false
            ps.setPoolable(false);
            if (ps.isPoolable())
                System.out.println("Expected a non-poolable statement");
            // Set the poolable statement hint to true
            ps.setPoolable(true);
            if (!ps.isPoolable())
                System.out.println("Expected a poolable statement");
        } catch(SQLException sqle) {
            // Check which SQLException state we've got and if it is
            // expected, do not print a stackTrace
            // Embedded uses XJ012, client uses XCL31.
            if (sqle.getSQLState().equals("XJ012") ||
                sqle.getSQLState().equals("XCL31")) {
                // All is good and is expected
            } else {
                System.out.println("Unexpected SQLException " + sqle);
                sqle.printStackTrace();
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method " + e);
            e.printStackTrace();
        }
    }
    void t_isPoolable() {
        try {
            // By default a prepared statement is poolable
            if (!ps.isPoolable())
                System.out.println("Expected a poolable statement");
        } catch(SQLException sqle) {
            // Check which SQLException state we've got and if it is
            // expected, do not print a stackTrace
            // Embedded uses XJ012, client uses XCL31.
            if (sqle.getSQLState().equals("XJ012") ||
                sqle.getSQLState().equals("XCL31")) {
                // All is good and is expected
            } else {
                System.out.println("Unexpected SQLException " + sqle);
                sqle.printStackTrace();
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception thrown in method " + e);
            e.printStackTrace();
        }
    }
    
    /**
     * Tests the wrapper methods isWrapperFor and unwrap. There are two cases
     * to be tested
     * Case 1: isWrapperFor returns true and we call unwrap
     * Case 2: isWrapperFor returns false and we call unwrap
     */
    void t_wrapper() {
        Class<PreparedStatement> wrap_class = PreparedStatement.class;
        
        //The if method succeeds enabling us to call the unwrap method without 
        //throwing an exception
        try {
            if(ps.isWrapperFor(wrap_class)) {
                PreparedStatement stmt1 = 
                        (PreparedStatement)ps.unwrap(wrap_class);
            }
            else {
                System.out.println("isWrapperFor wrongly returns false");
            }
        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
        }
        
        //Begin test for case2
        //test for the case when isWrapper returns false
        //using some class that will return false when 
        //passed to isWrapperFor
        Class<ResultSet> wrap_class1 = ResultSet.class;
        
        try {
            //returning false is the correct behaviour in this case
            //Generate a message if it returns true
            if(ps.isWrapperFor(wrap_class1)) {
                System.out.println("isWrapperFor wrongly returns true");
            }
            else {
                ResultSet rs1 = (ResultSet)
                                           ps.unwrap(wrap_class1);
                System.out.println("unwrap does not throw the expected " +
                                   "exception");
            }
        }
        catch (SQLException sqle) {
            //calling unwrap in this case throws an SQLException 
            //ensure that this SQLException has the correct SQLState
            if(!SQLStateConstants.UNABLE_TO_UNWRAP.equals(sqle.getSQLState())) {
                sqle.printStackTrace();
            }
        }
    }
    /*
     * Start the tests for the JDBC4.0 methods on the client side
     */
    void startClientTestMethods( Connection conn_main ) {
        PreparedStatement ps_main = null;
        
        try {
            ps_main = conn_main.prepareStatement("select count(*) from " +
                    "sys.systables");
            conn = conn_main;
            ps = ps_main;
            t_setNString();
            t_setNCharacterStream();
            t_setNClob1();
            t_setClob();
            t_setBlob();
            t_setNClob2();
            t_setSQLXML();
            t_isPoolable();
            t_setPoolable();
            t_wrapper();
            // Close the prepared statement and verify the poolable hint
            // cannot be set or retrieved
            ps.close();
            t_isPoolable();
            t_setPoolable();
        } catch(SQLException sqle) {
            sqle.printStackTrace();
        } finally {
            try {
                conn_main.close();
            } catch(SQLException sqle){
                sqle.printStackTrace();
            }
        }
    }
    /*
     * Start the tests for testing the JDBC4.0 methods on the embedded side
     */
    void startEmbeddedTestMethods( Connection conn_main ) {
        PreparedStatement ps_main = null;
        
        try {            
            Statement s = conn_main.createStatement();
            s.execute("create table clobtable3 (n int,clobcol CLOB)");
            File file = new File("extin/short.txt");
            int fileLength = (int) file.length();
            InputStream fin = getInputStream(file);
            ps = conn_main.prepareStatement("INSERT INTO " +
                    "clobtable3 " +
                    "VALUES (?, ?)");
            ps.setInt(1, 1000);
            ps.setAsciiStream(2, fin, fileLength);
            ps.execute();
            ps.close();
            
            Statement s1 = conn_main.createStatement();
            s1.execute("create table blobtable3 (n int,blobcol BLOB)");
            File file1 = new File("extin/short.txt");
            int fileLength1 = (int) file1.length();
            InputStream fin1 = getInputStream(file1);
            PreparedStatement ps1 = conn_main.prepareStatement("INSERT INTO " +
                    "blobtable3 " +
                    "VALUES (?, ?)");
            ps1.setInt(1, 1000);
            ps1.setBinaryStream(2, fin1, fileLength1);
            ps1.execute();
            
            conn_main.commit();
            t_Clob_setMethods_Embedded(conn_main);
            t_Blob_setMethods_Embedded(conn_main);
            ps_main = conn_main.prepareStatement("select count(*) from " +
                    "sys.systables");
            conn = conn_main;
            ps = ps_main;
            t_setNString();
            t_setNCharacterStream();
            t_setNClob1();
            t_setNClob2();
            t_setSQLXML();
            t_isPoolable();
            t_setPoolable();
            t_wrapper();
            // Close the prepared statement and verify the poolable hint
            // cannot be set or retrieved
            ps.close();
            t_isPoolable();
            t_setPoolable();
        }
        catch(SQLException sqle) {
            sqle.printStackTrace();
        } catch(PrivilegedActionException pae) {
            pae.printStackTrace();
        } catch(FileNotFoundException fne) {
            fne.printStackTrace();
        } finally {
            try {
                conn_main.close();
            } catch(SQLException sqle){
                sqle.printStackTrace();
            }
        }
    }
    
	/**
	 * <p>
	 * Return true if we're running under the embedded client.
	 * </p>
	 */
	private	static	boolean	usingEmbeddedClient()
	{
		return "embedded".equals( System.getProperty( "framework" ) );
	}

    public static void main(String args[]) {
		try {
			// use the ij utility to read the property file and
			// make the initial connection.
			ij.getPropertyArg(args);
		
			Connection	conn_main = ij.startJBMS();

			TestPreparedStatementMethods tpsm = new TestPreparedStatementMethods();

			if ( usingEmbeddedClient() )
			{
				tpsm.startEmbeddedTestMethods( conn_main );
			}
			else // DerbyNetClient
			{
				tpsm.startClientTestMethods( conn_main );
			}
        			
		} catch(Exception e) { printStackTrace( e ); }
    }

	private	static	void	printStackTrace( Throwable e )
	{
		System.out.println(""+e);
		e.printStackTrace();
	}
}
