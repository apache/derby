/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestConnectionMethods
 
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.Properties;
import org.apache.derby.tools.ij;
import org.apache.derby.shared.common.reference.SQLState;

/**
 * This class is used to test the implementations of the JDBC 4.0 methods
 * in the Connection interface
 */

public class TestConnectionMethods {
    Connection conn = null;
    
    public TestConnectionMethods(Connection connIn) {
        conn = connIn;
    }
    
    void t_createClob() {
        Clob clob;
        try {
            clob = conn.createClob();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    void t_createBlob() {
        Blob blob;
        try {
            blob = conn.createBlob();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    /**
     * Test the createClob method implementation in the Connection interface 
     * in the Network Client
     */
    void t_createClob_Client() {
        int c;
        Clob clob;
        try {
            Statement s = conn.createStatement();
            s.execute("create table clobtable2(n int,clobcol CLOB)");
            PreparedStatement ps = conn.prepareStatement("insert into clobtable2" +
                    " values(?,?)");
            ps.setInt(1,1000);
            clob = conn.createClob();
            File file = new File("extin/short.txt");
            FileInputStream is = new FileInputStream(file);
            OutputStream os = clob.setAsciiStream(1);
            c = is.read();
            while(c>0) {
                os.write(c);
                c = is.read();
            }
            ps.setClob(2, clob);
            ps.executeUpdate();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(FileNotFoundException fnfe){
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    /**
     * Test the createBlob method implementation in the Connection interface for
     * in the Network Client
     */
    void t_createBlob_Client() {
        int c;
        Blob blob;
        try {
            Statement s = conn.createStatement();
            s.execute("create table blobtable2(n int,blobcol BLOB)");
            PreparedStatement ps = conn.prepareStatement("insert into blobtable2" +
                    " values(?,?)");
            ps.setInt(1,1000);
            blob = conn.createBlob();
            File file = new File("extin/short.txt");
            FileInputStream is = new FileInputStream(file);
            OutputStream os = blob.setBinaryStream(1);
            c = is.read();
            while(c>0) {
                os.write(c);
                c = is.read();
            }
            ps.setBlob(2, blob);
            ps.executeUpdate();
        } catch(SQLException e) {
            e.printStackTrace();
        } catch(FileNotFoundException fnfe){
            fnfe.printStackTrace();
        } catch(IOException ioe) {
            ioe.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
    }
    
    void t_createNClob() {
        NClob nclob;
        try {
            nclob = conn.createNClob();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    void t_createSQLXML() {
        SQLXML sqlXML;
        try {
            sqlXML = conn.createSQLXML();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
        
    }
    
    void t_isValid() {
        boolean ret;
        try {
            ret = conn.isValid(0);
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    
    void t_setClientInfo1(){
        try {
            conn.setClientInfo("prop1","value1");
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    
    void t_setClientInfo2(){
        try {
            Properties p = new Properties();
            conn.setClientInfo(p);
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    
    void t_getClientInfo1(){
        String info;
        try {
            info = conn.getClientInfo("prop1");
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
        
    }
    
    void t_getClientInfo2(){
        Properties p=null;
        try {
            p = conn.getClientInfo();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(SQLState.NOT_IMPLEMENTED.equals (e.getSQLState())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    
    public void startTestConnectionMethods_Client() {
        t_createClob_Client();
        t_createBlob_Client();
        t_createNClob();
        t_createSQLXML();
        t_isValid();
        t_setClientInfo1();
        t_setClientInfo2();
        t_getClientInfo1();
        t_getClientInfo2();
    }
    
    public void startTestConnectionMethods_Embedded() {
        t_createClob();
        t_createBlob();
        t_createNClob();
        t_createSQLXML();
        t_isValid();
        t_setClientInfo1();
        t_setClientInfo2();
        t_getClientInfo1();
        t_getClientInfo2();
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

			if ( usingEmbeddedClient() )
			{
				TestConnectionMethods tcm = new TestConnectionMethods( conn_main );
				tcm.startTestConnectionMethods_Embedded();
			}
			else // DerbyNetClient
			{
				TestConnectionMethods tcm1 = new TestConnectionMethods( conn_main );
				tcm1.startTestConnectionMethods_Client();
			}

			conn_main.close();

		} catch(Exception e) {
			System.out.println(""+e);
			e.printStackTrace();
		}
    }
}
