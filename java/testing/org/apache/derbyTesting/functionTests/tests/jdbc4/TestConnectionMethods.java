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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Properties;
import org.apache.derby.impl.jdbc.Util;

public class TestConnectionMethods {
    Connection conn = null;
    SQLException sqle = Util.notImplemented();
    String message = sqle.getMessage();
    
    public TestConnectionMethods(Connection connIn) {
        conn = connIn;
    }
    
    void t_createClob() {
        Clob clob;
        try {
            clob = conn.createClob();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    void t_createNClob() {
        NClob nclob;
        try {
            nclob = conn.createNClob();
            System.out.println("unimplemented exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
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
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Unexpected exception caught in function"+e);
        }
    }
    
    public void startTestConnectionMethods() {
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
    public static void main(String args[]) {
        TestConnection tc = new TestConnection();
        
        Connection connEmbedded = tc.createEmbeddedConnection();
        TestConnectionMethods tcm = new TestConnectionMethods(connEmbedded);
        tcm.startTestConnectionMethods();
        
        
        Connection connNetwork = tc.createClientConnection();
        TestConnectionMethods tcm1 = new TestConnectionMethods(connNetwork);
        tcm1.startTestConnectionMethods();
    }
}
