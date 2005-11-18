/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestCallableStatementMethods
 
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

import org.apache.derby.impl.jdbc.Util;
import java.io.Reader;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.CallableStatement;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLXML;

public class TestCallableStatementMethods{
    
    Connection conn=null;
    CallableStatement cs=null;
    
    SQLException sqle = Util.notImplemented();
    String message = sqle.getMessage();
    
    void t_getRowId1() {
        try {
            cs.getRowId(0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_getRowId2() {
        try {
            cs.getRowId(null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setRowId() {
        try {
            cs.setRowId(null,null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setNString() {
        try {
            cs.setNString(null,null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setNCharacterStream() {
        try {
            cs.setNCharacterStream(null,null,0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setNClob1() {
        try {
            cs.setNClob(null,null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setClob() {
        try {
            cs.setClob(null,null,0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setBlob() {
        try {
            cs.setBlob(null,null,0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setNClob2() {
        try {
            cs.setNClob(null,null,0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_getNClob1() {
        try {
            NClob nclob = cs.getNClob(0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_getNClob2(){
        try {
            NClob nclob = cs.getNClob(null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_setSQLXML() {
        try {
            cs.setSQLXML(null,null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_getSQLXML1() {
        try {
            SQLXML sqlxml = cs.getSQLXML(0);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    void t_getSQLXML2() {
        try {
            SQLXML sqlxml = cs.getSQLXML(null);
            System.out.println("Not Implemented Exception not thrown");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
        } catch(Exception e) {
            System.out.println("Exception"+e);
            e.printStackTrace();
        }
    }
    
    
    void startCallableStatementMethodTest(Connection conn_in,CallableStatement cs_in) {
        conn = conn_in;
        cs = cs_in;
        t_getRowId1();
        t_getRowId2();
        t_setRowId();
        t_setNString();
        t_setNCharacterStream();
        t_setNClob1();
        t_setClob();
        t_setBlob();
        t_setNClob2();
        t_getNClob1();
        t_getNClob2();
        t_setSQLXML();
        t_getSQLXML1();
        t_getSQLXML2();
    }
    
    public static void main(String args[]) {
        TestConnection tc=null;
        Connection conn_main=null;
        CallableStatement cs_main=null;
        
        try {
            tc = new TestConnection();
            conn_main = tc.createEmbeddedConnection();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            cs_main = conn_main.prepareCall("select count(*) from sys.systables");
        } catch(SQLException e) {
            System.out.println(""+e);
            e.printStackTrace();
        }
        
        TestCallableStatementMethods tcsm = new TestCallableStatementMethods();
        tcsm.startCallableStatementMethodTest(conn_main,cs_main);
        
        conn_main=null;
        cs_main=null;
        
        try {
            conn_main = tc.createClientConnection();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            cs_main = conn_main.prepareCall("select count(*) from sys.systables");
        } catch(SQLException e) {
            System.out.println(""+e);
            e.printStackTrace();
        }
        
        tcsm.startCallableStatementMethodTest(conn_main,cs_main);
    }
}
