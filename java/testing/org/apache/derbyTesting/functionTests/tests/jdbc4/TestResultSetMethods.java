/*
 
   Derby - Class org.apache.derbyTesting.functionTests.tests.jdbc4.TestResultSetMethods
 
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestResultSetMethods {
    
    Connection conn=null;
    PreparedStatement ps=null;
    ResultSet rs=null;
    
    SQLException sqle = Util.notImplemented();
    String message = sqle.getMessage();
    
    void t_getRowId1() {
        try {
            rs.getRowId(0);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    void t_getRowId2(){
        try {
            rs.getRowId(null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateRowId1() {
        try {
            rs.updateRowId(0,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateRowId2(){
        try {
            rs.updateRowId(null,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getHoldability() {
        try {
            int i = rs.getHoldability();
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_isClosed(){
        try {
            boolean b = rs.isClosed();
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNString1() {
        try {
            rs.updateNString(0,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNString2() {
        try {
            rs.updateNString(null,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNClob1() {
        try {
            rs.updateNClob(0,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateNClob2() {
        try {
            rs.updateNClob(null,null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getNClob1() {
        try {
            rs.getNClob(0);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getNClob2() {
        try {
            rs.getNClob(null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getSQLXML1() {
        try {
            rs.getSQLXML(0);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_getSQLXML2() {
        try {
            rs.getSQLXML(null);
            System.out.println("unImplemented Exception not thrown in code");
        } catch(SQLException e) {
            if(!message.equals(e.getMessage())) {
                System.out.println("Unexpected SQLException"+e);
            }
            
        } catch(Exception e) {
            System.out.println("Unexpected exception caught"+e);
            e.printStackTrace();
        }
    }
    
    void t_updateSQLXML1(){
    }
    
    void t_updateSQLXML2() {
    }
    
    void startTestResultSetMethods(Connection conn_in,PreparedStatement ps_in,ResultSet rs_in) {
        conn = conn_in;
        ps = ps_in;
        rs = rs_in;
        
        t_getRowId1();
        t_getRowId2();
        
        t_updateRowId1();
        t_updateRowId2();
        
        t_getHoldability();
        t_isClosed();
        
        t_updateNString1();
        t_updateNString2();
        
        t_updateNClob1();
        t_updateNClob2();
        
        t_getNClob1();
        t_getNClob2();
        
        t_getSQLXML1();
        t_getSQLXML2();
        
        t_updateSQLXML1();
        t_updateSQLXML2();
    }
    
    
    public static void main(String args[]) {
        TestConnection tc=null;
        Connection conn_main=null;
        PreparedStatement ps_main=null;
        ResultSet rs_main=null;
        
        try {
            tc = new TestConnection();
            conn_main = tc.createEmbeddedConnection();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            ps_main = conn_main.prepareStatement("select count(*) from sys.systables");
            rs_main = ps_main.executeQuery();
        } catch(SQLException e) {
            System.out.println(""+e);
            e.printStackTrace();
        }
        
        TestResultSetMethods trsm = new TestResultSetMethods();
        trsm.startTestResultSetMethods(conn_main,ps_main,rs_main);
        
        /****************************************************************************************
         * This tests the client server part of derby
         *****************************************************************************************/
        
        conn_main=null;
        ps_main=null;
        
        try {
            conn_main = tc.createClientConnection();
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        try {
            ps_main = conn_main.prepareStatement("select count(*) from sys.systables");
            rs_main = ps_main.executeQuery();
        } catch(SQLException e) {
            System.out.println(""+e);
            e.printStackTrace();
        }
        
        trsm.startTestResultSetMethods(conn_main,ps_main,rs_main);
        
    }
}
